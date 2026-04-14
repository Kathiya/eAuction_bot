from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from curl_cffi import requests as cffi_requests
from curl_cffi.requests import Session as CurlSession
from curl_cffi.requests.exceptions import HTTPError as CurlHTTPError

from project.config.settings import Settings
from project.filters.models import PropertyListing
from project.scraper.api_parser import listings_from_api_payload
from project.scraper.portal_parse import parse_listings_page
from project.scraper.search_urls import build_fetch_urls

logger = logging.getLogger(__name__)

_ORIGIN = "https://www.eauctionsindia.com"

# Impersonate Chrome 120 at the TLS layer — bypasses JA3/JA4 fingerprint checks
# that block Python requests even when HTTP headers look correct.
_IMPERSONATE = "chrome120"


class AllSourcesBlocked(Exception):
    """All configured search URLs returned 403; the site is blocking CI/cloud IPs."""


def _build_session(settings: Settings) -> CurlSession:
    s = CurlSession(impersonate=_IMPERSONATE)
    s.verify = settings.http_verify_ssl
    s.headers.update(
        {
            "User-Agent": settings.http_user_agent,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;"
                "q=0.8,application/signed-exchange;v=b3;q=0.7"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
        }
    )
    return s


def _http_get(
    session: CurlSession,
    url: str,
    timeout: float,
    max_retries: int,
    extra_headers: dict[str, str] | None = None,
) -> cffi_requests.Response:
    """GET with simple retry on transient 5xx / network errors (not 4xx)."""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            r = session.get(url, timeout=timeout, headers=extra_headers)
            if r.status_code < 500:
                return r
            # 5xx — wait and retry
            last_exc = CurlHTTPError(f"HTTP Error {r.status_code}: {r.reason}", 0, r)
        except Exception as e:
            last_exc = e
        if attempt < max_retries:
            time.sleep(0.8 * (2**attempt))
    raise last_exc  # type: ignore[misc]


class HttpListingSource:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._session = _build_session(settings)

    @staticmethod
    def _remove_page_param(url: str) -> str:
        parts = urlparse(url)
        q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "page"]
        return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, urlencode(q), parts.fragment))

    def _warm_up(self) -> None:
        """Visit the homepage first to acquire cookies and set a realistic navigation context."""
        try:
            self._session.get(_ORIGIN + "/", timeout=self._s.request_timeout_s)
            logger.info("warm_up_ok", extra={"event": "warm_up_ok"})
            time.sleep(1.5)
        except Exception as e:
            logger.warning("warm_up_failed", extra={"event": "warm_up_failed", "error": str(e)})

    def _get_text(self, url: str, referer: str | None = None) -> str:
        extra: dict[str, str] = {}
        if referer:
            extra["Referer"] = referer
            extra["sec-fetch-site"] = "same-origin"
        r = _http_get(self._session, url, self._s.request_timeout_s, self._s.max_retries, extra or None)
        if r.status_code == 403:
            fallback_url = self._remove_page_param(url)
            if fallback_url != url:
                logger.warning(
                    "http_403_retry_without_page",
                    extra={
                        "event": "http_403_retry_without_page",
                        "url": url,
                        "fallback_url": fallback_url,
                    },
                )
                r = _http_get(
                    self._session, fallback_url, self._s.request_timeout_s, self._s.max_retries, extra or None
                )
        r.raise_for_status()
        return r.text

    def _get_json(self, url: str) -> Any:
        r = _http_get(self._session, url, self._s.request_timeout_s, self._s.max_retries)
        r.raise_for_status()
        return r.json()

    def _try_api_once(self) -> list[PropertyListing] | None:
        base = self._s.http_listing_api_url
        if not base:
            return None
        try:
            payload = self._get_json(base)
            bases = self._s.parsed_listing_search_urls()
            base_for_api = bases[0] if bases else self._s.listing_page_url
            items = listings_from_api_payload(payload, base_for_api)
            if items:
                logger.info("http_api_listings", extra={"event": "http_api_listings", "count": len(items)})
            return items
        except Exception as e:
            logger.warning(
                "http_api_failed",
                extra={"event": "http_api_failed", "error": str(e)},
            )
            return None

    def fetch_pages(self) -> list[PropertyListing]:
        api_items = self._try_api_once()
        if api_items:
            return api_items

        self._warm_up()

        merged: dict[str, PropertyListing] = {}
        urls = build_fetch_urls(self._s)
        blocked_count = 0

        for i, url in enumerate(urls):
            if i > 0:
                time.sleep(self._s.rate_limit_delay_s)
            try:
                html = self._get_text(url, referer=_ORIGIN + "/")
            except CurlHTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    blocked_count += 1
                    logger.warning(
                        "http_fetch_403",
                        extra={"event": "http_fetch_403", "url": url},
                    )
                    continue
                logger.error(
                    "http_fetch_failed",
                    extra={"event": "http_fetch_failed", "url": url, "error": str(e)},
                )
                raise
            except Exception as e:
                logger.error(
                    "http_fetch_failed",
                    extra={"event": "http_fetch_failed", "url": url, "error": str(e)},
                )
                raise

            batch = parse_listings_page(self._s, html, url)
            logger.info(
                "http_page_parsed",
                extra={"event": "http_page_parsed", "url": url, "count": len(batch)},
            )
            for x in batch:
                merged[x.stable_id] = x
            if not batch and i > 0:
                break

        if blocked_count > 0 and not merged:
            raise AllSourcesBlocked(
                f"{blocked_count} URL(s) returned 403 — the site is blocking requests from this IP range"
            )

        return list(merged.values())
