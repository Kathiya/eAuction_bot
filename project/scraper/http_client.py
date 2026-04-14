from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import certifi
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from project.config.settings import Settings
from project.filters.models import PropertyListing
from project.scraper.api_parser import listings_from_api_payload
from project.scraper.portal_parse import parse_listings_page
from project.scraper.search_urls import build_fetch_urls

logger = logging.getLogger(__name__)

_ORIGIN = "https://www.eauctionsindia.com"


class AllSourcesBlocked(Exception):
    """All configured search URLs returned 403; the site is blocking CI/cloud IPs."""


def _build_session(settings: Settings) -> requests.Session:
    s = requests.Session()
    if settings.http_verify_ssl:
        s.verify = certifi.where()
    else:
        s.verify = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    retries = Retry(
        total=settings.max_retries,
        backoff_factor=0.8,
        status_forcelist=(502, 503, 504),
        allowed_methods=("GET", "HEAD"),
    )
    adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
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
        """Visit the homepage first to acquire cookies and establish a realistic browsing session."""
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
        r = self._session.get(url, timeout=self._s.request_timeout_s, headers=extra or None)
        if r.status_code == 403:
            # Some WAF rules reject explicit page=1; retry once without the parameter.
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
                r = self._session.get(fallback_url, timeout=self._s.request_timeout_s, headers=extra or None)
        r.raise_for_status()
        return r.text

    def _get_json(self, url: str) -> Any:
        r = self._session.get(url, timeout=self._s.request_timeout_s)
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
            except requests.exceptions.HTTPError as e:
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
