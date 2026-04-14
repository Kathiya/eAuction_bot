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
from project.scraper.search_urls import build_fetch_urls, _with_page

logger = logging.getLogger(__name__)

_ORIGIN = "https://www.eauctionsindia.com"
_IMPERSONATE = "chrome120"

# ScraperAPI residential proxy endpoint — used only as fallback for page-1 403s.
_SCRAPER_API_PROXY = "http://scraperapi:{key}@proxy-server.scraperapi.com:8001"


class AllSourcesBlocked(Exception):
    """All configured search-URL page-1 requests returned 403."""


def _make_session(impersonate: str, verify_ssl: bool, proxy_url: str | None = None) -> CurlSession:
    s = CurlSession(impersonate=impersonate)
    s.verify = verify_ssl
    if proxy_url:
        s.proxies = {"http": proxy_url, "https": proxy_url}
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
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
    """GET with retry on transient 5xx / network errors only (never retries 4xx)."""
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            r = session.get(url, timeout=timeout, headers=extra_headers)
            if r.status_code < 500:
                return r
            last_exc = CurlHTTPError(f"HTTP Error {r.status_code}: {r.reason}", 0, r)
        except Exception as e:
            last_exc = e
        if attempt < max_retries:
            time.sleep(0.8 * (2**attempt))
    raise last_exc  # type: ignore[misc]


def _remove_page_param(url: str) -> str:
    parts = urlparse(url)
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "page"]
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, urlencode(q), parts.fragment))


class HttpListingSource:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._session = _make_session(_IMPERSONATE, settings.http_verify_ssl)
        # Proxy session is built lazily only when a key is configured.
        self._proxy_session: CurlSession | None = None
        if settings.scraper_api_key:
            proxy_url = _SCRAPER_API_PROXY.format(key=settings.scraper_api_key)
            self._proxy_session = _make_session(_IMPERSONATE, settings.http_verify_ssl, proxy_url)
            logger.info(
                "proxy_session_ready",
                extra={"event": "proxy_session_ready", "provider": "scraperapi"},
            )

    def _warm_up(self) -> None:
        """Visit the homepage first to acquire cookies and set a realistic navigation context."""
        try:
            self._session.get(_ORIGIN + "/", timeout=self._s.request_timeout_s)
            logger.info("warm_up_ok", extra={"event": "warm_up_ok"})
            time.sleep(1.5)
        except Exception as e:
            logger.warning("warm_up_failed", extra={"event": "warm_up_failed", "error": str(e)})

    def _fetch_html(self, url: str, referer: str | None = None, via_proxy: bool = False) -> str:
        """Fetch a single URL; optionally through the proxy session."""
        extra: dict[str, str] = {}
        if referer:
            extra["Referer"] = referer
            extra["sec-fetch-site"] = "same-origin"
        session = self._proxy_session if via_proxy else self._session
        assert session is not None
        r = _http_get(session, url, self._s.request_timeout_s, self._s.max_retries, extra or None)
        if r.status_code == 403 and not via_proxy:
            # Some WAF rules reject explicit page=N; retry once without the parameter.
            fallback_url = _remove_page_param(url)
            if fallback_url != url:
                logger.warning(
                    "http_403_retry_without_page",
                    extra={
                        "event": "http_403_retry_without_page",
                        "url": url,
                        "fallback_url": fallback_url,
                    },
                )
                r = _http_get(session, fallback_url, self._s.request_timeout_s, self._s.max_retries, extra or None)
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
            logger.warning("http_api_failed", extra={"event": "http_api_failed", "error": str(e)})
            return None

    def fetch_pages(self) -> list[PropertyListing]:
        api_items = self._try_api_once()
        if api_items:
            return api_items

        self._warm_up()

        merged: dict[str, PropertyListing] = {}
        base_urls = self._s.parsed_listing_search_urls() or [self._s.listing_page_url]
        page1_blocked = 0  # count of search URLs where even page 1 returned 403

        for base_url in base_urls:
            for page in range(1, self._s.max_pages_per_run + 1):
                if page > 1:
                    time.sleep(self._s.rate_limit_delay_s)

                url = _with_page(base_url, page)
                via_proxy = False

                try:
                    html = self._fetch_html(url, referer=_ORIGIN + "/")

                except CurlHTTPError as e:
                    status = e.response.status_code if e.response is not None else 0

                    if status == 403:
                        if page == 1:
                            # Real block on the entry page for this search URL.
                            if self._proxy_session:
                                logger.warning(
                                    "http_fetch_403_retrying_proxy",
                                    extra={"event": "http_fetch_403_retrying_proxy", "url": url},
                                )
                                try:
                                    html = self._fetch_html(url, referer=_ORIGIN + "/", via_proxy=True)
                                    via_proxy = True
                                except CurlHTTPError as proxy_exc:
                                    proxy_status = (
                                        proxy_exc.response.status_code
                                        if proxy_exc.response is not None
                                        else 0
                                    )
                                    logger.warning(
                                        "http_fetch_403_proxy_also_failed",
                                        extra={
                                            "event": "http_fetch_403_proxy_also_failed",
                                            "url": url,
                                            "proxy_status": proxy_status,
                                        },
                                    )
                                    page1_blocked += 1
                                    break  # move to next search URL
                                except Exception as proxy_exc:
                                    logger.warning(
                                        "http_fetch_403_proxy_also_failed",
                                        extra={
                                            "event": "http_fetch_403_proxy_also_failed",
                                            "url": url,
                                            "proxy_status": str(proxy_exc),
                                        },
                                    )
                                    page1_blocked += 1
                                    break
                            else:
                                logger.warning(
                                    "http_fetch_403_no_proxy",
                                    extra={"event": "http_fetch_403_no_proxy", "url": url},
                                )
                                page1_blocked += 1
                                break  # move to next search URL
                        else:
                            # 403 on page 2+ = no more results for this search URL (not a real block).
                            logger.info(
                                "http_pagination_end",
                                extra={"event": "http_pagination_end", "url": url, "page": page},
                            )
                            break  # move to next search URL

                    else:
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

                if via_proxy:
                    logger.info("proxy_fetch_ok", extra={"event": "proxy_fetch_ok", "url": url})

                batch = parse_listings_page(self._s, html, url)
                logger.info(
                    "http_page_parsed",
                    extra={"event": "http_page_parsed", "url": url, "page": page, "count": len(batch)},
                )
                for x in batch:
                    merged[x.stable_id] = x

                if not batch:
                    # Empty page means we've exhausted results for this search URL.
                    break

        if page1_blocked == len(base_urls) and not merged:
            raise AllSourcesBlocked(
                f"All {page1_blocked} search URL(s) returned 403 on page 1 — "
                "site is blocking this IP range. Set SCRAPER_API_KEY secret to enable proxy fallback."
            )

        return list(merged.values())
