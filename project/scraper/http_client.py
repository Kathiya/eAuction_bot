from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, quote_plus

from curl_cffi import requests as cffi_requests
from curl_cffi.requests import Session as CurlSession
from curl_cffi.requests.exceptions import HTTPError as CurlHTTPError

from project.config.settings import Settings
from project.filters.models import PropertyListing
from project.scraper.api_parser import listings_from_api_payload
from project.scraper.portal_parse import parse_listings_page
from project.scraper.search_urls import _with_page

logger = logging.getLogger(__name__)

_ORIGIN = "https://www.eauctionsindia.com"
_IMPERSONATE = "chrome120"

# ScraperAPI URL-based endpoint: ScraperAPI fetches the target URL from their
# residential IPs and returns the HTML body. No proxy config needed.
_SCRAPER_API_URL = "https://api.scraperapi.com"


class AllSourcesBlocked(Exception):
    """All configured search-URL page-1 requests returned 403."""


def _make_session(impersonate: str, verify_ssl: bool) -> CurlSession:
    s = CurlSession(impersonate=impersonate)
    s.verify = verify_ssl
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
        if settings.scraper_api_key:
            logger.info(
                "scraperapi_enabled",
                extra={"event": "scraperapi_enabled", "provider": "scraperapi"},
            )

    def _warm_up(self) -> None:
        """Visit the homepage first to acquire cookies and set a realistic navigation context."""
        try:
            self._session.get(_ORIGIN + "/", timeout=self._s.request_timeout_s)
            logger.info("warm_up_ok", extra={"event": "warm_up_ok"})
            time.sleep(1.5)
        except Exception as e:
            logger.warning("warm_up_failed", extra={"event": "warm_up_failed", "error": str(e)})

    def _fetch_html_direct(self, url: str, referer: str | None = None) -> str:
        """Fetch a URL directly (no proxy). Retries once without page param on 403."""
        extra: dict[str, str] = {}
        if referer:
            extra["Referer"] = referer
            extra["sec-fetch-site"] = "same-origin"
        r = _http_get(self._session, url, self._s.request_timeout_s, self._s.max_retries, extra or None)
        if r.status_code == 403:
            fallback_url = _remove_page_param(url)
            if fallback_url != url:
                logger.warning(
                    "http_403_retry_without_page",
                    extra={"event": "http_403_retry_without_page", "url": url, "fallback_url": fallback_url},
                )
                r = _http_get(
                    self._session, fallback_url, self._s.request_timeout_s, self._s.max_retries, extra or None
                )
        r.raise_for_status()
        return r.text

    def _fetch_html_via_scraperapi(self, url: str) -> str:
        """
        Fetch a URL through ScraperAPI's URL-based endpoint.
        ScraperAPI makes the request from its own residential/datacenter IPs
        and returns the HTML body directly — no proxy config needed.
        """
        api_url = f"{_SCRAPER_API_URL}?api_key={self._s.scraper_api_key}&url={quote_plus(url)}"
        r = _http_get(self._session, api_url, self._s.request_timeout_s + 30, self._s.max_retries)
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
                used_scraperapi = False

                try:
                    html = self._fetch_html_direct(url, referer=_ORIGIN + "/")

                except CurlHTTPError as e:
                    status = e.response.status_code if e.response is not None else 0

                    if status == 403:
                        if page > 1:
                            # 403 on page 2+ = site has no more results for this URL.
                            logger.info(
                                "http_pagination_end",
                                extra={"event": "http_pagination_end", "url": url, "page": page},
                            )
                            break

                        # page == 1: real IP block — try ScraperAPI if key is configured.
                        if not self._s.scraper_api_key:
                            logger.warning(
                                "http_fetch_403_no_scraperapi",
                                extra={"event": "http_fetch_403_no_scraperapi", "url": url},
                            )
                            page1_blocked += 1
                            break

                        logger.warning(
                            "http_fetch_403_retrying_scraperapi",
                            extra={"event": "http_fetch_403_retrying_scraperapi", "url": url},
                        )
                        try:
                            html = self._fetch_html_via_scraperapi(url)
                            used_scraperapi = True
                        except Exception as sa_exc:
                            logger.warning(
                                "scraperapi_also_failed",
                                extra={"event": "scraperapi_also_failed", "url": url, "error": str(sa_exc)},
                            )
                            page1_blocked += 1
                            break

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

                if used_scraperapi:
                    logger.info("scraperapi_fetch_ok", extra={"event": "scraperapi_fetch_ok", "url": url})

                batch = parse_listings_page(self._s, html, url)
                logger.info(
                    "http_page_parsed",
                    extra={"event": "http_page_parsed", "url": url, "page": page, "count": len(batch)},
                )
                for x in batch:
                    merged[x.stable_id] = x

                if not batch:
                    break

        if page1_blocked == len(base_urls) and not merged:
            raise AllSourcesBlocked(
                f"All {page1_blocked} search URL(s) returned 403 on page 1 — "
                "site is blocking this IP. Add SCRAPER_API_KEY secret to enable ScraperAPI fallback."
            )

        return list(merged.values())
