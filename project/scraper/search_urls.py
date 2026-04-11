from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from project.config.settings import Settings


def _with_page(url: str, page: int) -> str:
    parts = urlparse(url.strip())
    q = list(parse_qsl(parts.query, keep_blank_values=True))
    keys = {k for k, _ in q}
    if "page" in keys:
        q = [(k, v) for k, v in q if k != "page"]
    q.append(("page", str(page)))
    new_query = urlencode(q)
    return urlunparse((parts.scheme, parts.netloc, parts.path, parts.params, new_query, parts.fragment))


def build_fetch_urls(settings: Settings) -> list[str]:
    bases = settings.parsed_listing_search_urls()
    if bases:
        urls: list[str] = []
        for base in bases:
            for p in range(1, settings.max_pages_per_run + 1):
                urls.append(_with_page(base, p))
        return urls
    if settings.listing_page_url_template and "{page}" in settings.listing_page_url_template:
        return [
            settings.listing_page_url_template.format(page=p)
            for p in range(1, settings.max_pages_per_run + 1)
        ]
    return [settings.listing_page_url]
