from __future__ import annotations

import re
from html import unescape
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from project.filters.models import PropertyListing, parse_price_inr

_BASE = "https://www.eauctionsindia.com"
_AUCTION_ID_RE = re.compile(r"Auction\s+ID\s*:?\s*#\s*(\d+)", re.I)
_RESERVE_RE = re.compile(
    r"Reserve\s+Price\s*:\s*(₹\s*[\d.,]+(?:\.\d+)?)",
    re.I,
)
_HREF_VIEW = re.compile(r'href=(["\'])([^"\']*)\1', re.I)


def _title_from_chunk(chunk: str) -> str:
    soup = BeautifulSoup(chunk, "html.parser")
    for tag_name in ("h5", "h4", "h3"):
        el = soup.find(tag_name)
        if el:
            t = unescape(el.get_text(" ", strip=True))
            if t and len(t) > 3:
                return t
    return ""


def _price_from_chunk(chunk: str) -> str:
    m = _RESERVE_RE.search(chunk)
    if m:
        return m.group(1).strip()
    return ""


def _detail_url(chunk: str, page_url: str) -> str:
    for m in _HREF_VIEW.finditer(chunk):
        href = unescape(m.group(2).strip())
        if not href or href.startswith("#") or "javascript:" in href.lower():
            continue
        low = href.lower()
        if "view" in low or "property" in low or "auction" in low or "detail" in low:
            return urljoin(_BASE, href)
    for m in _HREF_VIEW.finditer(chunk):
        href = unescape(m.group(2).strip())
        hl = href.lower()
        if href.startswith("/") and "search" not in hl and "login" not in hl:
            return urljoin(_BASE, href)
    return page_url


def _hints_from_page_url(page_url: str) -> tuple[str | None, str | None]:
    q = parse_qs(urlparse(page_url).query)
    st = (q.get("state") or [None])[0]
    ct = (q.get("city") or [None])[0]
    state = st.strip().title() if st and st.strip() else None
    city = ct.strip().title() if ct and ct.strip() else None
    return state, city


def _stream_label_from_search_url(page_url: str) -> str | None:
    """Human-readable filter tag for Telegram (from LISTING_SEARCH_URLS query)."""
    q = parse_qs(urlparse(page_url).query)
    cat_raw = (q.get("category") or [""])[0].strip().lower()
    st = (q.get("state") or [None])[0]
    ct = (q.get("city") or [None])[0]
    state = st.strip().title() if st and str(st or "").strip() else ""
    city = ct.strip().title() if ct and str(ct or "").strip() else ""

    if cat_raw == "residential":
        parts = ["Residential"]
        if city:
            parts.append(city)
        if state:
            parts.append(state)
        return " · ".join(parts)
    if cat_raw == "vehicle-auctions":
        return f"Vehicles · {state}" if state else "Vehicles"
    if cat_raw:
        return cat_raw.replace("-", " ").title()
    return None


def _location_from_title(title: str) -> tuple[str | None, str | None]:
    m = re.search(r"\bin\s+(.+)$", title, re.I)
    if not m:
        return None, None
    tail = m.group(1).strip().rstrip(".")
    parts = [p.strip() for p in tail.split(",") if p.strip()]
    if not parts:
        return None, None
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[-1]


def parse_eauctionsindia_html(html: str, page_url: str) -> list[PropertyListing]:
    if not html or "Auction ID" not in html:
        return []

    out: list[PropertyListing] = []
    seen: set[str] = set()

    for m in _AUCTION_ID_RE.finditer(html):
        aid = m.group(1).strip()
        stable_id = f"ei_{aid}"
        if stable_id in seen:
            continue
        seen.add(stable_id)

        pos = m.start()
        chunk = html[max(0, pos - 2200) : min(len(html), pos + 900)]

        title = _title_from_chunk(chunk) or f"Auction {aid}"
        price_display = _price_from_chunk(chunk)
        url = _detail_url(chunk, page_url)
        city, district = _location_from_title(title)
        url_state, url_city = _hints_from_page_url(page_url)
        if url_state:
            state = url_state
        else:
            state = None
        if url_city:
            city = url_city or city

        stream_label = _stream_label_from_search_url(page_url)

        prop_type: str | None = None
        mpt = re.match(r"eAuction\s+(\w+(?:\s+\w+)?)\s+in\s", title, re.I)
        if mpt:
            prop_type = mpt.group(1).strip()

        price_inr = parse_price_inr(price_display) if price_display else None
        out.append(
            PropertyListing(
                stable_id=stable_id,
                url=url,
                title=title,
                price_display=price_display,
                price_inr=price_inr,
                state=state,
                city=city,
                district=district,
                property_type=prop_type,
                bank=None,
                stream_label=stream_label,
            ).with_content_hash()
        )

    return out
