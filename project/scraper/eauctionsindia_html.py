from __future__ import annotations

import re
from html import unescape
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from project.filters.models import PropertyListing, parse_price_inr

_BASE = "https://www.eauctionsindia.com"
_AUCTION_ID_RE = re.compile(r"Auction\s+ID\s*:?\s*#\s*(\d+)", re.I)
_RESERVE_RE = re.compile(
    r"Reserve\s+Price\s*[:\-]\s*(₹\s*[\d.,]+(?:\s*(?:Lac|Lakh|Lacs|Cr|Crore))?)",
    re.I,
)
_BANK_RE = re.compile(
    r"(?:Bank(?:\s+Name)?|Auctioned\s+by)\s*[:\-]\s*([A-Za-z][A-Za-z0-9 &,.\-']{2,60}?)(?:\s*[|<\n\r]|$)",
    re.I,
)
_DATE_RE = re.compile(
    r"(?:Auction\s+Date|Bid\s+(?:End\s+)?Date|Last\s+Date|E-Auction\s+Date)\s*[:\-]\s*"
    r"((?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{1,2}\s+\w{3,9}\s+\d{4}))",
    re.I,
)

# Tags that signal we have reached a structural boundary; stop walking up
_STOP_TAGS = {"body", "html", "main", "form"}
# Block-level tags that are plausible card containers
_BLOCK_TAGS = {"div", "article", "section", "li", "tr", "td", "table"}


def _find_card_container(node: Tag) -> Tag:
    """
    Walk up the DOM from the Auction-ID text node to find the tightest block
    element that wraps the entire listing card.

    Strategy:
    - Walk upward through parent tags.
    - Whenever we land on a block element (div, article, etc.), record it as
      the best candidate.
    - Stop early when the parent is a structural stop-tag (body, html, …) or
      when the parent contains ≥3x the text of the current element (meaning
      the parent spans multiple cards, so the current element is the card).
    """
    current: Tag = node.parent  # type: ignore[assignment]
    best: Tag = current

    for _ in range(14):
        if current is None or not isinstance(current, Tag):
            break
        tag_name = (current.name or "").lower()
        if tag_name in _STOP_TAGS:
            break

        if tag_name in _BLOCK_TAGS:
            current_len = len(current.get_text())
            parent = current.parent
            if parent and isinstance(parent, Tag):
                parent_name = (parent.name or "").lower()
                if parent_name in _STOP_TAGS:
                    # Parent is a document boundary — this block is as high as we go
                    best = current
                    break
                parent_len = len(parent.get_text())
                if current_len > 0 and parent_len >= current_len * 3:
                    # Parent spans multiple cards; current is the card container
                    best = current
                    break
            best = current

        current = current.parent  # type: ignore[assignment]

    return best


def _title_from_card(card: Tag) -> str:
    for tag_name in ("h5", "h4", "h3", "h2"):
        el = card.find(tag_name)
        if el and isinstance(el, Tag):
            t = unescape(el.get_text(" ", strip=True))
            if t and len(t) > 3:
                return t
    return ""


def _price_from_text(text: str) -> str:
    m = _RESERVE_RE.search(text)
    if m:
        return m.group(1).strip()
    return ""


def _bank_from_text(text: str) -> str | None:
    m = _BANK_RE.search(text)
    if not m:
        return None
    name = m.group(1).strip().rstrip(".,")
    # Skip obviously wrong matches (very short, or a URL fragment, etc.)
    if len(name) < 3 or "/" in name:
        return None
    return name


def _auction_date_from_text(text: str) -> str | None:
    m = _DATE_RE.search(text)
    if m:
        return m.group(1).strip()
    return None


def _detail_url_from_card(card: Tag, page_url: str) -> str:
    skip = {"#", "", "javascript:"}
    blocked = {"search", "login", "register", "signup"}
    for a in card.find_all("a", href=True):
        href = unescape(str(a["href"]).strip())
        if not href or any(href.lower().startswith(s) for s in skip):
            continue
        low = href.lower()
        if any(kw in low for kw in ("view", "property", "auction", "detail")):
            return urljoin(_BASE, href)
    for a in card.find_all("a", href=True):
        href = unescape(str(a["href"]).strip())
        low = href.lower()
        if href.startswith("/") and not any(kw in low for kw in blocked):
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


def parse_eauctionsindia_html(html_text: str, page_url: str) -> list[PropertyListing]:
    if not html_text or "Auction ID" not in html_text:
        return []

    soup = BeautifulSoup(html_text, "html.parser")
    out: list[PropertyListing] = []
    seen: set[str] = set()

    for text_node in soup.find_all(string=_AUCTION_ID_RE):
        m = _AUCTION_ID_RE.search(str(text_node))
        if not m:
            continue
        aid = m.group(1).strip()
        stable_id = f"ei_{aid}"
        if stable_id in seen:
            continue
        seen.add(stable_id)

        card = _find_card_container(text_node)  # type: ignore[arg-type]
        card_text = card.get_text(" ", strip=True)
        card_html = str(card)  # preserve tags so regex terminators (<, \n) work

        title = _title_from_card(card) or f"Auction {aid}"
        price_display = _price_from_text(card_text)
        url = _detail_url_from_card(card, page_url)
        bank = _bank_from_text(card_html)
        auction_end_date = _auction_date_from_text(card_html)

        city, district = _location_from_title(title)
        url_state, url_city = _hints_from_page_url(page_url)
        state = url_state or None
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
                bank=bank,
                auction_end_date=auction_end_date,
                stream_label=stream_label,
            ).with_content_hash()
        )

    return out
