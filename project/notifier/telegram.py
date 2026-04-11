from __future__ import annotations

import html
import logging
import httpx

from project.config.settings import Settings
from project.filters.models import PropertyListing

logger = logging.getLogger(__name__)

_TELEGRAM_HTML_SAFE = 3800


def format_full_digest_html(settings: Settings, current: dict[str, PropertyListing]) -> str:
    """
    Proof-oriented summary: exact search URLs (site-side filters) + every listing row after JSON filters.
    """
    lines: list[str] = [
        "<b>Full scrape digest</b>",
        f"Total listings in cache after this run: <b>{len(current)}</b>",
        "",
        "<b>Search URLs</b> (eAuctions only returns what these queries allow; "
        "default = residential Ahmedabad Gujarat + vehicles statewide Gujarat):",
    ]
    bases = settings.parsed_listing_search_urls()
    if not bases:
        lines.append(html.escape("(legacy single URL mode — check LISTING_PAGE_URL / template)"))
    for i, u in enumerate(bases, 1):
        lines.append(f"{i}. <code>{html.escape(u)}</code>")
    lines.extend(["", "<b>Every row</b> (after config/filters.json — empty lists = no extra drop):", ""])
    by_label: dict[str, list[PropertyListing]] = {}
    for p in current.values():
        lab = p.stream_label or "—"
        by_label.setdefault(lab, []).append(p)
    for lab in sorted(by_label.keys()):
        lines.append(f"<i>{html.escape(lab)}</i> ({len(by_label[lab])})")
        for p in sorted(by_label[lab], key=lambda x: x.stable_id):
            loc = ", ".join(x for x in [p.city, p.state] if x) or "—"
            t = (p.title or "")[:220]
            lines.append(
                f"• <code>{html.escape(p.stable_id)}</code> {html.escape(t)} "
                f"<i>({html.escape(loc)})</i>"
            )
        lines.append("")
    return "\n".join(lines).strip()


def _chunk_html_message(text: str, limit: int = _TELEGRAM_HTML_SAFE) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    buf: list[str] = []
    n = 0
    for line in text.split("\n"):
        add = len(line) + (1 if buf else 0)
        if buf and n + add > limit:
            chunks.append("\n".join(buf))
            buf = [line]
            n = len(line)
            continue
        if not buf:
            n = 0
        buf.append(line)
        n += add
    if buf:
        chunks.append("\n".join(buf))
    out: list[str] = []
    for c in chunks:
        while len(c) > limit:
            out.append(c[:limit])
            c = c[limit:]
        out.append(c)
    return out


def format_listing_message(listing: PropertyListing) -> str:
    t = html.escape(listing.title or "")
    price = html.escape(listing.price_display or "N/A")
    lines = [
        f"<b>{t}</b>",
        f"Price: {price}",
    ]
    if listing.stream_label:
        lines.append(f"Stream: {html.escape(listing.stream_label)}")
    if listing.state or listing.city:
        loc = ", ".join(
            html.escape(x) for x in [listing.city, listing.state] if x
        )
        lines.append(f"Location: {loc}")
    if listing.district:
        lines.append(f"District: {html.escape(listing.district)}")
    sid = html.escape(listing.stable_id)
    lines.append(f"Property ID: <code>{sid}</code>")
    safe_url = html.escape(listing.url or "", quote=True)
    lines.append(f'<a href="{safe_url}">Open listing</a>')
    return "\n".join(lines)


def _coerce_chat_id(chat_id: str) -> str | int:
    s = chat_id.strip()
    if s.startswith("-"):
        tail = s[1:]
        if tail.isdigit():
            return int(s)
        return s
    if s.isdigit():
        return int(s)
    return s


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_ids: list[str]) -> None:
        self._token = bot_token
        self._chat_ids = [c for c in chat_ids if c]

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._chat_ids)

    def send_listing(self, listing: PropertyListing) -> None:
        if not self.enabled:
            raise RuntimeError("Telegram notifier is not configured")
        text = format_listing_message(listing)
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        with httpx.Client(timeout=30.0) as client:
            for chat_id in self._chat_ids:
                payload = {
                    "chat_id": _coerce_chat_id(chat_id),
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False,
                }
                r = client.post(url, json=payload)
                if r.is_error:
                    detail = r.text
                    raise RuntimeError(
                        f"Telegram API {r.status_code}: {detail[:500]}"
                    ) from None
                r.raise_for_status()
                body = r.json()
                if not body.get("ok"):
                    raise RuntimeError(str(body))
                logger.info(
                    "notify_sent",
                    extra={
                        "event": "notify_sent",
                        "channel": "telegram",
                        "stable_id": listing.stable_id,
                        "chat_id": chat_id,
                    },
                )

    def send_html_multipart(self, html_body: str) -> None:
        """Send long HTML as multiple Telegram messages (same chats as listings)."""
        if not self.enabled:
            raise RuntimeError("Telegram notifier is not configured")
        parts = _chunk_html_message(html_body)
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        with httpx.Client(timeout=60.0) as client:
            for chat_id in self._chat_ids:
                for i, part in enumerate(parts):
                    header = f"<i>Digest {i + 1}/{len(parts)}</i>\n\n" if len(parts) > 1 else ""
                    payload = {
                        "chat_id": _coerce_chat_id(chat_id),
                        "text": header + part,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    }
                    r = client.post(url, json=payload)
                    if r.is_error:
                        raise RuntimeError(
                            f"Telegram API {r.status_code}: {r.text[:500]}"
                        ) from None
                    r.raise_for_status()
                    body = r.json()
                    if not body.get("ok"):
                        raise RuntimeError(str(body))
                    logger.info(
                        "digest_part_sent",
                        extra={
                            "event": "digest_part_sent",
                            "channel": "telegram",
                            "part": i + 1,
                            "parts": len(parts),
                            "chat_id": chat_id,
                        },
                    )
