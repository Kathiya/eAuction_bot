from __future__ import annotations

import html
import logging

import httpx

from project.filters.models import PropertyListing

logger = logging.getLogger(__name__)


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
