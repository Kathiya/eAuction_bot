from __future__ import annotations

import html
import logging
import re
import time
from typing import Any

import httpx

from project.config.settings import Settings
from project.filters.engine import load_listing_filter_from_path, save_listing_filter_to_path
from project.filters.models import ListingFilter

logger = logging.getLogger(__name__)


def _split_list_arg(s: str) -> list[str]:
    s = s.strip()
    if not s:
        return []
    if "|" in s or "," in s:
        normalized = s.replace(",", "|")
        return [x.strip() for x in normalized.split("|") if x.strip()]
    return [s]


def _normalize_cmd(text: str) -> tuple[str, str]:
    """Return (command_without_slash_and_bot_suffix, rest_of_line)."""
    t = text.strip()
    if not t.startswith("/"):
        return "", ""
    space = t.find(" ")
    if space < 0:
        head = t
        rest = ""
    else:
        head, rest = t[:space], t[space + 1 :].strip()
    cmd = head.split("@", 1)[0].lower()
    if cmd.startswith("/"):
        cmd = cmd[1:]
    return cmd, rest


def _format_filters(f: ListingFilter) -> str:
    kw_mode = f"({f.keyword_match_mode.upper()} — {'any one' if f.keyword_match_mode == 'any' else 'all'} must match)"
    lines = [
        "<b>Current filters</b>",
        f"States: {', '.join(f.states) or '(any)'}",
        f"Cities: {', '.join(f.cities) or '(any)'}",
        f"Districts: {', '.join(f.districts) or '(any)'}",
        f"Property types: {', '.join(f.property_types) or '(any)'}",
        f"Keywords: {', '.join(f.keywords) or '(any)'} {kw_mode if f.keywords else ''}",
        f"Price min INR: {f.price_min_inr if f.price_min_inr is not None else '—'}",
        f"Price max INR: {f.price_max_inr if f.price_max_inr is not None else '—'}",
        "",
        "<i>Empty lists mean no restriction on that field.</i>",
    ]
    return "\n".join(lines)


class FilterCommandBot:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._token = (settings.telegram_bot_token or "").strip()
        self._allowed = {str(x).strip() for x in settings.parsed_telegram_chat_ids() if str(x).strip()}
        self._path = settings.filters_json_path
        self._offset = 0

    @property
    def enabled(self) -> bool:
        return bool(self._token and self._allowed)

    def _api(self, method: str, **params: Any) -> dict[str, Any]:
        url = f"https://api.telegram.org/bot{self._token}/{method}"
        with httpx.Client(timeout=60.0) as client:
            r = client.post(url, json=params)
            r.raise_for_status()
            body = r.json()
        if not body.get("ok"):
            raise RuntimeError(str(body))
        return body

    def _send(self, chat_id: int | str, text: str) -> None:
        self._api(
            "sendMessage",
            chat_id=chat_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

    def _load(self) -> ListingFilter:
        return load_listing_filter_from_path(self._path)

    def _save(self, filt: ListingFilter) -> None:
        save_listing_filter_to_path(self._path, filt)
        logger.info("filters_saved", extra={"event": "filters_saved", "path": self._path})

    def _deny(self, chat_id: int | str) -> None:
        self._send(chat_id, "Not authorized to change filters.")

    def handle_message(self, chat_id: int, text: str) -> None:
        if str(chat_id) not in self._allowed:
            logger.warning("telegram_filter_bot_unauthorized", extra={"chat_id": chat_id})
            self._deny(chat_id)
            return

        cmd, rest = _normalize_cmd(text)
        if cmd in ("start", "help"):
            self._send(chat_id, _help_text())
            return

        if cmd == "filters":
            f = self._load()
            self._send(chat_id, _format_filters(f))
            return

        if cmd == "setstates":
            items = _split_list_arg(rest)
            f = self._load()
            f2 = f.model_copy(update={"states": items})
            self._save(f2)
            self._send(chat_id, f"States set to: {items or ['(any — empty list)']}")
            return

        if cmd == "setcities":
            items = _split_list_arg(rest)
            f = self._load()
            f2 = f.model_copy(update={"cities": items})
            self._save(f2)
            self._send(chat_id, f"Cities set to: {items or ['(any — empty list)']}")
            return

        if cmd == "addstate":
            if not rest.strip():
                self._send(chat_id, "Usage: <code>/addstate Karnataka</code>")
                return
            f = self._load()
            new_states = list(dict.fromkeys([*f.states, rest.strip()]))
            f2 = f.model_copy(update={"states": new_states})
            self._save(f2)
            self._send(chat_id, f"States: {', '.join(new_states)}")
            return

        if cmd == "addcity":
            if not rest.strip():
                self._send(chat_id, "Usage: <code>/addcity Bengaluru</code>")
                return
            f = self._load()
            new_cities = list(dict.fromkeys([*f.cities, rest.strip()]))
            f2 = f.model_copy(update={"cities": new_cities})
            self._save(f2)
            self._send(chat_id, f"Cities: {', '.join(new_cities)}")
            return

        if cmd == "clearstates":
            f = self._load()
            self._save(f.model_copy(update={"states": []}))
            self._send(chat_id, "States cleared (any state).")
            return

        if cmd == "clearcities":
            f = self._load()
            self._save(f.model_copy(update={"cities": []}))
            self._send(chat_id, "Cities cleared (any city).")
            return

        if cmd == "resetfilters":
            self._save(ListingFilter())
            self._send(chat_id, "All filters reset to defaults (no restrictions).")
            return

        if cmd:
            self._send(chat_id, "Unknown command. Send /help")
            return

    def run_forever(self) -> None:
        if not self.enabled:
            raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID for authorized user(s).")
        logger.info(
            "telegram_filter_bot_started",
            extra={"event": "telegram_filter_bot_started", "path": self._path},
        )
        while True:
            try:
                with httpx.Client(timeout=45.0) as client:
                    url = f"https://api.telegram.org/bot{self._token}/getUpdates"
                    r = client.get(
                        url,
                        params={"offset": self._offset, "timeout": 35},
                    )
                    r.raise_for_status()
                    body = r.json()
                if not body.get("ok"):
                    logger.error("getUpdates_failed", extra={"body": str(body)[:300]})
                    time.sleep(3)
                    continue
                for u in body.get("result", []):
                    self._offset = u["update_id"] + 1
                    msg = u.get("message") or u.get("edited_message")
                    if not msg:
                        continue
                    chat = msg.get("chat") or {}
                    cid = chat.get("id")
                    if cid is None:
                        continue
                    text = (msg.get("text") or "").strip()
                    if not text.startswith("/"):
                        continue
                    try:
                        self.handle_message(int(cid), text)
                    except Exception as e:
                        logger.exception("telegram_filter_bot_handler_error")
                        try:
                            self._send(cid, html.escape(f"Error: {e}")[:3500])
                        except Exception:
                            pass
            except Exception as e:
                logger.warning("telegram_filter_bot_poll_error", extra={"error": str(e)})
                time.sleep(3)


def _help_text() -> str:
    return (
        "<b>Listing filter bot</b>\n\n"
        "Only chats in <code>TELEGRAM_CHAT_ID</code> can use these.\n\n"
        "<b>View</b>\n"
        "/filters — show current rules\n\n"
        "<b>State</b>\n"
        "/setstates Karnataka, Maharashtra — replace list\n"
        "/addstate Goa — append one\n"
        "/clearstates — allow any state\n\n"
        "<b>City</b>\n"
        "/setcities Bengaluru, Mumbai — replace list\n"
        "/addcity Pune — append one\n"
        "/clearcities — allow any city\n\n"
        "<b>Other</b>\n"
        "/resetfilters — clear all filter fields\n"
        "/help — this message\n\n"
        "<i>Use spelling that matches the listing site (city often comes from the title).</i>"
    )


def run_filter_command_bot(settings: Settings) -> None:
    FilterCommandBot(settings).run_forever()
