"""Run from repo root: python telegram_test.py"""

from project.config.settings import get_settings
from project.filters.models import PropertyListing
from project.notifier.telegram import TelegramNotifier


def main() -> None:
    s = get_settings()
    n = TelegramNotifier(s.telegram_bot_token or "", s.parsed_telegram_chat_ids())
    if not n.enabled:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env (repo root).")
    try:
        n.send_listing(
            PropertyListing(
                stable_id="TEST-001",
                url="https://www.eauctionsindia.com/search",
                title="Baanknet monitor - Telegram test",
                price_display="Test (not a real listing)",
                state="Test",
                city="Test",
            )
        )
    except RuntimeError as e:
        err = str(e).lower()
        if "chat not found" in err:
            raise SystemExit(
                "Telegram: chat not found.\n"
                "1) Open Telegram and find THIS bot (same token as in .env).\n"
                "2) Tap Start and send any message (e.g. /start).\n"
                "3) Confirm TELEGRAM_CHAT_ID: message @userinfobot or open\n"
                "   https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates\n"
                "   and copy chat.id from the latest message.\n"
                "4) Run this script again."
            ) from e
        raise
    print("OK: Check Telegram for the test message.")


if __name__ == "__main__":
    main()
