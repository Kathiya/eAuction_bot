from __future__ import annotations

import logging

from project.cache.store import CacheFileLock, ListingCacheStore, diff_listings
from project.config.settings import Settings
from project.filters.engine import FilterEngine, load_listing_filter_from_path
from project.filters.models import PropertyListing
from project.notifier.telegram import TelegramNotifier, format_full_digest_html
from project.scraper.http_client import AllSourcesBlocked, HttpListingSource

logger = logging.getLogger(__name__)


def run_cycle(settings: Settings) -> int:
    """
    One monitoring cycle. Returns number of notifications attempted (success or fail logged separately).
    """
    store = ListingCacheStore(settings.listing_cache_path, settings.source_site)
    lock = CacheFileLock(store.lock_path())
    fetch_fn = HttpListingSource(settings).fetch_pages

    listing_filter = load_listing_filter_from_path(settings.filters_json_path)
    engine = FilterEngine(listing_filter)
    notifier = TelegramNotifier(
        settings.telegram_bot_token or "",
        settings.parsed_telegram_chat_ids(),
    )

    with lock:
        previous = store.load()
        cache_was_empty = len(previous) == 0
        logger.info(
            "cache_loaded",
            extra={
                "event": "cache_loaded",
                "path": str(store.path),
                "count": len(previous),
            },
        )

        try:
            raw = fetch_fn()
        except AllSourcesBlocked as e:
            logger.warning(
                "scrape_all_blocked",
                extra={
                    "event": "scrape_all_blocked",
                    "error": str(e),
                    "cached_count": len(previous),
                },
            )
            return 0
        except Exception as e:
            logger.exception(
                "scrape_failed",
                extra={"event": "scrape_failed", "error": str(e)},
            )
            raise

        logger.info(
            "scrape_complete",
            extra={"event": "scrape_complete", "raw_count": len(raw)},
        )

        filtered = engine.apply(raw)
        current: dict[str, PropertyListing] = {}
        for x in filtered:
            xh = x.with_content_hash()
            current[xh.stable_id] = xh

        new_items, changed_items = diff_listings(
            previous,
            current,
            notify_on_content_change=settings.notify_on_content_change,
        )

        to_notify: list[PropertyListing] = list(new_items)
        if settings.notify_on_content_change:
            to_notify.extend(changed_items)

        if cache_was_empty and not settings.notify_on_first_run:
            to_notify = []
            logger.info(
                "skip_notify_first_run",
                extra={"event": "skip_notify_first_run", "cache_empty": True},
            )

        notify_count = 0
        notify_errors = 0
        for item in to_notify:
            logger.info(
                "new_listing",
                extra={
                    "event": "new_listing",
                    "stable_id": item.stable_id,
                    "title": item.title,
                    "city": item.city,
                },
            )
            if notifier.enabled:
                try:
                    notifier.send_listing(item)
                    notify_count += 1
                except Exception as e:
                    notify_errors += 1
                    logger.error(
                        "notify_failed",
                        extra={
                            "event": "notify_failed",
                            "channel": "telegram",
                            "stable_id": item.stable_id,
                            "error": str(e),
                        },
                    )
            else:
                logger.warning(
                    "notify_skipped_no_telegram",
                    extra={"event": "notify_skipped_no_telegram", "stable_id": item.stable_id},
                )

        if notifier.enabled and notify_errors > 0:
            logger.error(
                "cache_unchanged_due_to_notify_errors",
                extra={
                    "event": "cache_unchanged_due_to_notify_errors",
                    "errors": notify_errors,
                },
            )
        else:
            store.save(current)
            logger.info(
                "cache_updated",
                extra={
                    "event": "cache_updated",
                    "path": str(store.path),
                    "count": len(current),
                },
            )
            if notifier.enabled and settings.telegram_send_full_digest_each_run:
                try:
                    digest_html = format_full_digest_html(settings, current)
                    notifier.send_html_multipart(digest_html)
                except Exception:
                    logger.exception(
                        "telegram_digest_failed",
                        extra={"event": "telegram_digest_failed"},
                    )

    return notify_count


def run_cycle_logged(settings: Settings) -> None:
    logger.info("poll_tick", extra={"event": "poll_tick"})
    run_cycle(settings)
