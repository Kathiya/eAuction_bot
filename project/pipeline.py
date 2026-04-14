from __future__ import annotations

import logging
from datetime import datetime, timezone

from project.cache.store import CacheFileLock, ListingCacheStore, diff_listings
from project.config.settings import Settings
from project.filters.engine import FilterEngine, load_listing_filter_from_path
from project.filters.models import PropertyListing
from project.notifier.telegram import TelegramNotifier, format_full_digest_html
from project.scraper.http_client import AllSourcesBlocked, HttpListingSource

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_cycle(settings: Settings) -> int:
    """
    One monitoring cycle. Returns number of notifications sent successfully.

    Cache save strategy:
    - Items that failed to notify are excluded from the saved snapshot so they
      are retried as "new" on the next run.
    - Items already in the previous cache are always preserved regardless of
      notify errors (we only withhold newly-discovered items that failed).
    - This prevents both silent loss of alerts AND mass re-notification of
      already-known listings when Telegram has a transient error.
    """
    store = ListingCacheStore(settings.listing_cache_path, settings.source_site)
    lock = CacheFileLock(store.lock_path())
    fetch_fn = HttpListingSource(settings).fetch_pages

    listing_filter = load_listing_filter_from_path(settings.filters_json_path)
    engine = FilterEngine(listing_filter)

    with TelegramNotifier(
        settings.telegram_bot_token or "",
        settings.parsed_telegram_chat_ids(),
    ) as notifier:
        if not notifier.enabled:
            logger.warning(
                "telegram_disabled",
                extra={
                    "event": "telegram_disabled",
                    "reason": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing/empty",
                },
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
                if notifier.enabled:
                    try:
                        notifier.send_html_multipart(
                            f"⚠️ <b>eAuctions scrape blocked (403)</b>\n\n"
                            f"The site returned 403 for all search URLs — likely blocking the CI/cloud IP.\n"
                            f"Cached data ({len(previous)} listings) is unchanged."
                        )
                    except Exception:
                        logger.exception("notify_blocked_alert_failed", extra={"event": "notify_blocked_alert_failed"})
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
            now = _now_iso()
            current: dict[str, PropertyListing] = {}
            for x in filtered:
                xh = x.with_content_hash()
                # Preserve first_seen_at from cache; stamp it now for brand-new items
                prior = previous.get(xh.stable_id)
                first_seen = prior.first_seen_at if (prior and prior.first_seen_at) else now
                xh = xh.model_copy(update={"first_seen_at": first_seen})
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
            # Track IDs of new/changed items that failed notification so we can
            # exclude them from the cache save (they will be retried next run).
            failed_new_ids: set[str] = set()

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
                        failed_new_ids.add(item.stable_id)
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

            # Build the snapshot to persist:
            # - All previously-known listings are always kept (no churn for known items).
            # - New/changed listings are included only if notification succeeded.
            #   Failed ones stay out of the cache so the next run re-notifies them.
            saveable: dict[str, PropertyListing] = {
                sid: listing
                for sid, listing in current.items()
                if sid not in failed_new_ids or sid in previous
            }

            if failed_new_ids:
                logger.warning(
                    "cache_save_partial",
                    extra={
                        "event": "cache_save_partial",
                        "failed_ids": sorted(failed_new_ids),
                        "will_retry_count": len(failed_new_ids),
                    },
                )

            store.save(saveable)
            logger.info(
                "cache_updated",
                extra={
                    "event": "cache_updated",
                    "path": str(store.path),
                    "count": len(saveable),
                },
            )

            if notifier.enabled and settings.telegram_send_full_digest_each_run:
                try:
                    digest_html = format_full_digest_html(settings, saveable)
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
