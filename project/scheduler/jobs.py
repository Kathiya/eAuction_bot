from __future__ import annotations

import logging
from collections.abc import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger(__name__)


def build_scheduler(interval_seconds: int, job: Callable[[], None]) -> BackgroundScheduler:
    sched = BackgroundScheduler()
    sched.add_job(
        job,
        trigger=IntervalTrigger(seconds=interval_seconds),
        id="poll_listings",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    return sched


def shutdown_scheduler(sched: BackgroundScheduler) -> None:
    try:
        sched.shutdown(wait=False)
    except Exception as e:
        logger.warning(
            "scheduler_shutdown_error",
            extra={"event": "scheduler_shutdown_error", "error": str(e)},
        )
