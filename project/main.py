from __future__ import annotations

import argparse
import atexit
import signal
import sys
import time

from project.config.settings import get_settings
from project.logging_setup import configure_logging
from project.pipeline import run_cycle, run_cycle_logged
from project.scheduler.jobs import build_scheduler, shutdown_scheduler


def main() -> None:
    parser = argparse.ArgumentParser(description="Bank auction property listing monitor")
    parser.add_argument(
        "command",
        choices=["run-once", "daemon", "serve-api", "filter-bot"],
        help="Run mode",
    )
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_json)

    if args.command == "run-once":
        try:
            run_cycle(settings)
        except Exception:
            sys.exit(1)
        return

    if args.command == "daemon":
        sched = build_scheduler(settings.poll_interval_seconds, lambda: run_cycle_logged(settings))
        sched.start()

        def _stop(*_a: object) -> None:
            shutdown_scheduler(sched)
            sys.exit(0)

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
        atexit.register(lambda: shutdown_scheduler(sched))

        while True:
            time.sleep(3600)

    if args.command == "serve-api":
        import uvicorn

        uvicorn.run(
            "project.api.app:app",
            host=settings.api_host,
            port=settings.api_port,
            reload=False,
        )

    if args.command == "filter-bot":
        from project.telegram_bot.filter_commands import run_filter_command_bot

        run_filter_command_bot(settings)


if __name__ == "__main__":
    main()
