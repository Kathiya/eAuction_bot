import logging
import sys
from typing import Any

from pythonjsonlogger import jsonlogger


def configure_logging(json_logs: bool) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    if json_logs:
        fmt = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            rename_fields={"levelname": "level", "asctime": "ts"},
        )
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    handler.setFormatter(fmt)
    root.addHandler(handler)


def log_extra(**kwargs: Any) -> dict[str, Any]:
    return kwargs
