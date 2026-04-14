from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import portalocker

from project.filters.models import PropertyListing


def diff_listings(
    previous: dict[str, PropertyListing],
    current: dict[str, PropertyListing],
    *,
    notify_on_content_change: bool,
) -> tuple[list[PropertyListing], list[PropertyListing]]:
    """
    Returns (new_listings, changed_listings) where changed is only used if notify_on_content_change.
    """
    prev_ids = set(previous.keys())
    cur_ids = set(current.keys())
    new_ids = cur_ids - prev_ids
    new_listings = [current[i] for i in sorted(new_ids)]

    changed: list[PropertyListing] = []
    if notify_on_content_change:
        for sid in sorted(cur_ids & prev_ids):
            a = previous[sid]
            b = current[sid]
            ha = a.content_hash or a.with_content_hash().content_hash
            hb = b.content_hash or b.with_content_hash().content_hash
            if ha != hb:
                changed.append(b)

    return new_listings, changed


def _decode_listings_object(listings_raw: Any) -> dict[str, PropertyListing]:
    if not isinstance(listings_raw, dict):
        return {}
    out: dict[str, PropertyListing] = {}
    for sid, row in listings_raw.items():
        if not isinstance(row, dict):
            continue
        try:
            out[str(sid)] = PropertyListing.from_cache_dict(row)
        except Exception:
            continue
    return out


def parse_cache_json_text(raw_text: str) -> tuple[dict[str, Any], dict[str, PropertyListing]]:
    """
    Parse a listings_cache.json body (e.g. from GitHub raw or local file).
    Returns (metadata keys: version, source_site, updated_at), listings by stable_id).
    """
    data = json.loads(raw_text)
    meta: dict[str, Any] = {
        "version": data.get("version"),
        "source_site": data.get("source_site"),
        "updated_at": data.get("updated_at"),
    }
    return meta, _decode_listings_object(data.get("listings"))


@dataclass
class SnapshotEnvelope:
    version: int
    source_site: str
    updated_at: str
    listings: dict[str, PropertyListing]


class ListingCacheStore:
    def __init__(self, path: str | Path, source_site: str) -> None:
        self._path = Path(path)
        self._source_site = source_site

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict[str, PropertyListing]:
        import logging
        _log = logging.getLogger(__name__)

        if not self._path.is_file():
            return {}
        try:
            raw_text = self._path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
        except json.JSONDecodeError:
            bad = self._path.with_suffix(self._path.suffix + ".corrupt")
            shutil.copy2(self._path, bad)
            try:
                self._path.unlink()
            except OSError:
                pass
            _log.critical(
                "cache_corrupt_reset",
                extra={
                    "event": "cache_corrupt_reset",
                    "backup": str(bad),
                    "action": "starting_fresh",
                },
            )
            return {}

        return _decode_listings_object(data.get("listings"))

    def save(self, listings: dict[str, PropertyListing]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        env = {
            "version": 1,
            "source_site": self._source_site,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "listings": {k: v.to_cache_dict() for k, v in sorted(listings.items())},
        }
        payload = json.dumps(env, ensure_ascii=False, indent=2)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(self._path.parent),
            prefix=self._path.name + ".",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, self._path)
        finally:
            if os.path.isfile(tmp_name):
                try:
                    os.remove(tmp_name)
                except OSError:
                    pass

    def lock_path(self) -> Path:
        return self._path.with_suffix(self._path.suffix + ".lock")


class CacheFileLock:
    """Exclusive lock for read-modify-write cycles (optional)."""

    def __init__(self, lock_path: Path) -> None:
        self._lock_path = lock_path
        self._fh: Any = None

    def __enter__(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._lock_path, "a+", encoding="utf-8")
        portalocker.lock(self._fh, portalocker.LOCK_EX)

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._fh:
            try:
                portalocker.unlock(self._fh)
            except Exception:
                pass
            self._fh.close()
            self._fh = None
