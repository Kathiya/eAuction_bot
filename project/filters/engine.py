import json
import os
import tempfile
from pathlib import Path

from project.filters.models import ListingFilter, PropertyListing


class FilterEngine:
    def __init__(self, listing_filter: ListingFilter) -> None:
        self._f = listing_filter

    @property
    def filter_spec(self) -> ListingFilter:
        return self._f

    def apply(self, listings: list[PropertyListing]) -> list[PropertyListing]:
        return [x for x in listings if self._f.matches(x)]


def load_listing_filter_from_path(path: str | Path) -> ListingFilter:
    p = Path(path)
    if not p.is_file():
        return ListingFilter()
    raw = json.loads(p.read_text(encoding="utf-8"))
    return ListingFilter.model_validate(raw)


def save_listing_filter_to_path(path: str | Path, filt: ListingFilter) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(filt.model_dump(mode="json"), ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(p.parent),
        prefix=p.name + ".",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, p)
    finally:
        if os.path.isfile(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass
