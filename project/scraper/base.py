from __future__ import annotations

from typing import Protocol

from project.filters.models import PropertyListing


class ListingSource(Protocol):
    def fetch_pages(self) -> list[PropertyListing]:
        ...


def fetch_all_listings(source: ListingSource) -> list[PropertyListing]:
    return source.fetch_pages()
