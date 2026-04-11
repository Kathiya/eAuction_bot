from __future__ import annotations

from project.config.settings import Settings
from project.filters.models import PropertyListing
from project.scraper.eauctionsindia_html import parse_eauctionsindia_html


def resolved_portal(settings: Settings) -> str:
    """Always eAuctions India (Baanknet support removed)."""
    if settings.listing_portal == "eauctionsindia":
        return "eauctionsindia"
    return "eauctionsindia"


def parse_listings_page(settings: Settings, html: str, page_url: str) -> list[PropertyListing]:
    resolved_portal(settings)
    return parse_eauctionsindia_html(html, page_url)
