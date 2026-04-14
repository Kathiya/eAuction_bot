import hashlib
import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


def compute_content_hash(
    title: str,
    price_display: str,
    url: str,
    state: str | None,
    city: str | None,
    district: str | None = None,
    property_type: str | None = None,
    bank: str | None = None,
    auction_end_date: str | None = None,
) -> str:
    raw = "|".join(
        [
            title.strip().lower(),
            price_display.strip().lower(),
            url.strip().lower(),
            (state or "").strip().lower(),
            (city or "").strip().lower(),
            (district or "").strip().lower(),
            (property_type or "").strip().lower(),
            (bank or "").strip().lower(),
            (auction_end_date or "").strip().lower(),
        ]
    )
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


class PropertyListing(BaseModel):
    stable_id: str
    url: str
    title: str
    price_display: str = ""
    price_inr: float | None = None
    state: str | None = None
    city: str | None = None
    district: str | None = None
    property_type: str | None = None
    bank: str | None = None
    auction_end_date: str | None = None
    content_hash: str | None = None
    stream_label: str | None = None
    first_seen_at: str | None = None

    model_config = {"frozen": False}

    @property
    def keywords_text(self) -> str:
        parts = [
            self.title,
            self.property_type or "",
            self.state or "",
            self.city or "",
            self.district or "",
            self.bank or "",
        ]
        return " ".join(p for p in parts if p).lower()

    def with_content_hash(self) -> "PropertyListing":
        h = compute_content_hash(
            self.title,
            self.price_display,
            self.url,
            self.state,
            self.city,
            self.district,
            self.property_type,
            self.bank,
            self.auction_end_date,
        )
        clone = self.model_copy(update={"content_hash": h})
        return clone

    def to_cache_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_cache_dict(cls, data: dict[str, Any]) -> "PropertyListing":
        return cls.model_validate(data)


class ListingFilter(BaseModel):
    states: list[str] = Field(default_factory=list)
    cities: list[str] = Field(default_factory=list)
    districts: list[str] = Field(default_factory=list)
    property_types: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    # "any" = at least one keyword must match (OR); "all" = every keyword must match (AND)
    keyword_match_mode: Literal["any", "all"] = "any"
    price_min_inr: float | None = None
    price_max_inr: float | None = None

    @field_validator("states", "cities", "districts", "property_types", "keywords", mode="before")
    @classmethod
    def lower_strip_list(cls, v: object) -> list[str]:
        if v is None:
            return []
        if not isinstance(v, list):
            return []
        out: list[str] = []
        for x in v:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                out.append(s)
        return out

    def matches(self, listing: PropertyListing) -> bool:
        if self.states:
            st = (listing.state or "").strip().lower()
            allowed = {x.strip().lower() for x in self.states}
            if st not in allowed:
                return False
        if self.cities:
            ct = (listing.city or "").strip().lower()
            allowed = {x.strip().lower() for x in self.cities}
            if ct not in allowed:
                return False
        if self.districts:
            d = (listing.district or "").strip().lower()
            allowed = {x.strip().lower() for x in self.districts}
            if d not in allowed:
                return False
        if self.property_types:
            pt = (listing.property_type or "").strip().lower()
            allowed = {x.strip().lower() for x in self.property_types}
            if pt not in allowed:
                return False
        if self.keywords:
            blob = listing.keywords_text
            normalized = [kw.strip().lower() for kw in self.keywords]
            if self.keyword_match_mode == "all":
                if not all(kw in blob for kw in normalized):
                    return False
            else:
                if not any(kw in blob for kw in normalized):
                    return False
        if self.price_min_inr is not None:
            if listing.price_inr is None or listing.price_inr < self.price_min_inr:
                return False
        if self.price_max_inr is not None:
            if listing.price_inr is None or listing.price_inr > self.price_max_inr:
                return False
        return True


_INR_RE = re.compile(
    r"₹\s*([\d.,]+)\s*(Lac|Cr|Crore|Lakh|Lacs)?",
    re.IGNORECASE,
)


def parse_price_inr(price_display: str) -> float | None:
    if not price_display:
        return None
    m = _INR_RE.search(price_display.replace(",", ""))
    if not m:
        num_match = re.search(r"([\d.]+)\s*(Lac|Cr|Crore|Lakh|Lacs)?", price_display, re.I)
        if not num_match:
            return None
        amount = float(num_match.group(1))
        unit = (num_match.group(2) or "").lower()
    else:
        amount = float(m.group(1).replace(",", ""))
        unit = (m.group(2) or "").lower()

    if unit in ("lac", "lakh", "lacs"):
        return amount * 100_000
    if unit in ("cr", "crore"):
        return amount * 10_000_000
    if amount < 1_000 and "lac" in price_display.lower():
        return amount * 100_000
    if amount < 1_000 and "cr" in price_display.lower():
        return amount * 10_000_000
    return amount
