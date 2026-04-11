from __future__ import annotations

from typing import Any

from project.filters.models import PropertyListing, parse_price_inr


def listings_from_api_payload(payload: Any, base_url: str) -> list[PropertyListing]:
    """
    Best-effort mapping for unknown JSON shapes (internal APIs).
    Looks for list of dicts or dict with common list keys.
    """
    items: list[dict[str, Any]] = []
    if isinstance(payload, list):
        items = [x for x in payload if isinstance(x, dict)]
    elif isinstance(payload, dict):
        for key in ("data", "results", "items", "properties", "list", "content"):
            v = payload.get(key)
            if isinstance(v, list):
                items = [x for x in v if isinstance(x, dict)]
                break
        if not items and isinstance(payload.get("records"), list):
            items = [x for x in payload["records"] if isinstance(x, dict)]

    out: list[PropertyListing] = []
    for row in items:
        sid = (
            row.get("propertyId")
            or row.get("property_id")
            or row.get("id")
            or row.get("code")
            or row.get("referenceNo")
        )
        if sid is None:
            continue
        stable_id = str(sid).strip()
        title = str(row.get("title") or row.get("propertyTitle") or row.get("name") or stable_id)
        url = str(row.get("url") or row.get("detailUrl") or row.get("link") or base_url)
        price_display = str(row.get("price") or row.get("reservePrice") or row.get("indicativePrice") or "")
        state = row.get("state") or row.get("stateName")
        city = row.get("city") or row.get("cityName")
        district = row.get("district") or row.get("districtName")
        prop_type = row.get("propertyType") or row.get("type")
        bank = row.get("bank") or row.get("bankName")
        st = str(state).strip() if state else None
        ct = str(city).strip() if city else None
        ds = str(district).strip() if district else None
        pt = str(prop_type).strip() if prop_type else None
        bk = str(bank).strip() if bank else None
        price_inr = parse_price_inr(price_display) if price_display else None
        pl = PropertyListing(
            stable_id=stable_id,
            url=url,
            title=title,
            price_display=price_display,
            price_inr=price_inr,
            state=st,
            city=ct,
            district=ds,
            property_type=pt,
            bank=bk,
        ).with_content_hash()
        out.append(pl)
    return out
