"""
eAuctions listing cache dashboard for Streamlit Community Cloud.

Secrets (Streamlit Cloud: App settings → Secrets), or set the same names in the environment:

- CACHE_JSON_URL — HTTPS URL to `listings_cache.json` (e.g. GitHub raw URL for your repo).
  For a **private** repo, GitHub still serves raw with auth; set GITHUB_RAW_TOKEN.
- GITHUB_RAW_TOKEN — optional; sent as ``Authorization: token <value>`` when fetching CACHE_JSON_URL.

If CACHE_JSON_URL is unset, the app reads ``./data/listings_cache.json`` next to this file (local dev).

Data is refreshed on a TTL (see ``load_cache``); adjust ``ttl=`` if you want fresher data without redeploying.

---

GitHub Actions (this repo’s ``.github/workflows/auction-poll.yml``) should hold **repository secrets**, never committed:

- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID — required for Telegram alerts from the worker.
- LISTING_SEARCH_URLS — optional; overrides default Gujarat/Ahmedabad + vehicle URLs.
- HTTP_VERIFY_SSL — optional; set ``false`` only if the runner hits TLS issues (define as secret ``false``).

Scheduled runs use UTC. The worker runs ``python -m project.main run-once`` and may commit ``data/listings_cache.json``
with ``[skip ci]`` so pushes do not re-trigger the workflow.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from project.cache.store import parse_cache_json_text  # noqa: E402


def _secret(key: str, default: str = "") -> str:
    try:
        if key not in st.secrets:
            return default
        v = st.secrets[key]
        return str(v).strip() if v is not None else default
    except Exception:
        return default


@st.cache_data(ttl=300, show_spinner=True)
def load_cache() -> tuple[dict, dict, str | None]:
    """
    Returns (meta dict, listings dict stable_id -> PropertyListing, error message or None).
    """
    url = _secret("CACHE_JSON_URL") or os.environ.get("CACHE_JSON_URL", "").strip()
    token = _secret("GITHUB_RAW_TOKEN") or os.environ.get("GITHUB_RAW_TOKEN", "").strip()

    try:
        if url:
            headers: dict[str, str] = {}
            if token:
                headers["Authorization"] = f"token {token}"
            r = httpx.get(url, headers=headers, timeout=60.0, follow_redirects=True)
            r.raise_for_status()
            raw = r.text
        else:
            p = ROOT / "data" / "listings_cache.json"
            if not p.is_file():
                return {}, {}, "No local data/listings_cache.json and CACHE_JSON_URL is not set."
            raw = p.read_text(encoding="utf-8")
        meta, listings = parse_cache_json_text(raw)
        return meta, listings, None
    except Exception as e:
        return {}, {}, str(e)


def main() -> None:
    st.set_page_config(page_title="eAuctions cache", layout="wide")
    st.title("eAuctions listing cache")

    meta, listings, err = load_cache()
    if err:
        st.error(err)
        st.info(
            "Configure **CACHE_JSON_URL** (and optional **GITHUB_RAW_TOKEN** for private raw URLs) "
            "in Streamlit secrets, or run locally after `python -m project.main run-once`."
        )
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Listings", len(listings))
    with c2:
        st.metric("Source", str(meta.get("source_site") or "—"))
    with c3:
        st.metric("Cache updated (UTC)", str(meta.get("updated_at") or "—"))

    q = st.text_input("Filter (title, city, id)", value="").strip().lower()

    rows = []
    for p in sorted(listings.values(), key=lambda x: (x.city or "", x.title or "", x.stable_id)):
        blob = " ".join(
            filter(
                None,
                [p.stable_id, p.title, p.city, p.state, p.district, p.property_type or ""],
            )
        ).lower()
        if q and q not in blob:
            continue
        rows.append(
            {
                "stable_id": p.stable_id,
                "title": p.title,
                "price": p.price_display,
                "city": p.city,
                "state": p.state,
                "type": p.property_type,
                "url": p.url,
            }
        )

    if not rows:
        st.warning("No rows match the filter.")
        return

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _running_inside_streamlit() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


if _running_inside_streamlit():
    main()
