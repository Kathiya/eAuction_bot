from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from project.config.settings import Settings, get_settings
from project.filters.engine import load_listing_filter_from_path
from project.filters.models import ListingFilter
from project.logging_setup import configure_logging
from project.pipeline import run_cycle

logger = logging.getLogger(__name__)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    configure_logging(get_settings().log_json)
    yield


app = FastAPI(title="Auction listing monitor", version="1.0.0", lifespan=_lifespan)

_last_run: dict[str, object] = {}


class FiltersUpdate(BaseModel):
    payload: ListingFilter


def _settings() -> Settings:
    return get_settings()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/filters")
def get_filters() -> JSONResponse:
    s = _settings()
    p = Path(s.filters_json_path)
    if not p.is_file():
        return JSONResponse(content=ListingFilter().model_dump())
    data = json.loads(p.read_text(encoding="utf-8"))
    return JSONResponse(content=data)


@app.put("/api/filters")
def put_filters(body: FiltersUpdate) -> dict[str, str]:
    s = _settings()
    p = Path(s.filters_json_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(body.payload.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {"status": "saved"}


@app.post("/api/run-once")
def post_run_once(background_tasks: BackgroundTasks) -> dict[str, str]:
    s = _settings()

    def job() -> None:
        try:
            run_cycle(s)
            _last_run["ok"] = True
            _last_run["error"] = None
        except Exception as e:
            _last_run["ok"] = False
            _last_run["error"] = str(e)
            logger.exception("api_run_once_failed")

    background_tasks.add_task(job)
    return {"status": "started"}


@app.get("/api/last-run")
def last_run() -> dict[str, object]:
    return dict(_last_run)


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    s = _settings()
    filt = load_listing_filter_from_path(s.filters_json_path)
    j = json.dumps(filt.model_dump(), indent=2)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Auction listing monitor</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; max-width: 900px; }}
    textarea {{ width: 100%; height: 320px; font-family: monospace; }}
    button {{ margin-top: 0.5rem; margin-right: 0.5rem; }}
    pre {{ background: #f4f4f4; padding: 1rem; overflow: auto; }}
  </style>
</head>
<body>
  <h1>Auction listing monitor</h1>
  <p>Cache: <code>{s.listing_cache_path}</code> · Filters: <code>{s.filters_json_path}</code></p>
  <h2>Filters (JSON)</h2>
  <textarea id="f">{j}</textarea>
  <div>
    <button type="button" id="save">Save filters</button>
    <button type="button" id="run">Run once (background)</button>
  </div>
  <h2>Last run</h2>
  <pre id="lr">{{}}</pre>
  <script>
    async function refreshLastRun() {{
      const r = await fetch('/api/last-run');
      const d = await r.json();
      document.getElementById('lr').textContent = JSON.stringify(d, null, 2);
    }}
    document.getElementById('save').onclick = async () => {{
      let obj;
      try {{ obj = JSON.parse(document.getElementById('f').value); }}
      catch (e) {{ alert('Invalid JSON'); return; }}
      const r = await fetch('/api/filters', {{
        method: 'PUT',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ payload: obj }})
      }});
      if (!r.ok) alert('Save failed');
      else alert('Saved');
    }};
    document.getElementById('run').onclick = async () => {{
      const r = await fetch('/api/run-once', {{ method: 'POST' }});
      const d = await r.json();
      alert(d.status || 'ok');
      setTimeout(refreshLastRun, 500);
    }};
    refreshLastRun();
    setInterval(refreshLastRun, 5000);
  </script>
</body>
</html>"""


@app.get("/api/validate-filters")
def validate_filters() -> dict[str, object]:
    s = _settings()
    try:
        f = load_listing_filter_from_path(s.filters_json_path)
        return {"ok": True, "filter": f.model_dump()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
