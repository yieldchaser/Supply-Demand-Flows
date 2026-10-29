"""Archive Synmax public dashboards (and the datasets they bind to) to local disk.

Why: https://agents.synmax.com/public/dashboard/<share_token> renders entirely
from unauthenticated JSON endpoints. The dashboard payload embeds the document
markup plus a list of chart/map templates; each template's ``config`` names the
``dataset_id`` values it plots, and ``/public/dataset/<id>/data?share_token=...``
serves the rows. Archiving the three levels means the captured dashboards stay
readable offline instead of depending on a live vendor API.

Endpoints (base https://api.atlas-prod.synmax.com/api, no auth):
  GET /public/dashboard/{token}                 -> {dashboard, templates, share_info}
  GET /public/template/{template_id}?share_token&template_type=chart|map
  GET /public/dataset/{dataset_id}/data?share_token&limit&offset

There is no public endpoint that enumerates share tokens, so dashboards must be
added to ``SEED_TOKENS`` (or passed on the command line) explicitly.

Run:
  .venv/Scripts/python.exe scripts/archive_synmax_public.py --out data/vendor/synmax
  .venv/Scripts/python.exe scripts/archive_synmax_public.py --token <share_token>
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

API_BASE = "https://api.atlas-prod.synmax.com/api"
PAGE = 50_000  # rows per dataset page; the map layers are ~15k rows each
UA = "Supply-Demand-Flows/1.0 (archival; contact: repo owner)"

# Share tokens are opaque ids published at agents.synmax.com/public/dashboard/<token>.
SEED_TOKENS = (
    "51ae9fe515eb404eb65022952101c018dd37ffb05d3e12b1acfd0754c713009a",
    "f04a81153f5ae58c0041c2e57701fccb249edda37eaa8d6eba4949a8dff94735",
)

TEMPLATE_ID_RE = re.compile(r'data-template-id="([0-9a-zA-Z_-]+)"')
SHARE_TOKEN_RE = re.compile(r"[0-9a-f]{64}")


def _get(path: str, params: dict[str, Any] | None = None, timeout: int = 60) -> Any:
    url = f"{API_BASE}{path}"
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _slug(text: str, fallback: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    return (slug[:60] or fallback)


def _dataset_ids(template: dict[str, Any]) -> list[int]:
    """Collect every dataset_id a template references.

    Three shapes occur: map layers carry ``dataset_id: <int>``, chart templates
    from ``/public/template`` carry ``dataset_id: [<int>, ...]``, and the chart
    templates embedded in a dashboard payload carry no id at all. Callers should
    prefer the ``/public/template`` copy, which resolves the ids.
    """
    found: list[int] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in ("dataset_id", "dataset_ids"):
                    # ``dataset_id`` is an int on map layers, a list of ints on
                    # chart templates, and ``dataset_ids`` is a list on maps.
                    for item in value if isinstance(value, list) else [value]:
                        if isinstance(item, int) and not isinstance(item, bool):
                            found.append(item)
                        elif isinstance(item, str) and item.isdigit():
                            found.append(int(item))
                else:
                    walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    # Maps put ``layers``/``dataset_ids`` at the top level, charts nest them
    # under ``config``; scan both shapes rather than assuming one.
    walk(template)
    return sorted(set(found))


def fetch_dataset(dataset_id: int, token: str) -> dict[str, Any] | None:
    """Page through a public dataset until every row is captured."""
    rows: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    offset = 0
    while True:
        page = _get(
            f"/public/dataset/{dataset_id}/data",
            {"share_token": token, "limit": PAGE, "offset": offset},
        )
        if not meta:
            meta = {k: v for k, v in page.items() if k != "data"}
        chunk = page.get("data") or []
        rows.extend(chunk)
        total = page.get("total_rows") or 0
        offset += len(chunk)
        if not chunk or offset >= total:
            break
        time.sleep(0.2)  # be gentle with the vendor's API
    if not rows:
        return None
    return {**meta, "returned_rows": len(rows), "data": rows}


def archive(token: str, out_dir: Path, force: bool = False) -> dict[str, Any] | None:
    dash_dir = out_dir / token[:12]
    dash_dir.mkdir(parents=True, exist_ok=True)

    listing = dash_dir / "dashboard.json"
    if listing.exists() and not force:
        payload = json.loads(listing.read_text(encoding="utf-8"))
    else:
        payload = _get(f"/public/dashboard/{token}")
        listing.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    dash = payload.get("dashboard") or {}
    templates = payload.get("templates") or []
    html = ""
    for comp in dash.get("components") or []:
        html += ((comp.get("config") or {}).get("html_content") or "")

    # The dashboard payload embeds a thin copy of each chart template (no
    # dataset ids), so always consult /public/template, which resolves them.
    # Maps are only reachable through the payload's own config.
    by_id = {t.get("id"): t for t in templates if t.get("id")}
    resolved: dict[str, dict[str, Any]] = {}
    for tid, embedded in by_id.items():
        kinds = ("map", "chart") if embedded.get("type") == "map" else ("chart", "map")
        for kind in kinds:
            try:
                cfg = _get(f"/public/template/{tid}", {"share_token": token, "template_type": kind})
            except (urllib.error.HTTPError, urllib.error.URLError):
                continue
            if cfg:
                # The updater maps by template id, so normalise the key the
                # endpoint answers with (chart_id / map_id) back to tid.
                resolved[tid] = {**cfg, "id": cfg.get("id") or cfg.get("chart_id") or cfg.get("map_id") or tid}
                break
        else:
            resolved[tid] = embedded
    by_id = resolved

    datasets: dict[str, Any] = {}
    for template in by_id.values():
        for ds_id in _dataset_ids(template):
            cache = dash_dir / f"dataset_{ds_id}.json"
            if cache.exists() and not force:
                continue
            try:
                data = fetch_dataset(ds_id, token)
            except urllib.error.HTTPError as exc:
                print(f"    dataset {ds_id}: HTTP {exc.code}", file=sys.stderr)
                continue
            if data:
                cache.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                datasets[str(ds_id)] = data
            time.sleep(0.2)

    summary = {
        "share_token": token,
        "dashboard_id": dash.get("dashboard_id"),
        "name": dash.get("name"),
        "created_at": dash.get("created_at"),
        "updated_at": dash.get("updated_at"),
        "share_info": payload.get("share_info"),
        "templates": [
            {
                "id": t.get("id") or t.get("chart_id") or t.get("map_id"),
                "name": t.get("name") or t.get("chart_name") or t.get("map_name"),
                "type": t.get("type"),
                "subtype": t.get("subtype") or t.get("chart_type"),
                "dataset_ids": _dataset_ids(t),
            }
            for t in by_id.values()
        ],
        "datasets": {
            ds_id: {
                "dataset_name": d.get("dataset_name"),
                "total_rows": d.get("total_rows"),
                "returned_rows": d.get("returned_rows"),
                "columns": d.get("columns"),
            }
            for ds_id, d in datasets.items()
        },
    }
    (dash_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary


def refresh_index(out_dir: Path) -> None:
    """Write an index of every archived dashboard so nothing is silently lost."""
    entries = []
    for summary_path in sorted(out_dir.glob("*/summary.json")):
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        entries.append(
            {
                "dir": summary_path.parent.name,
                "share_token": summary.get("share_token"),
                "url": f"https://agents.synmax.com/public/dashboard/{summary.get('share_token')}",
                "name": summary.get("name"),
                "updated_at": summary.get("updated_at"),
                "template_count": len(summary.get("templates") or []),
                "dataset_count": len(summary.get("datasets") or {}),
                "datasets": summary.get("datasets"),
                "templates": [t.get("name") for t in summary.get("templates") or []],
            }
        )
    (out_dir / "index.json").write_text(json.dumps(entries, indent=2, ensure_ascii=False), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="data/vendor/synmax", help="archive directory")
    parser.add_argument("--token", action="append", default=[], help="share token (repeatable)")
    parser.add_argument("--from-index", help="read a previously written index.json for tokens")
    parser.add_argument("--force", action="store_true", help="re-download cached files")
    args = parser.parse_args(argv)

    tokens: list[str] = list(dict.fromkeys(args.token))
    if not tokens and args.from_index:
        prior = json.loads(Path(args.from_index).read_text(encoding="utf-8"))
        tokens = [row["share_token"] for row in prior if row.get("share_token")]
    if not tokens:
        tokens = list(SEED_TOKENS)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    failures = 0
    for token in tokens:
        if not SHARE_TOKEN_RE.fullmatch(token):
            print(f"skip malformed token: {token!r}", file=sys.stderr)
            failures += 1
            continue
        print(f"-> {token[:12]}")
        try:
            summary = archive(token, out_dir, force=args.force)
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
            print(f"   FAILED: {exc}", file=sys.stderr)
            failures += 1
            continue
        if summary:
            print(f"   {summary['name']!r}: {len(summary['templates'])} templates, {len(summary['datasets'])} datasets")

    refresh_index(out_dir)
    print(f"index -> {out_dir / 'index.json'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
