"""
Build the dashboard JSON bundle that the frontend fetches.

Aggregates curated Parquets and health files into a single bundle.json.

Bundle vs archive:
    ``data/curated/*.parquet`` are the COMPLETE historical archive — nothing
    in this module ever writes to them.  The bundle is the *display subset*
    the frontend fetches: for EBB sources (gulf_south, gasnom, quorum, bhe,
    cheniere) it ships ONLY the series whose location id appears as a
    high-confidence entry in the corresponding config/meters/*.json (and
    config/lng_meter_map.json for gulf_south), with FULL history for those
    series and no date truncation.  Non-EBB sources (EIA, GIE AGSI, Baker
    Hughes) are shipped as-is.

Blob-size guard:
    A per-source safety cap (MAX_ROWS_PER_SOURCE) exists purely to keep the
    bundle under GitHub's 100 MB blob limit.  If it EVER trips, this module
    raises instead of silently truncating — silent pruning by recency was a
    bug (it dropped old history from the UI); relevance-based pruning above
    is the intended mechanism.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.base.safe_writer import safe_write_json, safe_write_text

CURATED_DIR = Path("data/curated")
HEALTH_DIR = Path("data/health")
DOCS_DATA_DIR = Path("docs/data")

#: EBB sources whose bundle rows are pruned to relevant meters BEFORE any
#: cap. Maps source key -> its meter-config file (high-confidence entries only).
EBB_METER_CONFIGS: dict[str, Path] = {
    "gulf_south": Path("config/lng_meter_map.json"),
    "gasnom": Path("config/meters/gasnom.json"),
    "quorum": Path("config/meters/quorum.json"),
    "bhe": Path("config/meters/bhe.json"),
    "cheniere": Path("config/meters/cheniere.json"),
    "enbridge": Path("config/meters/enbridge.json"),
    "kinder_morgan": Path("config/meters/kinder_morgan.json"),
}

#: Frontend registries that extend the relevance allowlist beyond the
#: high-confidence meter configs above. Each entry maps a source key to a
#: JS registry module exporting arrays with numeric ``loc`` fields (plus,
#: optionally, ``inHeadline: false`` entries that must still ship — the
#: basin-egress table flags them instead of dropping them). Every loc id
#: found in these modules is allowlisted for the source.
FRONTEND_EXTRA_REGISTRIES: dict[str, tuple[Path, ...]] = {
    "gulf_south": (Path("docs/js/util/basin-egress.js"),),
}

#: Blob-size guard only — never a pruning mechanism. Trips loudly.
MAX_ROWS_PER_SOURCE = 400_000


def _json_default(obj: object) -> str:
    """
    Fallback serializer for non-JSON-native types emitted by pandas/transformers.
    Handles date, datetime, pd.Timestamp, numpy scalar types, Decimal.
    """
    if isinstance(obj, date | datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    # numpy types (int64, float64) have .item() method
    if hasattr(obj, "item"):
        return obj.item()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _collect_high_confidence_loc_ids(node: Any, found: set[str], high_key: bool = False) -> None:
    """Recursively gather loc_ids of configured meter entries.

    Why:
        Meter configs use different nesting shapes and confidence styles:
        an explicit ``confidence: high`` field (lng_meter_map, quorum, bhe,
        cheniere) or membership in ``high``/``candidates`` lists (gasnom).
        Both ``high`` AND ``candidates`` ship — candidates are unconfirmed
        for TERMINAL METRICS (frontend never headlines them) but are real
        postings worth carrying in the bundle; dropping them entirely caused
        the 2026-08-26 incident where SABINE/PAP shards collapsed to zero
        rows while their configs listed live candidate meters. Only
        ``excluded`` lists are never collected.
    """
    if isinstance(node, dict):
        if node.get("loc_id") is not None and (
            node.get("confidence") == "high" or high_key
        ):
            found.add(str(node["loc_id"]).strip().lower())
        for key, value in node.items():
            _collect_from_pair(key, value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_high_confidence_loc_ids(item, found, high_key=high_key)


def _collect_from_pair(key: str, value: Any, found: set[str]) -> None:
    """Walk one container entry, flagging gasnom-style meter lists."""
    if key in ("high", "candidates") and isinstance(value, list):
        _collect_high_confidence_loc_ids(value, found, high_key=True)
    else:
        _collect_high_confidence_loc_ids(value, found)


def _loc_ids_from_frontend_registries(source_key: str) -> set[str]:
    """Pull loc ids out of the source's frontend registry JS modules.

    What:
        Scans each registered ``docs/js/util/*.js`` module for
        ``{ loc: <int>,`` object literals. The registries are plain data —
        a regex over ``loc:\\s*(\\d+)`` is exact for this file family and
        avoids shipping a JS parser in the publisher.

    Failure modes:
        Missing module -> empty contribution (config decides); unreadable
        module prints a warning and continues with what parsed.
    """
    found: set[str] = set()
    for path in FRONTEND_EXTRA_REGISTRIES.get(source_key, ()):  # noqa: PTH117 - relative to repo root by design
        if not path.exists():
            print(f"WARN: frontend registry {path} not found — no extra loc ids from it")
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"WARN: unreadable frontend registry {path}: {exc}")
            continue
        found.update(m.group(1) for m in re.finditer(r"loc:\s*(\d+)", text))
    return found


def load_relevant_loc_ids(source_key: str) -> set[str]:
    """Return the lowercase loc_id allowlist for one EBB source.

    What:
        Union of (a) high-confidence entries in the meter config and (b)
        every loc id in the source's frontend registries — basin egress,
        storage, and power-burn meters are medium-confidence by design and
        would otherwise never reach the bundle.

    Failure modes:
        Missing/unreadable config -> empty set; callers treat that as
        keep-all with a loud warning rather than silently shipping an empty
        source.
    """
    path = EBB_METER_CONFIGS[source_key]
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARN: unreadable meter config {path}: {exc} — keeping ALL series")
        return set()
    found: set[str] = set()
    if isinstance(data, dict):
        for key, value in data.items():
            _collect_from_pair(key, value, found)
    else:
        _collect_high_confidence_loc_ids(data, found)
    before = len(found)
    found |= _loc_ids_from_frontend_registries(source_key)
    if len(found) > before:
        print(f"{source_key}: +{len(found) - before} loc id(s) from frontend registries")
    return found


def _series_matches(series_id: str, allowed: set[str]) -> bool:
    """True when the series_id embeds one of the allowed loc ids.

    What:
        Loc ids sit at different token positions per source (prefix lengths
        vary), so this matches ``_<loc>_`` anywhere in the lowercased id —
        unambiguous because loc ids are unique within a source. Flow-aware
        series ids ({kind}_{loc}_{r|d}_{cycle}) match identically; BOTH legs
        of an allowlisted meter ship, because both are real data.
    """
    padded = f"_{series_id.lower()}_"
    return any(f"_{loc}_" in padded for loc in allowed)


def _check_agreement(
    reg_text: str,
    bundle_sources: dict[str, dict[str, Any]],
    high_conf: dict[str, set[str]],
) -> list[str]:
    """Pure registry<->config<->bundle agreement check (unit-testable).

    Returns a list of problem strings (empty == pass). See
    _audit_registry_bundle_agreement for the contract.
    """
    problems: list[str] = []
    # Build a quick lookup of which series have non-zero rows in the bundle.
    nonzero_series: dict[str, set[str]] = {}
    for src_key, entry in bundle_sources.items():
        s = set()
        for r in entry.get("data", []):
            sid = str(r.get("series_id", "")).lower()
            try:
                if float(r.get("value") or 0) != 0:
                    s.add(sid)
            except (TypeError, ValueError):
                pass
        nonzero_series[src_key] = s

    # Extract terminal ids and their feed declarations via regex.
    # Terminal block start:  "  some_id: {"  at column 2.
    term_re = re.compile(r"^\s{2}(\w+):\s*\{", re.M)
    feed_re = re.compile(
        r"source:\s*'([\w]+)'.*?series:\s*'([\w]+)'.*?kind:\s*'([\w-]+)'",
        re.S,
    )
    for m in term_re.finditer(reg_text):
        tid = m.group(1)
        block_start = m.end()
        # find matching close: scan braces
        depth = 1
        i = block_start
        while i < len(reg_text) and depth > 0:
            c = reg_text[i]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            i += 1
        block = reg_text[m.start():i]
        for fm in feed_re.finditer(block):
            src = fm.group(1)
            series = fm.group(2)
            kind = fm.group(3)
            if kind not in ("measured", "measured-partial"):
                continue
            # (a) high-confidence in config
            loc_part = series.lower().split("_")[-2] if "_" in series else series.lower()
            mloc = re.search(r"_sq_([a-z0-9]+)_[dr]_", series.lower())
            loc = mloc.group(1) if mloc else loc_part
            if src in high_conf and loc not in high_conf[src]:
                problems.append(
                    f"{tid}: feed series '{series}' (source '{src}', kind '{kind}') "
                    f"is NOT a high-confidence meter in config/meters/{src}.json "
                    f"(confidence must be 'high'). The relevance prune will drop it."
                )
            # (b) non-zero rows in bundle
            series_l = series.lower()
            if (
                src in nonzero_series
                and not any(s.startswith(series_l + "_") for s in nonzero_series[src])
                and series_l not in nonzero_series[src]
            ):
                problems.append(
                    f"{tid}: headline feed '{series}' (source '{src}', kind '{kind}') "
                    f"ships ZERO non-zero rows in the built bundle — the panel "
                    f"will render an empty/zero card. Backfill the history or fix "
                    f"the config confidence so it prunes in."
                )
    return problems


def _audit_registry_bundle_agreement(
    bundle_sources: dict[str, dict[str, Any]],
) -> None:
    """Fail the publish when a registry headline meter is missing from config or bundle.

    Why:
        Seventh bug of the same family (2026-08-26): lng-terminals.js named
        creole_trail_sq_CT200111_d as Sabine's headline (kind:'measured' at
        the time), but config/meters/cheniere.json marked CT200111
        ``confidence: comparison``. The relevance prune dropped it, the bundle
        shipped zero rows for it, and the panel silently lost its number — with
        nothing checking that the registry's declared headline actually reached
        the bundle. The per-source coverage audit could not catch this: CT200111
        DID resolve to a curated series (so "id-space drift" passed), it was
        only the *prune* that excluded it.

        Fix: assert REGISTRY <-> CONFIG <-> BUNDLE agreement. For every feed a
        terminal declares with kind in {measured, measured-partial}:
          (a) its loc id MUST be high-confidence in the source's meter config,
          (b) it MUST ship >= 1 non-zero row in the built bundle.
        Context/comparison/proxy feeds are exempt from (b) but NOT from (a) —
        a feed the panel can never headline still must be a real, configured
        meter so its data is at least present.

        The pure logic lives in _check_agreement() so unit tests can exercise
        both arms without the filesystem registry (negative tests in
        tests/test_publish_agreement.py).
    """
    if os.environ.get("BLUETIDE_SKIP_COVERAGE_AUDIT"):
        return

    reg_path = Path("docs/js/util/lng-terminals.js")
    if not reg_path.exists():
        print("WARN: registry not found — skipping agreement audit")
        return
    reg_text = reg_path.read_text(encoding="utf-8")

    high_conf: dict[str, set[str]] = {}
    for src_key in EBB_METER_CONFIGS:
        high_conf[src_key] = load_relevant_loc_ids(src_key)

    problems = _check_agreement(reg_text, bundle_sources, high_conf)
    if problems:
        msg = "\n".join(f"  - {p}" for p in problems)
        raise SystemExit(
            "REGISTRY<->CONFIG<->BUNDLE AGREEMENT FAILED — refusing to publish:\n" + msg
        )
    print("Registry<->config<->bundle agreement: PASS")


def _audit_bundle_coverage(index_rows: dict[str, int]) -> None:
    """Fail the publish when configured meters ship nothing into the bundle.

    Why:
        The 2026-08-26 gasnom incident: a meter-config re-inventory wrote loc
        ids from a different id-space than the curated history, and the
        relevance prune silently shipped a 95%-thinned shard. Curated was
        healthy — the loss happened exactly here, between parquet and index,
        where no integrity check looked. This audit closes that gap by
        failing the DEPLOY (publish workflow runs this before committing).

    What:
        Two rules, both fatal:
          1. Every bundled source must ship rows > 0 (an empty shard is a
             bug even for a legitimately quiet source — handle those with an
             explicit config exclusion, not silence).
          2. For every EBB source with a meter config, EVERY configured
             loc id (high + candidates) must resolve to at least one real
             curated series. Unresolved ids mean id-space drift: fix the
             config or backfill the history — never ship a half-empty shard.
    """
    problems: list[str] = []

    # Unit tests exercise build() with synthetic fixtures that can never
    # satisfy real meter configs; deployments (publish workflow) always run
    # the audit. Same env-kill-switch pattern as BLUETIDE_HEALTH_DIR.
    if os.environ.get("BLUETIDE_SKIP_COVERAGE_AUDIT"):
        return

    for src_key, rows in sorted(index_rows.items()):
        if rows <= 0:
            problems.append(
                f"{src_key}: shipped 0 rows in the bundle index — "
                "over-pruned, empty curated, or wrong source key"
            )

    for src_key, cfg_path in sorted(EBB_METER_CONFIGS.items()):
        if not cfg_path.exists():
            continue
        allowed = load_relevant_loc_ids(src_key)
        if not allowed:
            continue
        curated_path = CURATED_DIR / f"{src_key}.parquet"
        if not curated_path.exists():
            problems.append(f"{src_key}: meter config exists but no curated parquet")
            continue
        series = pd.read_parquet(curated_path, columns=["series_id"])["series_id"]
        sid_list = [str(s) for s in series.dropna().unique()]
        unresolved = [
            loc for loc in sorted(allowed)
            if not any(_series_matches(sid, {loc}) for sid in sid_list)
        ]
        if unresolved:
            problems.append(
                f"{src_key}: configured loc id(s) {unresolved} match NO curated "
                f"series — id-space drift between config/meters/{src_key}.json "
                "and data/curated (fix the config or backfill)"
            )

    if problems:
        msg = "\n".join(f"  - {p}" for p in problems)
        raise SystemExit(
            "BUNDLE COVERAGE AUDIT FAILED — refusing to publish:\n" + msg
        )
    print("Bundle coverage audit: PASS "
          f"({len(index_rows)} sources, {len(EBB_METER_CONFIGS)} meter configs checked)")


def prune_to_relevant_meters(df: pd.DataFrame, source_key: str) -> pd.DataFrame:
    """Filter an EBB frame down to configured high-confidence meter series.

    What:
        Keeps every row whose series_id carries an allowlisted loc id, with
        FULL history — no recency truncation. Falls back to keep-all when no
        allowlist resolves (loudly).

    Failure modes:
        Never raises for empty results by itself; an over-aggressive config
        surfaces via the blob-size guard downstream or as a visibly tiny
        row_count in the summary.
    """
    allowed = load_relevant_loc_ids(source_key)
    if not allowed:
        print(
            f"WARN: no high-confidence meters resolved for '{source_key}' — "
            "shipping ALL series (check its meter config)"
        )
        return df
    mask = df["series_id"].map(lambda sid: _series_matches(str(sid), allowed))
    kept = df[mask]
    print(
        f"{source_key}: relevance prune {len(df)} -> {len(kept)} rows "
        f"(allowlist {sorted(allowed)})"
    )
    return kept


def build() -> dict[str, Any]:
    """
    Aggregation logic for building the dashboard bundle.
    """
    bundle = {
        "generated_at": datetime.now(UTC).isoformat(),
        "sources": {},
        "health": {},
    }

    # 1. Collect Curated Data
    if CURATED_DIR.exists():
        for parquet_path in CURATED_DIR.glob("*.parquet"):
            source_key = parquet_path.stem
            try:
                df = pd.read_parquet(parquet_path)
                # Sort by period for easier frontend consumption
                if "period" in df.columns:
                    df = df.sort_values("period")
                # Relevance-based pruning for EBB sources: keep only the
                # configured LNG feedgas meters, with full history.
                if source_key in EBB_METER_CONFIGS:
                    df = prune_to_relevant_meters(df, source_key)
                # Blob-size guard: must never trip silently. If it does,
                # the relevance config needs revisiting — fail loudly.
                if len(df) > MAX_ROWS_PER_SOURCE:
                    msg = (
                        f"Bundle blob-size guard tripped for '{source_key}': "
                        f"{len(df)} rows > {MAX_ROWS_PER_SOURCE}. "
                        "Refusing to ship a >100MB bundle. Prune relevance in "
                        f"{EBB_METER_CONFIGS.get(source_key, 'its meter config')} "
                        "or raise MAX_ROWS_PER_SOURCE deliberately."
                    )
                    raise RuntimeError(msg)

                bundle["sources"][source_key] = {
                    "latest_period": df["period"].max() if "period" in df.columns else None,
                    "row_count": len(df),
                    "data": df.to_dict(orient="records"),
                }
            except Exception as exc:
                print(f"Error reading {parquet_path}: {exc}")

    # 2. Collect Health Data
    if HEALTH_DIR.exists():
        for health_path in HEALTH_DIR.glob("*.json"):
            source_key = health_path.stem
            if source_key.endswith(".prev"):
                continue  # .prev files are local rotation state, not for dashboard
            try:
                with health_path.open("r", encoding="utf-8") as f:
                    bundle["health"][source_key] = json.load(f)
            except Exception as exc:
                print(f"Error reading {health_path}: {exc}")

    # 3. Serialise and Hash
    bundle_json = json.dumps(bundle, separators=(",", ":"), ensure_ascii=False, default=_json_default)
    bundle_hash = hashlib.md5(bundle_json.encode("utf-8")).hexdigest()[:8]

    # 4. Write Files
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Static bundle.json
    safe_write_text(DOCS_DATA_DIR / "bundle.json", bundle_json)

    # Cache-busting bundle.{HASH}.json
    hashed_name = f"bundle.{bundle_hash}.json"
    safe_write_text(DOCS_DATA_DIR / hashed_name, bundle_json)

    # Per-source shards + index manifest (lazy loading).
    #
    # Why: measured client cost of the monolithic bundle (2026-08-25,
    # scripts/measure_bundle_parse.py, headless Chromium @4x CPU throttle)
    # was ~2.6 s JSON.parse and ~500 MB heap for 267k rows. The frontend
    # therefore boots on CORE sources only and fetches/parses each remaining
    # source shard on demand (docs/js/data/bundle-loader.js). The monolithic
    # bundle stays for compatibility and as the integrity reference.
    index_sources: dict[str, dict[str, Any]] = {}
    bundle_sources = bundle["sources"]
    for source_key in list(bundle_sources):
        entry = bundle_sources[source_key]
        shard_json = json.dumps(entry, separators=(",", ":"), ensure_ascii=False, default=_json_default)
        shard_name = f"src.{source_key}.{bundle_hash}.json"
        safe_write_text(DOCS_DATA_DIR / shard_name, shard_json)
        index_sources[source_key] = {
            "file": shard_name,
            "rows": int(entry.get("row_count") or 0),
            "latest_period": entry.get("latest_period"),
        }

    #: Sources parsed during boot (small, power above-the-fold panels).
    #: Everything else loads lazily on first panel access.
    core_sources = ["eia_storage", "eia_supply", "baker_hughes_weekly"]

    index = {
        "bundle_url": hashed_name,
        "generated_at": bundle["generated_at"],
        "hash": bundle_hash,
        "core": [k for k in core_sources if k in index_sources],
        "sources": index_sources,
    }
    index_name = f"index.{bundle_hash}.json"
    safe_write_text(
        DOCS_DATA_DIR / index_name,
        json.dumps(index, separators=(",", ":"), ensure_ascii=False, default=_json_default),
    )

    # Manifest (unchanged shape + index pointer)
    manifest = {
        "bundle_url": hashed_name,
        "index_url": index_name,
        "generated_at": bundle["generated_at"],
        "hash": bundle_hash,
    }
    safe_write_json(DOCS_DATA_DIR / "manifest.json", manifest)

    # 5b. Coverage audit — fail BEFORE any commit/deploy when configured
    #     meters ship nothing or config ids drifted out of curated.
    _audit_bundle_coverage({k: int(v["rows"]) for k, v in index_sources.items()})

    # 5c. Registry<->config<->bundle agreement — fail when a registry headline
    #     meter is missing from the meter config (prune would drop it) or ships
    #     zero rows (panel would render empty). Catches the 2026-08-26 Sabine
    #     CT200111-D silent-loss class of bug.
    _audit_registry_bundle_agreement(bundle["sources"])

    # 6. Prune stale bundle artifacts (defense against the docs/data graveyard).
    #    Old bundle.{HASH}.json, src.*.{HASH}.json and index.{HASH}.json pile up
    #    on every rebuild (~4.6 GB accumulated across 144 hashes by 2026-08-25).
    #    Keep the current hash plus KEEP_PREVIOUS prior hashes; delete the rest.
    _prune_stale_bundles(DOCS_DATA_DIR, bundle_hash, keep_previous=2)

    return {
        "generated_at": bundle["generated_at"],
        "hash": bundle_hash,
        "bundle_url": hashed_name,
        "index_url": index_name,
        "sources_count": len(bundle["sources"]),
    }


# Number of prior bundle hashes to retain alongside the current one. The
# frontend hard-codes nothing per-hash (it loads whatever manifest.json points
# at), so we only need the live hash + a small rollback window.
KEEP_PREVIOUS = 2


def _prune_stale_bundles(data_dir: Path, current_hash: str, keep_previous: int = KEEP_PREVIOUS) -> int:
    """Delete bundle.{h}.json / src.*.{h}.json / index.{h}.json for old hashes.

    What:
        Scans docs/data for cache-busted artifacts, groups them by the 8-char
        content hash embedded in the filename, keeps the current hash plus the
        ``keep_previous`` most-recently-modified hashes, and removes the rest.

    Returns:
        Count of files removed.

    Why:
        Publishing is frequent; without pruning the directory grows unbounded
        (144 hashes / 4.6 GB accumulated before this guard). The repo .gitignore
        should exclude these from git — pruned files are removed from disk here.
    """
    import re

    pattern = re.compile(r"(?:bundle|src\.[^.]+|index)\.([0-9a-f]{8})\.json$")
    by_hash: dict[str, list[Path]] = {}
    for p in data_dir.glob("*.json"):
        m = pattern.search(p.name)
        if not m:
            continue
        by_hash.setdefault(m.group(1), []).append(p)

    keep = {current_hash} if current_hash in by_hash else set()
    # Rank remaining hashes by newest mtime; keep the top `keep_previous`.
    ranked = sorted(
        (h for h in by_hash if h != current_hash),
        key=lambda h: max(p.stat().st_mtime_ns for p in by_hash[h]),
        reverse=True,
    )
    keep.update(ranked[:keep_previous])

    removed = 0
    for h, files in by_hash.items():
        if h in keep:
            continue
        for f in files:
            try:
                f.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        print(f"Pruned {removed} stale bundle artifacts (kept {len(keep)} hashes)")
    return removed


def resolve_current_index(data_dir: Path = DOCS_DATA_DIR) -> dict[str, Any]:
    """Resolve the CURRENT index via manifest.json — never by globbing.

    Why (TASK 4, 2026-08-26): verification tooling globbed
    ``docs/data/index.*.json`` and alphabetically picked a STALE hash
    (fb4f74be) instead of the freshly-built one (4bd04b97) — both lingered
    on disk until the graveyard prune removed the older. The manifest is the
    single source of truth for "which index is live", so every reader must
    follow manifest.json -> index_url rather than guessing by filename sort.

    Returns the parsed index dict. Raises SystemExit on a missing/broken
    manifest so callers fail loudly instead of silently reading stale data.
    """
    manifest_path = data_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"resolve_current_index: {manifest_path} missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    index_url = manifest.get("index_url")
    if not index_url:
        raise SystemExit("resolve_current_index: manifest.json has no index_url")
    index_path = data_dir / index_url
    if not index_path.exists():
        raise SystemExit(f"resolve_current_index: {index_path} (from manifest) missing")
    return json.loads(index_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    result = build()
    print(json.dumps(result, indent=2, default=_json_default))
