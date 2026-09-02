"""Build the Gulf South SQ commit message from scraper output.

Why:
    The previous inline ``python - <<'PY'`` heredoc in
    ``.github/workflows/gulf-south-sq.yml`` was indented under a ``run: |`` block
    whose own indentation YAML stripped, so Python received its first line indented
    and died with ``IndentationError``. That failure aborted the Commit step and
    discarded the already-computed curated parquet — reintroducing the exact
    data-loss bug the branch exists to fix.

    A standalone, importable helper removes the YAML-indentation hazard entirely:
    CI runs ``python scripts/gulf_south_commit_msg.py < /tmp/scraper_output.json``
    and a unit test calls :func:`build_commit_message` directly.

What:
    Reads the JSON the scraper emits on stdout (``{"files": [{"cycle": ...,
    "gas_day": ...}, ...]}``) and prints one commit message:

    * empty ``files`` list  -> ``data(gulf-south): SQ sync (no new postings)``
    * one gas day, N files   -> ``data(gulf-south): SQ 2026-08-31 (N files, CYC)``
    * multi gas day, N files -> ``data(gulf-south): SQ 2026-08-31..2026-09-01
      (N files, ID1/ID2/ID3)``
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

#: Prefix shared by every Gulf South SQ commit message.
MSG_PREFIX = "data(gulf-south): SQ"


def build_commit_message(files: list[dict[str, str]]) -> str:
    """Return the commit message for a Gulf South SQ run.

    Args:
        files: The ``files`` list from the scraper's JSON output, each entry a
            mapping with ``gas_day`` (``YYYY-MM-DD``) and ``cycle`` (e.g.
            ``ID1``) keys.

    Returns:
        A single-line commit message. Empty input yields the "no new postings"
        sync message; one gas day yields a single-date message; multiple gas
        days yield a ``first..last`` span.
    """
    if not files:
        return f"{MSG_PREFIX} sync (no new postings)"

    days = sorted({f["gas_day"] for f in files})
    cycles = sorted({f["cycle"] for f in files})
    cyc = "/".join(cycles)
    span = days[0] if len(days) == 1 else f"{days[0]}..{days[-1]}"
    return f"{MSG_PREFIX} {span} ({len(files)} files, {cyc})"


def main(argv: list[str] | None = None) -> int:
    """Read scraper JSON (stdin or first arg) and print the commit message."""
    text = Path(argv[0]).read_text(encoding="utf-8") if argv else sys.stdin.read()

    payload = json.loads(text) if text.strip() else {}
    files = payload.get("files") or []
    sys.stdout.write(build_commit_message(files) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
