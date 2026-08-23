"""``python -m scrapers.gasnom`` entry point (delegates to package CLI)."""

from __future__ import annotations

import sys

from scrapers.gasnom import main

if __name__ == "__main__":
    sys.exit(main())
