"""CLI shim so `python -m scrapers.williams` executes the package main()."""

import sys

from scrapers.williams import main

if __name__ == "__main__":
    sys.exit(main())
