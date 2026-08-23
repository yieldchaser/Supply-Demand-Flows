# 🌊 Blue Tide
**The physical observatory of North American natural gas.**

**Live dashboard:** [https://yieldchaser.github.io/Supply-Demand-Flows/](https://yieldchaser.github.io/Supply-Demand-Flows/)

## Philosophy
Blue Tide is an observatory, not an oracle. We prioritize transparency over prediction. The system is built on a foundation of zero randomness — no stochastic models, no hidden seeds, and no synthetic noise. Our goal is to make every molecule of natural gas visible through high-fidelity data collection, transformation, and visualization.

## Status
![Status](https://img.shields.io/badge/status-6%20live%20sources%20%C2%B7%20observatory%20shipped-brightgreen)

Six ingestion pipelines run on schedule in CI. The **LNG Feedgas Observatory** is shipped and live (Freeport LNG, first terminal), with EBB coverage expanding to further terminals and platforms.

## Data Sources

### Live
| Source | Description | Feeds |
| :--- | :--- | :--- |
| **EIA API** | U.S. Energy Information Administration | Weekly storage, monthly supply & demand, LNG exports |
| **Baker Hughes** | North American Rig Count (weekly) | Drilling activity by basin |
| **GIE AGSI+** | Gas Infrastructure Europe aggre­gates | European storage context |
| **Boardwalk / Gulf South OAC** | FERC EBB scheduled quantities via the public Operational Capacity endpoint | LNG feedgas (Freeport) |

### Planned — not yet implemented
| Source | Intended Use |
| :--- | :--- |
| **AISStream** | Real-time vessel tracking → LNG ship movements |
| **ENTSOG** | European cross-border transmission flows |
| **SEC EDGAR** | Institutional filings (10-K, 10-Q) |
| **Additional EBB platforms** | More feedgas terminals (TETCO/KM, myQuorum tenants) and pipelines |

## Coverage

### LNG Feedgas Observatory
| Terminal | Pipeline / Platform | Status |
| :--- | :--- | :--- |
| **Freeport LNG** | Gulf South Pipeline (Boardwalk public OAC, tspId=1) · TSQ at Stratton Ridge (loc 24329, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d | ✅ Live |

Freeport is additionally fed by TETCO (KM); full multi-pipeline per-terminal coverage is a later wave.

**Not yet covered:** Golden Pass, Cameron LNG, Sabine Pass, Plaquemines, Corpus Christi, Calcasieu Pass, Cove Point, Port Arthur. Terminal chips for these render as placeholders on the dashboard.

## Data Retention — why history starts small
FERC Electronic Bulletin Boards (EBBs) only retain roughly **90 days** of postings. Older cycle CSVs cannot be recovered retroactively, so curated histories **accumulate forward from each source's first scrape** rather than starting with a long back-history:

- Gulf South scheduled quantities begin **2026-05-25** — that is the platform's retention horizon from first scrape, not a gap in our collection.
- Charts reflect this: year-over-year lines and 2-year envelopes build automatically as gas years accumulate ("Historical envelope builds as data accumulates").

If you read a chart, assume any history older than the first-scrape date is unknowable from these platforms.

## Architecture
```text
Supply-Demand-Flows/
├── scrapers/           # Raw data ingestion from EIA, EBBs, AIS, etc.
├── transformers/       # Normalization and cleaning logic
├── derived/            # High-level metrics (e.g. implied demand, salt/non-salt splits)
├── publishers/         # Formatted outputs for visualization layers
├── validators/         # Data integrity and health checks
├── schemas/            # Canonical asset registries (assets.yaml)
├── data/               # Local tiered storage (raw, curated, health)
├── docs/               # Frontend visualization templates and documentation
└── tests/              # Pytest suite for ingestion and transformation pipelines
```

## Ecosystem
Blue Tide is part of a broader intelligence suite:
- **Blue Meridian**: Strategic regional analysis
- **Weather Desk**: Meteorological impact modeling
- **Blue Flux**: Real-time flow monitoring
- **Blue Pulse**: Market sentiment and volatility
- **Blue Margin**: Natural gas margin intelligence

## License
MIT
