# 🌊 Blue Tide
**The physical observatory of North American natural gas.**

## Philosophy
Blue Tide is an observatory, not an oracle. We prioritize transparency over prediction. The system is built on a foundation of zero randomness — no stochastic models, no hidden seeds, and no synthetic noise. Our goal is to make every molecule of natural gas visible through high-fidelity data collection, transformation, and visualization.

## Status
![Status](https://img.shields.io/badge/status-phase%201%20foundation-blue)

## Data Sources
The observatory aggregates data from the following free and public sources:

| Source | Description | Type |
| :--- | :--- | :--- |
| **EIA** | U.S. Energy Information Administration API | Production, Storage, Demand |
| **Baker Hughes** | North American Rig Count | Supply Leading Indicator |
| **AISStream** | Real-time vessel tracking | LNG Ship Movements |
| **GIE AGSI+** | Gas Infrastructure Europe | Global Storage Context |
| **ENTSOG** | European Network of Transmission System Operators | Cross-border flows |
| **FERC EBBs** | Electronic Bulletin Boards | Pipeline Nominations & Capacity |
| **SEC EDGAR** | Securities and Exchange Commission | Institutional Filings (10-K, 10-Q) |

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