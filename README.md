# 🌊 Blue Tide
**The physical observatory of North American natural gas.**

**Live dashboard:** [https://yieldchaser.github.io/Supply-Demand-Flows/](https://yieldchaser.github.io/Supply-Demand-Flows/)

## Philosophy
Blue Tide is an observatory, not an oracle. We prioritize transparency over prediction. The system is built on a foundation of zero randomness — no stochastic models, no hidden seeds, and no synthetic noise. Our goal is to make every molecule of natural gas visible through high-fidelity data collection, transformation, and visualization.

## Status
![Status](https://img.shields.io/badge/status-12%20live%20sources%20%C2%B7%20observatory%20shipped-brightgreen)

Twelve ingestion pipelines run on schedule in CI. The **LNG Feedgas Observatory** covers all 8 operational U.S. export terminals with mathematically invariant cross-panel models, automated coverage guards, and multi-pipeline feed routing across 18 interactive dashboard panels.

## Observatory Panels (18 Live Panels)
The observatory dashboard (`docs/index.html`) renders 18 specialized panels organized into eight domain sections:
1. **National Balance & Macro**: Storage Trajectory (EIA weekly), Supply & Demand Balance (EIA monthly), Rig Counts (Baker Hughes weekly), Basin Production Trajectories.
2. **Basin Momentum Deep Dive**: Basin Metrics Table, Trajectory Scatter, Regional Share Trends, Historical Basin Extremes.
3. **Cross-Market Context**: Transatlantic Storage Divergence (US vs EU derived), European Storage Inventory (GIE AGSI+ daily).
4. **Macro LNG Exports**: U.S. LNG Export Volumes (EIA monthly), Export Destination Shares (EIA monthly).
5. **LNG Feedgas Fleet**: Fleet Overview Cards (Section 7, all 8 terminals with 8-day sparklines and latest utilization).
6. **LNG Feedgas Detail**: Hero Terminal Feedgas (Section 5, multi-feed stacked and single-meter with full cycle revision tracking).
7. **Feedgas Cross-Analysis**: Multi-Terminal Normalized & Absolute Comparison (Section 6), Multi-Pipeline Feed Substitution.
8. **Basin & Terminal Reliability**: Permian Basin Egress (Matterhorn / Gulf South daily), Terminal Downtime & Outage Tracking (Section 8, automated outage classification, cargo zero tracking, and event timelines).

## Data Sources

### Live (12 Curated Datasets)
| Source | Platform / Provider | Feeds & Scope | Curated Periods / Gas Days |
| :--- | :--- | :--- | :--- |
| **EIA Storage** | U.S. Energy Information Administration | Weekly working gas in underground storage by region | 451 weekly periods (2018–2026) |
| **EIA Supply** | U.S. Energy Information Administration | Monthly dry production, net imports, and total consumption | 78 monthly periods (2020–2026) |
| **EIA LNG Exports** | U.S. Energy Information Administration | Monthly gross LNG export volumes and country shares | 65 monthly periods (2021–2026) |
| **Baker Hughes** | Baker Hughes Rig Count | Weekly oil and gas drilling rig counts by basin | 139 weekly periods (2024–2026) |
| **GIE AGSI+** | Gas Infrastructure Europe | Daily European underground gas storage inventory and fullness % | 2,069 daily periods (2021–2026) |
| **Quorum** | myQuorumCloud IPWS (Tsp 2 & 10) | Plaquemines (Gator Express) and Calcasieu Pass (TransCameron) SQ | 1,996 gas days (2021–2026) |
| **Enbridge** | Enbridge Link EBB | TETCO delivery into Freeport LNG (loc 79999) + Southeast/Midwest egress | 1,107 gas days (2023–2026) |
| **BHE** | Berkshire Hathaway Energy / EGTS | Cove Point LNG consolidated plant intake delivery meter (loc 10001) | 612 gas days (2024–2026) |
| **Gulf South** | Energy Transfer / BWP GasQuest (Tsp 1) | Freeport LNG feedgas at Stratton Ridge (loc 24329) | 101 gas days (2026) |
| **GasNom** | GasNom ESG-Latitude | Cameron LNG (loc 772300), Golden Pass (loc 1097217), Sabine Pipe Line | 99 gas days (2026) |
| **Cheniere** | lngconnection API (Tsp 200 & 400) | Sabine Pass (Creole Trail Pipeline) and Corpus Christi Pipeline | 101 gas days (2026) |
| **Kinder Morgan** | KM pipeline2 EBB | NGPL (Sabine Pass context) and TGP (Corpus Christi comparison) | 10 gas days (2026) |

### Planned — not yet implemented
| Source | Intended Use |
| :--- | :--- |
| **AISStream** | Real-time vessel tracking → LNG carrier movements and berth tracking |
| **ENTSOG** | European cross-border transmission flows and pipeline interconnects |
| **SEC EDGAR** | Institutional filings (10-K, 10-Q) for operator capacity announcements |

## Coverage

### LNG Feedgas Observatory
All 8 operational U.S. LNG export terminals are live, coverage-guarded, and tracked across Section 5 (Hero Feedgas), Section 7 (Fleet Overview), and Section 8 (Terminal Downtime):

| Terminal | Pipeline / Platform | Feeds & Signal Type | Nameplate | Coverage & Baseline | History Depth | Status |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Freeport LNG** | Gulf South (BWP) + TETCO (Enbridge) | Multi-feed SQ (2 feeds) | 2,100 MMcf/d | 52.9% median (KMTP intrastate feed missing) | 101 gas days | ✅ Live |
| **Cove Point LNG** | BHE Cove Point Pipeline | Plant intake delivery SQ (loc 10001) | 750 MMcf/d | 97.1% median (full plant gate meter) | 612 gas days | ✅ Live |
| **Sabine Pass LNG** | Cheniere Creole Trail (CTPL) | Inferred delivery (OAC proxy) + NGPL context | 4,500 MMcf/d | 30.3% measured (non-CTPL feeds invisible) | 101 gas days | ✅ Live |
| **Plaquemines LNG** | Quorum Gator Express | Delivery SQ at VGPQD (loc vgpqd) | 3,400 MMcf/d | 112.4% median (Phase 1 & 2 commissioning) | 1,996 gas days | ✅ Live |
| **Cameron LNG** | GasNom Cameron Interstate (CIP) | Delivery SQ (loc 772300) | 2,000 MMcf/d | 72.9% measured (Columbia Gulf unmeasured) | 99 gas days | ✅ Live |
| **Calcasieu Pass LNG**| Quorum TransCameron | Delivery SQ at VGCPD (loc vgcpd) | 1,300 MMcf/d | 123.5% median (runs above baseload design) | 1,996 gas days | ✅ Live |
| **Golden Pass LNG** | GasNom Golden Pass Pipeline | Consolidated plant intake SQ (loc 1097217) | 2,600 MMcf/d | 12.7% median (active Train 1 commissioning) | 99 gas days | ✅ Live |
| **Corpus Christi LNG**| Cheniere Corpus Christi Pipeline | Inferred delivery (OAC proxy) + TGP comparison | 2,400 MMcf/d | 99.4% median (Cheniere pipeline gate) | 101 gas days | ✅ Live |
| **Port Arthur LNG** | GasNom Port Arthur Pipeline | Pipeline delivery SQ | 1,900 MMcf/d | 0.0% (construction / pre-commissioning) | 99 gas days | ⏳ Non-Op |

### History Depth & Data Retention
FERC Electronic Bulletin Boards (EBBs) enforce differing data retention horizons:
- **Quorum (Plaquemines, Calcasieu Pass)**: 1,996 distinct gas days (history extends back to March 2021, ~5.5 years).
- **Enbridge (TETCO / Freeport)**: 1,107 distinct gas days (history extends back to August 2023, ~3 years).
- **BHE (Cove Point)**: 612 distinct gas days (history extends back to December 2024, ~1.7 years).
- **Gulf South, GasNom, Cheniere**: ~99–101 distinct gas days in curated storage (collection initiated May 2026). Upstream servers enforce rolling retention windows (~90 days for Gulf South and Cheniere; up to 3 years queryable via bulk TSV for GasNom).
- **Kinder Morgan**: 10 distinct gas days (collection initiated August 2026; server does not support dated queries and accumulates daily).

When viewing LNG panels, time presets (such as 1-year) automatically surface honest provenance caveats if the requested window exceeds the available historical horizon.

## Architecture
```text
Supply-Demand-Flows/
├── scrapers/           # Raw data ingestion from EIA, EBBs, AIS, etc.
├── transformers/       # Normalization, cycle priority, and cleaning logic
├── derived/            # High-level metrics (implied demand, storage divergence)
├── publishers/         # Formatted outputs for visualization layers
├── validators/         # Coverage anti-rot guards, integrity rules, and health checks
├── schemas/            # Canonical asset registries (assets.yaml)
├── data/               # Local tiered storage (raw, curated, health)
├── docs/               # Frontend visualization templates, D3 charts, and panels
└── tests/              # Pytest & Node.js test suites with mathematical invariants
```

## Parallel-Agent Workflow Policy
Multiple AI agents work in this repository. **Each agent MUST use its own clone or `git worktree` — never a single shared working tree.** On 2026-08-23 two agents sharing one checkout switched branches under each other mid-run: commits landed on the wrong branch, background jobs died when files vanished from disk, and integration required SHA-level forensic reconciliation. Separate clones/worktrees make this class of incident structurally impossible.

## Ecosystem
Blue Tide is part of a broader intelligence suite:
- **Blue Meridian**: Strategic regional analysis
- **Weather Desk**: Meteorological impact modeling
- **Blue Flux**: Real-time flow monitoring
- **Blue Pulse**: Market sentiment and volatility
- **Blue Margin**: Natural gas margin intelligence

## License
MIT
