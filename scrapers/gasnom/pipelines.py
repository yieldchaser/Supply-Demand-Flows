"""Frozen per-pipeline configuration for the GASNom ESG-Latitude scrapers.

Why:
    All four GASNom-hosted pipelines share one ColdFusion template; the only
    thing that varies is the URL slug, the curated series prefix, the LNG
    terminal it feeds, and the terminal's nameplate capacity.  A frozen
    dataclass registry keeps those facts in one auditable place instead of
    scattered literals.

What:
    ``GasnomPipeline`` — immutable per-pipeline config.
    ``GASNOM_PIPELINES`` — slug → config mapping (the single source of truth
    consumed by client, backfill, transformer, and tests).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GasnomPipeline:
    """Immutable configuration for one GASNom-hosted pipeline.

    Attributes:
        slug: Path segment on gasnom.com.  Sabine is uppercase (``SABINE``)
            — the site's ColdFusion templates are case-sensitive in practice.
        name: Human-readable pipeline name as reported by the TSP header.
        series_prefix: Prefix for curated series ids, e.g.
            ``{prefix}_sq_{loc}_{cycle_code}`` / ``{prefix}_oac_{loc}_{cycle_code}``.
        terminal: Blue Tide terminal identifier this pipeline feeds.
        nameplate_dth: Terminal nameplate liquefaction capacity in Dth/d
            (context metadata only — never used to alter scraped values).
    """

    slug: str
    name: str
    series_prefix: str
    terminal: str
    nameplate_dth: int


GOLDEN_PASS = GasnomPipeline(
    slug="goldenpass",
    name="Golden Pass Pipeline LLC",
    series_prefix="golden_pass",
    terminal="golden_pass_lng",
    nameplate_dth=2600,
)

CAMERON_INTERSTATE = GasnomPipeline(
    slug="cameron",
    name="Cameron Interstate Pipeline LLC",
    series_prefix="cameron_interstate",
    terminal="cameron_lng",
    nameplate_dth=2000,
)

SABINE_PIPE_LINE = GasnomPipeline(
    slug="SABINE",
    name="Sabine Pipe Line, LLC",
    series_prefix="sabine_pipe_line",
    terminal="sabine_pass_lng",
    nameplate_dth=4500,
)

PORT_ARTHUR_PIPELINE = GasnomPipeline(
    slug="portarthurpipeline",
    name="Port Arthur Pipeline, LLC",
    series_prefix="port_arthur_pipeline",
    terminal="port_arthur_lng",
    nameplate_dth=1900,
)

#: Slug → frozen config; the registry every other module iterates.
GASNOM_PIPELINES: dict[str, GasnomPipeline] = {
    GOLDEN_PASS.slug: GOLDEN_PASS,
    CAMERON_INTERSTATE.slug: CAMERON_INTERSTATE,
    SABINE_PIPE_LINE.slug: SABINE_PIPE_LINE,
    PORT_ARTHUR_PIPELINE.slug: PORT_ARTHUR_PIPELINE,
}
