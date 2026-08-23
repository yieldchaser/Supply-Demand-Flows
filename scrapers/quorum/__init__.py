"""Quorum myQuorumCloud IPWS scraper package.

Pipeline coverage:
    Gator Express  (Plaquemines LNG feedgas)   TspNo=2   [pipelines.py]
    TransCameron   (Calcasieu Pass LNG feedgas) TspNo=10  [pipelines.py]
    Historical backfill with checkpointing              [backfill.py]

The tenant-generic ``QuorumIPWSScraper`` also serves future Quorum IPWS
families (BBTPA1IPWS, HPEPA1IPWS, PNGPA1IPWS, ...) by parameterising
tenant + tsp_no.
"""
