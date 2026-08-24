## VERDICT 2026-08-24 (CI live-fire): the legacy JSP flow is DEAD

The williams.yml workflow ran successfully from GitHub Actions runners
(no WAF block — GETs return 200), but every response for the JSP paths is
a 44,459-byte Angular SPA shell (title `portal-app`). Discrimination test:
`/api/definitely-not-real-xyz` and `/api/portal-service/v1/public/oac/cycle`
both return the identical shell — there is NO public API on this host;
every path serves the SPA catch-all. Williams migrated 1Line to the new
portal-app; `OACQueryRequest.jsp` / `OACreport.jsp` no longer exist.

Consequence: this scraper can never return rows again. Kept in-tree as a
parser reference + watchlist config; CI workflow left scheduled but will
record `empty`/warn health until a new data source (the SPA's private API,
which requires an auth handshake we have not replicated) is wired up.
Do not trust any future `status: ok` from this source without re-verifying
the endpoint landscape first.
