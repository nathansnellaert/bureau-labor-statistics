# bureau-labor-statistics

Time series data from the U.S. Bureau of Labor Statistics, exposed as 28 topic-based datasets covering prices, employment, productivity, and labor force statistics.

## Source

- API: BLS Public API v2 (`https://api.bls.gov/publicAPI/v2/`)
- Auth: `BLS_API_KEY` environment variable (registered users get higher quotas)
- License: U.S. Government Work, public domain
- Docs: <https://www.bls.gov/developers/>

## Coverage

We pull ~10,500 series across 28 surveys. Selection is documented in `catalog.json`:
- **Top 500 series per survey by BLS popularity rank**, with `OE` (Occupational Employment) and `WM` (Modeled Wages) bumped to 2,000 because they're point-in-time surveys with a single observation per series.
- **Hardcoded JOLTS fallback** (20 series) for the `JT` survey, which is missing from both the catalog and `popular_series` API.
- The selected series IDs are pinned in `series_catalog.txt` (one per line, ranked by popularity). The list was generated once via `discovery/download_series_list.py`, which scrapes <https://data.bls.gov/dataQuery/find>.

### What's intentionally excluded

- **QCEW (`EN`)** — Quarterly Census of Employment and Wages is not available through the BLS timeseries API. It ships as bulk downloads at <https://www.bls.gov/cew/downloadable-data-files.htm> and would need a separate ingestion path.

## Refresh strategy

The connector is designed to be re-runnable cheaply. Two modes:

| Mode | Window | When |
|------|--------|------|
| **backfill** | 20 years | First run, or when the last backfill is older than 90 days |
| **refresh** | 2 years (rolling) | Default for routine runs |

The transform step uses `merge(key=["series_id", "date"])` so historical rows are preserved across runs. A refresh run only upserts new and revised observations into the existing tables — no full overwrite.

State lives in `data/state/series_data.json`:
```json
{
  "backfill_done": true,
  "last_full_refresh": "2026-04-13"
}
```

If the BLS daily quota is hit mid-run, completed series are persisted to state and the next invocation resumes from where the previous one stopped.

## Output schema

Every dataset has the same long-format schema:

| Column | Type | Description |
|--------|------|-------------|
| `series_id` | string | BLS series identifier — the canonical row key. Join back to the BLS catalog for full metadata. |
| `date` | string | `YYYY`, `YYYY-MM`, `YYYY-QN`, or `YYYY-HN` |
| `indicator` | string | Full series title |
| `unit` | string | Best-effort unit (`percent`, `index`, `thousands`, `dollars`, `hours`, `rate`, ...) — may be empty |
| `value` | float64 | Numeric observation |
| *(varying dimensions)* | string | Per-dataset: `seasonality`, `area`, `area_type`, `industry`, `occupation`, `demographic_*` — only included when they actually vary within the dataset |

Composite primary key: `(series_id, date)`.

### Caveats

- `bls_unemployment_local`, `bls_employment_state`, and `bls_occupational_employment` mix multiple units (rates, levels, percent changes) under one `value` column. The `unit` column helps disambiguate but isn't always populated. Splitting these by unit is a follow-up.

## Running locally

```bash
cd integrations/bureau-labor-statistics
export BLS_API_KEY=...
uv run python src/main.py
```

To force a deep refresh, delete `data/state/series_data.json` (or set `last_full_refresh` to an old date).

To deploy: `python scripts/deploy.py bureau-labor-statistics`.
