# Hospital price transparency intelligence

Pipeline scaffold for collecting hospital machine-readable price transparency files (MRFs), normalizing negotiated rates for total knee (HCPCS 27447), and joining to CMS Medicare knee-replacement provider data. See [docs/design.md](docs/design.md) and [docs/regulatory-and-assessment-reference.md](docs/regulatory-and-assessment-reference.md) for scope and domain context.

## Prerequisites

- **Python 3.11+** (3.11 is the minimum supported version; use `python3.11` if your default `python3` is older).
- **pip** (recent enough for PEP 517 builds).
- Optional: **pre-commit** (`pip install pre-commit`) if you want git hooks for Ruff.

## Environment setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -U pip
pip install -e ".[dev]"
```

Or use the Makefile (creates `.venv` and installs dev extras):

```bash
make install-dev
```

Copy `.env.example` to `.env` when you add settings that read from the environment.

## CLI (scaffold)

```bash
hpt              # print help
hpt --version    # print version
python -m hpt    # same as hpt when the package is on PYTHONPATH / installed editable
```

Pipeline subcommands (`discover`, `download`, `extract`, …) will be added in later phases.

## Tests and linting

```bash
pytest
ruff check src tests
ruff format src tests
mypy src
```

`make test`, `make lint`, `make format`, and `make typecheck` run the same commands (invoke from an activated venv with dev deps installed).

## Optional pre-commit

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
```

## Data layout

- `config/` — hospital roster and pipeline configuration (to be added).
- `data/raw/` — downloaded MRFs (contents gitignored; `.gitkeep` preserves the directory).
- `data/processed/` — combined exports and intermediates (contents gitignored).
- `data/cms_knee_replacement_by_provider.csv` — CMS extract tracked in-repo.

## Pipeline Lifecycle (What's Guaranteed)
The pipeline is organized as: `discovery -> download -> raw storage -> parse/extract -> normalize -> join -> export`.

Key governance and operability expectations:
- **Reproducibility:** manifests and exports are stamped with snapshot/hash metadata so results can be replayed.
- **Lineage:** every extracted row must be traceable back to its source file and position/path.
- **Deterministic parsing:** extraction is rule-based (no LLMs) and strategy-tagged for auditability.
- **Data quality is explicit:** parse/join issues are flagged (via DQ flags / nulls), not silently coerced away.
- **Run metadata vs determinism:** `extracted_at` is run-time metadata; treat it as non-deterministic for equality checks. Use the accompanying snapshot/hash fields to compare runs meaningfully.

Data quality taxonomy (DQ flags):
- Structural DQ: parse/layout problems (missing columns, unexpected nested shapes, parse failures).
- Semantic DQ: negotiated-rate meaning problems (expressed via `dq_flags` when extraction can emit a flag).
- Join DQ: CCN join failures/ambiguities under CCN-first policy (represented primarily via `cms_match_status` and null CMS fields like `cms_ccn`).

Semantic `dq_flags` codes (pipe-delimited in exports; empty/null means “no flags”):
- `algorithm_only_rate`: negotiated dollar amount is missing (`negotiated_amount` is null) while a rate-algorithm/string exists in `rate_raw`.
- `zero_negotiated_rate`: negotiated dollar amount exists and parses to exactly `0`.
- `unparseable_numeric` (reserved): numeric conversion for negotiated dollar failed; original text preserved in `rate_raw`.
- `missing_payer_name` (reserved): payer block is malformed and a payer name cannot be recovered reliably.
- `percent_of_charges_noncomparable` (reserved): rate type indicates percent-of-charges (or similar) where conversion to a comparable negotiated dollar is not available; downstream ratios should be treated as not comparable.

## Stage Contracts (Bronze/Silver/Gold)
This section defines the required lineage/reproducibility metadata surface between pipeline layers.

### Bronze — Discovery + Download (raw landing)
Required per hospital manifest:
- `content_sha256`
- `downloaded_at` (ISO-8601 UTC)
- `http_status`
- optional `etag`
- optional `last_modified`
- `local_path`

### Silver — Parse + Extract + Normalize (canonical rows)
Required per emitted canonical row:
- `source_row_index` (CSV) or `source_json_path` (JSON)
- `parser_strategy`
- `extractor_version`
- `dq_flags`

### Gold — Join + Export (dataset serving)
Required dataset-level reproducibility fields:
- `cms_snapshot_hash`
- `pipeline_version`
- `output_schema_version`

Determinism note: `extracted_at` is run-time metadata and should be excluded from deterministic row equality checks; use snapshot/hash fields to compare meaningful results across runs.

## Project layout

- `src/hpt/` — Python package (CLI, future discovery, download, parsers, join, export).
- `tests/` — pytest tests and fixtures.
