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

## Project layout

- `src/hpt/` — Python package (CLI, future discovery, download, parsers, join, export).
- `tests/` — pytest tests and fixtures.
