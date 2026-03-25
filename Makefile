.PHONY: venv install install-dev test coverage lint format typecheck pre-commit snapshot-ui-data ui

PYTHON ?= python3.11
VENV ?= .venv

venv:
	$(PYTHON) -m venv $(VENV)
	@echo "Activate: source $(VENV)/bin/activate  (Windows: $(VENV)\\Scripts\\activate)"

install: venv
	. $(VENV)/bin/activate && pip install -U pip && pip install -e .

install-dev: venv
	. $(VENV)/bin/activate && pip install -U pip && pip install -e ".[dev]"

test:
	pytest

coverage:
	pytest --cov=hpt --cov-report=term-missing

lint:
	ruff check src tests

format:
	ruff format src tests

typecheck:
	mypy src

pre-commit:
	pre-commit install
	pre-commit run --all-files

snapshot-ui-data:
	mkdir -p app/data
	cp data/processed/combined.csv app/data/combined.csv
	cp data/processed/qa_summary.json app/data/qa_summary.json
	cp data/processed/export_metadata.json app/data/export_metadata.json
	@echo "UI data snapshot updated in app/data/"

ui:
	streamlit run app/streamlit_app.py
