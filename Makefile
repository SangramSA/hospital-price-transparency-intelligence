.PHONY: venv install install-dev test coverage lint format typecheck pre-commit

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
