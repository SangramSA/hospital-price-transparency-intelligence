"""Allow `python -m hpt` to invoke the CLI."""

from hpt.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
