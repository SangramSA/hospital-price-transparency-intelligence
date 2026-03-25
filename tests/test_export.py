from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from hpt.constants import OUTPUT_SCHEMA_VERSION
from hpt.export import run_export
from hpt.extract import CANONICAL_COLUMNS


def _minimal_row(**overrides: object) -> dict[str, object]:
    base = {k: None for k in CANONICAL_COLUMNS}
    base.update(
        {
            "hospital_key": "h-a",
            "hospital_name": "Hospital A",
            "state": "XX",
            "procedure_code": "27447",
            "rate_type": "negotiated",
            "payer_name_normalized": "payer",
            "payer_name": "Payer",
            "cms_match_status": "matched_ccn_roster",
            "cms_snapshot_hash": "abc123",
        }
    )
    base.update(overrides)
    return base


def test_export_deterministic_sort_and_artifacts() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        joined = root / "joined" / "h-a"
        joined.mkdir(parents=True)
        p = joined / "x.joined.jsonl"
        rows = [
            _minimal_row(
                payer_name_normalized="zebra",
                source_row_index=2,
                negotiated_amount=100.0,
            ),
            _minimal_row(
                payer_name_normalized="alpha",
                source_row_index=1,
                negotiated_amount=200.0,
            ),
        ]
        p.write_text(
            "\n".join(json.dumps(r, sort_keys=True) for r in rows) + "\n", encoding="utf-8"
        )

        out = root / "out"
        result = run_export(joined_root=root / "joined", output_dir=out, write_jsonl=True)

        assert result.row_count == 2
        assert out.joinpath("combined.csv").is_file()
        assert out.joinpath("qa_summary.json").is_file()
        assert out.joinpath("export_metadata.json").is_file()
        assert result.combined_jsonl is not None
        assert result.combined_jsonl.is_file()

        lines = out.joinpath("combined.csv").read_text(encoding="utf-8").strip().splitlines()
        assert lines[0].split(",")[0] == "hospital_key"
        # Second row should be alpha (sorted before zebra)
        assert "alpha" in lines[1]

        meta = json.loads(out.joinpath("export_metadata.json").read_text(encoding="utf-8"))
        assert meta["pipeline_version"]
        assert meta["output_schema_version"] == OUTPUT_SCHEMA_VERSION
        assert meta["cms_snapshot_hash"] == "abc123"
        assert meta["row_count"] == 2

        qa = json.loads(out.joinpath("qa_summary.json").read_text(encoding="utf-8"))
        assert qa["rows_total"] == 2
        assert "cms_match_status_counts" in qa
        assert qa["null_rates"]["negotiated_amount"] == 0.0


def test_export_raises_when_no_joined_files() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "joined").mkdir(parents=True)
        with pytest.raises(FileNotFoundError):
            run_export(joined_root=root / "joined", output_dir=root / "out")
