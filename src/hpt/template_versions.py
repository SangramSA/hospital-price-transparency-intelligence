"""Detect CMS template version signals and basic conformance by source file."""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path

import ijson

from hpt.csv_encoding import CSV_TEXT_ENCODING_ORDER

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TemplateDetection:
    template_version_raw: str | None
    template_family: str
    format_kind: str
    is_conformant: bool
    issues: tuple[str, ...]

    @property
    def strategy_suffix(self) -> str:
        return f"{self.format_kind}_{self.template_family}"


def _family_from_version(raw: str | None) -> str:
    if not raw:
        return "unknown"
    text = str(raw).strip()
    m = re.search(r"(?:^|[^0-9])([23])(?:\D|$)", text)
    if not m:
        m = re.search(r"([23])\.\d", text)
    if not m:
        return "unknown"
    major = int(m.group(1))
    if major >= 3:
        return "v3"
    if major == 2:
        return "v2"
    return "unknown"


def _read_csv_first_rows(path: Path) -> tuple[list[str], list[str], list[str]]:
    last_err: Exception | None = None
    for enc in CSV_TEXT_ENCODING_ORDER:
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                r1 = next(reader, [])
                r2 = next(reader, [])
                r3 = next(reader, [])
                return r1, r2, r3
        except UnicodeDecodeError as e:
            last_err = e
            continue
    raise ValueError(f"Unable to read CSV header rows for {path}: {last_err}")


def detect_csv_template(path: Path) -> TemplateDetection:
    r1, r2, r3 = _read_csv_first_rows(path)
    meta_header = [str(x).strip() for x in r1]
    meta_row = [str(x).strip() for x in r2]
    table_header = [str(x).strip() for x in r3]
    if any(str(x).strip().lower() == "code|1" for x in meta_header):
        # Header-only CSV fixture / non-metadata format.
        table_header = meta_header
        meta_header = []
        meta_row = []

    version_raw: str | None = None
    idx_version = None
    for i, h in enumerate(meta_header):
        if h.lower() == "version":
            idx_version = i
            break
    if idx_version is not None and idx_version < len(meta_row):
        version_raw = meta_row[idx_version].strip() or None

    family = _family_from_version(version_raw)
    issues: list[str] = []

    if not table_header:
        issues.append("missing_table_header_row")
    else:
        lower_header = {h.lower().replace(" ", "") for h in table_header}
        if "code|1" not in lower_header:
            issues.append("missing_code_1_column")
        if "standard_charge|gross" not in lower_header:
            issues.append("missing_standard_charge_gross")
        has_tall_payer = "payer_name" in lower_header
        has_wide_payer = any(
            h.startswith("standard_charge|") and h.endswith("|negotiated_dollar")
            for h in lower_header
        )
        if not has_tall_payer and not has_wide_payer:
            issues.append("missing_payer_negotiated_columns")

    meta_lower = {h.lower().replace(" ", "") for h in meta_header}
    if family == "v3":
        if "type_2_npi" not in meta_lower and "type2_npi" not in meta_lower:
            issues.append("v3_missing_type_2_npi_metadata")
        if not any("attest" in h for h in meta_lower):
            issues.append("v3_missing_attestation_metadata")
    elif family == "v2":
        if not any("best of its knowledge" in h.lower() for h in meta_header):
            issues.append("v2_missing_affirmation_like_metadata")
    else:
        # Unknown versions are common for test fixtures and non-standard exports.
        # Keep classification unknown but avoid marking hard non-conformance by default.
        pass

    return TemplateDetection(
        template_version_raw=version_raw,
        template_family=family,
        format_kind="csv",
        is_conformant=not issues,
        issues=tuple(dict.fromkeys(issues)),
    )


def detect_json_template(path: Path) -> TemplateDetection:
    version_raw: str | None = None
    top_keys: set[str] = set()
    pending_version_value = False
    try:
        with path.open("rb") as f:
            for prefix, event, value in ijson.parse(f):
                if prefix == "" and event == "map_key":
                    k = str(value)
                    top_keys.add(k)
                    pending_version_value = k == "version"
                    if k == "standard_charge_information":
                        break
                elif (
                    pending_version_value
                    and prefix == "version"
                    and event
                    in {
                        "string",
                        "number",
                    }
                ):
                    version_raw = str(value)
                    pending_version_value = False
    except Exception as e:
        logger.warning("template detect failed for JSON %s: %s", path, e)
        return TemplateDetection(
            template_version_raw=None,
            template_family="unknown",
            format_kind="json",
            is_conformant=False,
            issues=("json_parse_error_for_template_detection",),
        )

    family = _family_from_version(version_raw)
    issues: list[str] = []
    if "standard_charge_information" not in top_keys:
        issues.append("missing_standard_charge_information")
    if family == "v3":
        if "attestation" not in top_keys:
            issues.append("v3_missing_attestation_object")
    elif family == "v2":
        if "affirmation" not in top_keys and "attestation" not in top_keys:
            issues.append("v2_missing_affirmation_or_attestation")
    else:
        # Unknown/variant version strings exist in the wild.
        pass

    return TemplateDetection(
        template_version_raw=version_raw,
        template_family=family,
        format_kind="json",
        is_conformant=not issues,
        issues=tuple(dict.fromkeys(issues)),
    )


def detect_template_for_file(path: Path) -> TemplateDetection:
    suf = path.suffix.lower()
    if suf == ".csv":
        return detect_csv_template(path)
    if suf in {".json", ".jsonl"}:
        return detect_json_template(path)
    return TemplateDetection(
        template_version_raw=None,
        template_family="unknown",
        format_kind=suf.lstrip(".") or "unknown",
        is_conformant=False,
        issues=("unsupported_extension_for_template_detection",),
    )
