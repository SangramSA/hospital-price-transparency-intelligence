"""Load hospital roster from YAML and resolve path/HTTP settings from the environment."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import yaml

from hpt.constants import (
    DEFAULT_EXTRACT_STREAM_THRESHOLD_BYTES,
    DEFAULT_HTTP_MAX_RETRIES,
    DEFAULT_HTTP_TIMEOUT_SEC,
    DEFAULT_RAW_DIR,
    DEFAULT_SILVER_DIR,
    DEFAULT_USER_AGENT,
    ENV_CONFIG_PATH,
    ENV_EXTRACT_STREAM_THRESHOLD_BYTES,
    ENV_HTTP_MAX_RETRIES,
    ENV_HTTP_TIMEOUT_SEC,
    ENV_HTTP_USER_AGENT,
    ENV_RAW_DIR,
    ENV_SILVER_DIR,
)
from hpt.models import Hospital

logger = logging.getLogger(__name__)


def project_root() -> Path:
    """Repository root (contains pyproject.toml)."""
    here = Path(__file__).resolve()
    return here.parents[2]


def default_hospitals_config_path() -> Path:
    override = os.environ.get(ENV_CONFIG_PATH)
    if override:
        return Path(override).expanduser().resolve()
    return project_root() / "config" / "hospitals.yaml"


def raw_dir() -> Path:
    raw = os.environ.get(ENV_RAW_DIR, DEFAULT_RAW_DIR)
    return Path(raw).expanduser().resolve()


def silver_dir() -> Path:
    s = os.environ.get(ENV_SILVER_DIR, DEFAULT_SILVER_DIR)
    return Path(s).expanduser().resolve()


def extract_stream_threshold_bytes() -> int:
    raw = os.environ.get(ENV_EXTRACT_STREAM_THRESHOLD_BYTES)
    if raw is None or raw == "":
        return int(DEFAULT_EXTRACT_STREAM_THRESHOLD_BYTES)
    return max(0, int(raw))


def http_timeout_sec() -> float:
    raw = os.environ.get(ENV_HTTP_TIMEOUT_SEC)
    if raw is None or raw == "":
        return float(DEFAULT_HTTP_TIMEOUT_SEC)
    return float(raw)


def http_max_retries() -> int:
    raw = os.environ.get(ENV_HTTP_MAX_RETRIES)
    if raw is None or raw == "":
        return DEFAULT_HTTP_MAX_RETRIES
    return max(0, int(raw))


def http_user_agent() -> str:
    return os.environ.get(ENV_HTTP_USER_AGENT, DEFAULT_USER_AGENT)


def load_hospitals(config_path: Path | None = None) -> list[Hospital]:
    """Parse config/hospitals.yaml into Hospital records."""
    path = config_path or default_hospitals_config_path()
    if not path.is_file():
        msg = f"Hospital config not found: {path}"
        raise FileNotFoundError(msg)
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict) or "hospitals" not in data:
        msg = f"Invalid hospital config structure (missing 'hospitals'): {path}"
        raise ValueError(msg)
    rows = data["hospitals"]
    if not isinstance(rows, list):
        msg = f"Invalid hospital config: 'hospitals' must be a list in {path}"
        raise ValueError(msg)
    out: list[Hospital] = []
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            msg = f"Hospital entry {i} must be a mapping in {path}"
            raise ValueError(msg)
        try:
            out.append(
                Hospital(
                    hospital_key=str(row["hospital_key"]),
                    hospital_name=str(row["hospital_name"]),
                    state=str(row["state"]),
                    ccn=str(row.get("ccn")).strip() if row.get("ccn") not in (None, "") else None,
                    tier=int(row["tier"]),
                    website_root=str(row["website_root"]),
                    cms_hpt_index_url=row.get("cms_hpt_index_url"),
                    mrf_url=row.get("mrf_url"),
                    source_page_url=row.get("source_page_url"),
                )
            )
        except KeyError as e:
            msg = f"Hospital entry {i} missing required field {e!s} in {path}"
            raise ValueError(msg) from e
    logger.info(f"Loaded {len(out)} hospitals from hospitals.yaml")
    return out


def filter_hospitals(hospitals: list[Hospital], keys: set[str] | None) -> list[Hospital]:
    if not keys:
        return hospitals
    selected = [h for h in hospitals if h.hospital_key in keys]
    missing = keys - {h.hospital_key for h in selected}
    if missing:
        msg = f"Unknown hospital_key(s): {sorted(missing)}"
        raise ValueError(msg)
    return selected
