"""Named constants for procedure codes and environment configuration (no magic strings in logic)."""

from __future__ import annotations

# Procedure focus (45 CFR Part 180 / assessment scope)
HCPCS_TKA = "27447"
DRG_MAJOR_JOINT_WITH_MCC = "469"
DRG_MAJOR_JOINT_WITHOUT_MCC = "470"

# Environment variable names (paths and HTTP)
ENV_RAW_DIR = "HPT_RAW_DIR"
ENV_HTTP_TIMEOUT_SEC = "HPT_HTTP_TIMEOUT_SEC"
ENV_HTTP_MAX_RETRIES = "HPT_HTTP_MAX_RETRIES"
ENV_HTTP_USER_AGENT = "HPT_HTTP_USER_AGENT"
ENV_CONFIG_PATH = "HPT_HOSPITALS_CONFIG"

# Defaults
DEFAULT_RAW_DIR = "data/raw"
DEFAULT_HTTP_TIMEOUT_SEC = 120
DEFAULT_HTTP_MAX_RETRIES = 3
# Browser-shaped default: some hospital CDNs/WAFs return 403 for non-browser User-Agents on
# public MRF/cms-hpt URLs. Override with HPT_HTTP_USER_AGENT for an explicit bot identity.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 hpt/0.1"
)

# Raw artifact filenames
CMS_HPT_LOCAL_NAME = "cms-hpt.txt"
MANIFEST_JSON_NAME = "manifest.json"
