from __future__ import annotations

from hpt.config import default_hospitals_config_path
from hpt.pipeline import resolve_hospital_keys


def test_resolve_hospital_keys_all_none() -> None:
    assert resolve_hospital_keys(config_path=default_hospitals_config_path(), hospital_keys=None, tier=None) is None


def test_resolve_hospital_keys_tier_filters() -> None:
    cfg = default_hospitals_config_path()
    keys = resolve_hospital_keys(config_path=cfg, hospital_keys=None, tier=1)
    assert keys is not None
    assert len(keys) >= 1
    for k in keys:
        assert isinstance(k, str)
