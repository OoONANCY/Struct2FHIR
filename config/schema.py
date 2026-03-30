"""Config loader and validator.

Loads a YAML source config, validates required keys, applies defaults,
and supports env-var fallback for secrets.
"""

import os
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

REQUIRED_COLUMN_MAP_KEYS = {"patient_id", "lab_name", "value", "unit"}
OPTIONAL_COLUMN_MAP_KEYS = {"collected_at", "reference_range"}

DEFAULTS = {
    "delimiter": ",",
    "encoding": "utf-8",
    "skip_rows": 0,
    "date_formats": ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y %H:%M"],
    "transform_rules": {"unit_map": {}, "custom_rules": []},
    "fhir_auth_token": "",
    "patient_id_system": "urn:oid:2.16.840.1.113883.3.unknown",
}


class ConfigError(Exception):
    """Raised when a config file is invalid."""


def load_config(path: str) -> dict:
    """Load and validate a YAML config file.

    Args:
        path: Path to the YAML config.

    Returns:
        Validated config dict with defaults applied.

    Raises:
        ConfigError: If the config is invalid.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with open(config_path, "r", encoding="utf-8") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc

    if not isinstance(config, dict):
        raise ConfigError(f"Config must be a YAML mapping, got {type(config).__name__}")

    _validate(config, path)
    _apply_defaults(config)
    _resolve_secrets(config)

    logger.info("Loaded config for source '%s' from %s", config.get("source_name"), path)
    return config


def _validate(config: dict, path: str) -> None:
    """Check required keys exist."""
    if "source_name" not in config:
        raise ConfigError(f"Missing 'source_name' in {path}")

    if "fhir_server_url" not in config:
        raise ConfigError(f"Missing 'fhir_server_url' in {path}")

    col_map = config.get("column_map")
    if not isinstance(col_map, dict):
        raise ConfigError(f"Missing or invalid 'column_map' in {path}")

    missing = REQUIRED_COLUMN_MAP_KEYS - set(col_map.keys())
    if missing:
        raise ConfigError(
            f"column_map is missing required keys: {', '.join(sorted(missing))} in {path}"
        )


def _apply_defaults(config: dict) -> None:
    """Fill in missing optional keys with defaults."""
    for key, default in DEFAULTS.items():
        if key not in config:
            config[key] = default

    # Ensure transform_rules sub-keys exist
    tr = config.setdefault("transform_rules", {})
    tr.setdefault("unit_map", {})
    tr.setdefault("custom_rules", [])


def _resolve_secrets(config: dict) -> None:
    """Use env-var fallback for sensitive fields."""
    if not config.get("fhir_auth_token"):
        env_token = os.environ.get("FHIR_AUTH_TOKEN", "")
        if env_token:
            config["fhir_auth_token"] = env_token
            logger.info("Using FHIR_AUTH_TOKEN from environment variable")
