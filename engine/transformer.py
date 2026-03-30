"""Transformer — cleans values, normalizes dates/units, applies custom rules."""

import logging
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class TransformError(Exception):
    """Raised when a row cannot be transformed and should be quarantined."""


def transform_row(row: dict, config: dict) -> dict:
    """Apply all transformation rules to a single normalized row.

    Args:
        row:    Dict with standardized keys from csv_reader.
        config: Validated config dict.

    Returns:
        Transformed row dict ready for LOINC resolution.

    Raises:
        TransformError: If a critical field cannot be processed.
    """
    row = dict(row)  # shallow copy
    rules = config.get("transform_rules", {})

    # 0. Validate required fields
    _validate_required_fields(row)

    # 1. Apply custom find/replace rules (BEFORE any cleaning)
    for rule in rules.get("custom_rules", []):
        field = rule["field"]
        if field in row and row[field]:
            row[field] = row[field].replace(rule["find"], rule["replace"])

    # 2. Normalize units
    unit_map = rules.get("unit_map", {})
    if "unit" in row and row["unit"]:
        raw = row["unit"]
        for src, target in unit_map.items():
            if raw.upper() == src.upper():
                row["unit"] = target
                break

    # 3. Parse / compute date
    row["effective_datetime"] = _resolve_datetime(row, config)

    # 4. Normalize numeric value
    if "value" in row and row["value"]:
        row["value"] = _normalize_value(row["value"])

    # 5. Anomaly detection against reference range
    _check_value_anomaly(row)

    # 6. Strip whitespace from all string fields
    for k, v in row.items():
        if isinstance(v, str):
            row[k] = v.strip()

    return row


def _validate_required_fields(row: dict) -> None:
    """Raise TransformError if critical fields are blank."""
    patient_id = row.get("patient_id", "").strip()
    if not patient_id:
        raise TransformError("Missing required field: patient_id is blank")

    lab_name = row.get("lab_name", "").strip()
    if not lab_name:
        raise TransformError("Missing required field: lab_name is blank")


def _resolve_datetime(row: dict, config: dict) -> str:
    """Resolve an effective datetime from the row.

    Supports two modes:
    - Standard: parse ``collected_at`` using ``date_formats``.
    - Offset:   compute from ``reference_date`` + ``_offset_minutes``.

    Returns ISO-8601 string or empty string if no date can be resolved.
    """
    # Offset mode (e.g. eICU)
    if "_offset_minutes" in row and config.get("offset_column"):
        try:
            offset = int(float(row["_offset_minutes"]))
            ref = config.get("reference_date", "2024-01-01T00:00:00Z")
            ref_dt = datetime.fromisoformat(ref.replace("Z", "+00:00"))
            result_dt = ref_dt + timedelta(minutes=offset)
            return result_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        except (ValueError, TypeError) as exc:
            logger.warning("Cannot compute offset datetime: %s", exc)
            return ""

    # Standard mode
    raw_date = row.get("collected_at", "")
    if not raw_date:
        return ""

    for fmt in config.get("date_formats", []):
        try:
            dt = datetime.strptime(raw_date, fmt)
            return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        except ValueError:
            continue

    # None of the formats matched — flag for quarantine
    raise TransformError(
        f"Cannot parse date '{raw_date}' with any configured format: "
        f"{config.get('date_formats')}"
    )


def _normalize_value(raw: str) -> str:
    """Normalize a lab result value.

    Non-numeric values (e.g., '<0.5', '>100', 'POSITIVE') are returned as-is.
    """
    try:
        num = float(raw)
        if num == int(num):
            return str(int(num))
        return str(num)
    except (ValueError, TypeError):
        return raw


_RANGE_RE = re.compile(r"([\d.]+)\s*[-–]\s*([\d.]+)")


def _check_value_anomaly(row: dict) -> None:
    """Warn if a numeric value falls far outside the reference range."""
    value_str = row.get("value", "")
    ref_range = row.get("reference_range", "")
    if not value_str or not ref_range:
        return

    try:
        value = float(value_str)
    except (ValueError, TypeError):
        return

    match = _RANGE_RE.search(ref_range)
    if not match:
        return

    try:
        low = float(match.group(1))
        high = float(match.group(2))
    except (ValueError, TypeError):
        return

    # Flag values that are >10× outside the reference range
    range_span = high - low
    if range_span <= 0:
        return

    if value < (low - 10 * range_span) or value > (high + 10 * range_span):
        row["_anomaly_warning"] = (
            f"Value {value} is extremely far outside reference range {ref_range}"
        )
        logger.warning(
            "ANOMALY: lab_name='%s' value=%s reference_range='%s' — "
            "value is >10× outside expected range",
            row.get("lab_name", ""), value, ref_range,
        )
