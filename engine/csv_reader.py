"""CSV Reader — reads any CSV and normalizes column names via config column_map."""

import csv
import io
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_EXTRA_PREFIX = "_extra_"


def read_csv(file_path: str, config: dict):
    """Yield normalized row dicts from a CSV file.

    Each yielded dict uses the internal standard keys (patient_id, lab_name,
    value, unit, collected_at, reference_range) regardless of the original
    CSV headers.  Unmapped columns are preserved with the ``_extra_`` prefix.

    Duplicate rows (same patient_id + lab_name + collected_at/offset) are
    detected and skipped with a warning.

    Args:
        file_path: Path to the CSV file.
        config:    Validated config dict.

    Yields:
        dict with standardized keys.
    """
    column_map = config["column_map"]  # internal_name -> csv_header
    delimiter = config.get("delimiter", ",")
    encoding = config.get("encoding", "utf-8")
    skip_rows = config.get("skip_rows", 0)
    dedup_enabled = config.get("dedup", True)

    # Build reverse map: csv_header -> internal_name
    reverse_map = {v: k for k, v in column_map.items()}

    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    seen_keys: set = set()
    dup_count = 0

    with open(path, "r", encoding=encoding, newline="") as f:
        # Skip leading rows (e.g. metadata lines before the header)
        for _ in range(skip_rows):
            next(f, None)

        reader = csv.DictReader(f, delimiter=delimiter)

        # Validate that required CSV headers are present
        if reader.fieldnames is None:
            raise ValueError(f"CSV file is empty or has no header: {file_path}")

        csv_headers = set(reader.fieldnames)
        for internal_name, csv_header in column_map.items():
            if csv_header not in csv_headers:
                raise ValueError(
                    f"Column '{csv_header}' (mapped to '{internal_name}') "
                    f"not found in CSV. Available: {sorted(csv_headers)}"
                )

        row_count = 0
        for row in reader:
            normalized = {}
            for csv_header, value in row.items():
                internal = reverse_map.get(csv_header)
                if internal:
                    normalized[internal] = value.strip() if value else ""
                else:
                    # Prefix unmapped columns to avoid key collisions
                    normalized[f"{_EXTRA_PREFIX}{csv_header}"] = value.strip() if value else ""

            # Include offset column if configured
            offset_col = config.get("offset_column")
            if offset_col and offset_col in row:
                normalized["_offset_minutes"] = row[offset_col].strip() if row[offset_col] else ""

            # Duplicate detection
            if dedup_enabled:
                dedup_key = (
                    normalized.get("patient_id", ""),
                    normalized.get("lab_name", ""),
                    normalized.get("collected_at", normalized.get("_offset_minutes", "")),
                    normalized.get("value", ""),
                )
                if dedup_key in seen_keys:
                    dup_count += 1
                    logger.debug("Skipping duplicate row: %s", dedup_key)
                    continue
                seen_keys.add(dedup_key)

            row_count += 1
            yield normalized

    if dup_count > 0:
        logger.warning("Skipped %d duplicate rows", dup_count)
    logger.info("Read %d rows from %s", row_count, file_path)
