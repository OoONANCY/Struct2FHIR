"""Validate Config — validates config YAML structure and CSV compatibility."""

import argparse
import csv
import logging
import sys
from pathlib import Path

from config.schema import load_config, ConfigError

logger = logging.getLogger(__name__)


def validate(config_path: str, csv_path: str | None = None) -> bool:
    """Validate a config file and optionally check CSV compatibility.

    Args:
        config_path: Path to source config YAML.
        csv_path:    Optional path to CSV to check header compatibility.

    Returns:
        True if valid, False otherwise.
    """
    errors = []

    # 1. Validate YAML structure
    try:
        config = load_config(config_path)
        print(f"✅ Config structure valid: {config['source_name']}")
    except ConfigError as exc:
        print(f"❌ Config error: {exc}")
        return False

    # 2. Check CSV compatibility
    if csv_path:
        csv_file = Path(csv_path)
        if not csv_file.exists():
            print(f"❌ CSV file not found: {csv_path}")
            return False

        encoding = config.get("encoding", "utf-8")
        delimiter = config.get("delimiter", ",")
        skip_rows = config.get("skip_rows", 0)

        try:
            with open(csv_file, "r", encoding=encoding) as f:
                for _ in range(skip_rows):
                    next(f, None)
                reader = csv.DictReader(f, delimiter=delimiter)
                if reader.fieldnames is None:
                    print(f"❌ CSV is empty or has no header")
                    return False

                csv_headers = set(reader.fieldnames)
                column_map = config["column_map"]

                print(f"\n📋 CSV headers: {sorted(csv_headers)}")
                print(f"📋 Column map:")

                all_ok = True
                for internal, csv_col in column_map.items():
                    if csv_col in csv_headers:
                        print(f"   ✅ {internal} → '{csv_col}'")
                    else:
                        print(f"   ❌ {internal} → '{csv_col}' NOT FOUND")
                        all_ok = False

                # Check offset column
                offset_col = config.get("offset_column")
                if offset_col:
                    if offset_col in csv_headers:
                        print(f"   ✅ offset → '{offset_col}'")
                    else:
                        print(f"   ❌ offset → '{offset_col}' NOT FOUND")
                        all_ok = False

                # Preview first 3 rows
                print(f"\n📋 First 3 rows:")
                row_count = 0
                for row in reader:
                    if row_count >= 3:
                        break
                    mapped = {k: row.get(v, "N/A") for k, v in column_map.items()}
                    print(f"   {mapped}")
                    row_count += 1

                if all_ok:
                    print(f"\n✅ CSV is compatible with config\n")
                    return True
                else:
                    print(f"\n❌ CSV has missing columns\n")
                    return False

        except Exception as exc:
            print(f"❌ Error reading CSV: {exc}")
            return False

    return True


def main():
    parser = argparse.ArgumentParser(description="Validate config + CSV compatibility")
    parser.add_argument("--config", required=True, help="Path to source config YAML")
    parser.add_argument("--csv", default=None, help="Path to CSV to check compatibility")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    valid = validate(args.config, args.csv)
    sys.exit(0 if valid else 1)


if __name__ == "__main__":
    main()
