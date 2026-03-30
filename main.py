"""Synchronous pipeline entry point — CSV → FHIR Observations."""

import argparse
import json
import logging
import sys
import time

from config.schema import load_config
from engine.csv_reader import read_csv
from engine.transformer import transform_row, TransformError
from engine.fhir_assembler import assemble_observation
from engine.validator import validate_observation
from engine.http_sender import send_observation, create_session
from loinc.resolver import LoincResolver
from quarantine.store import QuarantineStore

logger = logging.getLogger(__name__)


def run_pipeline(config_path: str, input_path: str, *,
                 dry_run: bool = False, limit: int | None = None) -> dict:
    """Run the synchronous CSV → FHIR pipeline.

    Args:
        config_path: Path to source config YAML.
        input_path:  Path to input CSV file.
        dry_run:     If True, don't send to FHIR server.
        limit:       Max rows to process (None = all).

    Returns:
        Summary dict with processing stats.
    """
    config = load_config(config_path)
    resolver = LoincResolver()
    quarantine = QuarantineStore()
    session = create_session()

    stats = {
        "total": 0,
        "sent": 0,
        "quarantined": 0,
        "transform_errors": 0,
        "validation_errors": 0,
        "send_errors": 0,
    }

    start_time = time.time()

    for row in read_csv(input_path, config):
        stats["total"] += 1

        if limit and stats["total"] > limit:
            break

        # 1. Transform
        try:
            transformed = transform_row(row, config)
        except TransformError as exc:
            logger.warning("Row %d transform error: %s", stats["total"], exc)
            quarantine.add(
                row.get("lab_name", "unknown"),
                row,
                reason=f"transform_error: {exc}",
            )
            stats["transform_errors"] += 1
            continue

        # 2. Resolve LOINC
        lab_name = transformed.get("lab_name", "")
        result = resolver.resolve(lab_name)

        if result.quarantined:
            quarantine.add(lab_name, transformed, candidates=result.candidates)
            stats["quarantined"] += 1
            continue

        # 3. Assemble FHIR
        observation = assemble_observation(transformed, result.to_dict(), config)

        # 4. Validate
        errors = validate_observation(observation)
        if errors:
            logger.warning("Row %d validation errors: %s", stats["total"], errors)
            stats["validation_errors"] += 1
            continue

        # 5. Send (or dry-run)
        if dry_run:
            if stats["sent"] < 3:  # print first 3 in dry-run
                print(json.dumps(observation, indent=2))
                print("---")
            send_result = {"success": True}
        else:
            send_result = send_observation(observation, config, session=session)

        if send_result.get("success"):
            stats["sent"] += 1
        else:
            stats["send_errors"] += 1

        # Progress logging
        if stats["total"] % 1000 == 0:
            logger.info("Processed %d rows...", stats["total"])

    elapsed = time.time() - start_time
    stats["elapsed_seconds"] = round(elapsed, 2)
    stats["rows_per_second"] = round(stats["total"] / elapsed, 1) if elapsed > 0 else 0

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="FHIR Gateway — convert lab CSV to FHIR R4 Observations"
    )
    parser.add_argument("--config", required=True, help="Path to source config YAML")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print FHIR JSON but don't send to server")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max rows to process (default: all)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Log level (default: INFO)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n🚀 FHIR Gateway — {mode}\n")

    stats = run_pipeline(
        args.config, args.input,
        dry_run=args.dry_run, limit=args.limit,
    )

    print(f"\n{'=' * 50}")
    print(f"📊 Pipeline Summary")
    print(f"{'=' * 50}")
    print(f"   Total rows:        {stats['total']}")
    print(f"   Sent/printed:      {stats['sent']}")
    print(f"   Quarantined:       {stats['quarantined']}")
    print(f"   Transform errors:  {stats['transform_errors']}")
    print(f"   Validation errors: {stats['validation_errors']}")
    print(f"   Send errors:       {stats['send_errors']}")
    print(f"   Elapsed:           {stats['elapsed_seconds']}s")
    print(f"   Throughput:        {stats['rows_per_second']} rows/sec")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
