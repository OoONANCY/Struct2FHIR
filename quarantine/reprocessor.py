"""Quarantine Reprocessor — sends resolved quarantine records as FHIR."""

import argparse
import logging
import sys

from config.schema import load_config
from engine.fhir_assembler import assemble_observation
from engine.validator import validate_observation
from engine.http_sender import send_observation, create_session
from quarantine.store import QuarantineStore

logger = logging.getLogger(__name__)


def reprocess(config_path: str, record_id: str | None = None,
              dry_run: bool = False) -> dict:
    """Re-process resolved quarantine records as FHIR Observations.

    Args:
        config_path: Path to source config YAML.
        record_id:   Optional specific quarantine ID. If None, process all resolved.
        dry_run:     If True, validate but don't send.

    Returns:
        Summary dict with counts of sent, failed, skipped.
    """
    config = load_config(config_path)
    store = QuarantineStore()
    session = create_session()

    if record_id:
        record = store.get_record(record_id)
        if not record:
            logger.error("Record %s not found", record_id)
            return {"sent": 0, "failed": 0, "skipped": 1}
        if record["status"] != "resolved":
            logger.error("Record %s is not in 'resolved' state (current: %s)", record_id, record["status"])
            return {"sent": 0, "failed": 0, "skipped": 1}
        records = [record]
    else:
        records = store.get_resolved()

    if not records:
        logger.info("No resolved records to reprocess.")
        return {"sent": 0, "failed": 0, "skipped": 0}

    stats = {"sent": 0, "failed": 0, "skipped": 0}

    for record in records:
        qid = record["id"]
        loinc_result = {
            "loinc": record.get("resolved_loinc", ""),
            "display": record.get("resolved_display", ""),
        }

        row = record.get("row_data", {})

        try:
            observation = assemble_observation(row, loinc_result, config)
            errors = validate_observation(observation)

            if errors:
                logger.warning("Validation errors for %s: %s", qid, errors)
                store.update_status(
                    qid, "reprocess_failed",
                    failure_reason=f"Validation: {'; '.join(errors)}",
                )
                stats["failed"] += 1
                continue

            result = send_observation(observation, config, dry_run=dry_run, session=session)

            if result["success"]:
                store.update_status(qid, "sent")
                stats["sent"] += 1
                logger.info("Reprocessed %s successfully", qid)
            else:
                store.update_status(
                    qid, "reprocess_failed",
                    failure_reason=f"HTTP {result.get('status_code')}: {result.get('response', '')[:200]}",
                )
                stats["failed"] += 1

        except Exception as exc:
            logger.error("Reprocess error for %s: %s", qid, exc)
            try:
                store.update_status(qid, "reprocess_failed", failure_reason=str(exc))
            except ValueError:
                pass
            stats["failed"] += 1

    logger.info("Reprocess complete: %s", stats)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Reprocess resolved quarantine records")
    parser.add_argument("--config", required=True, help="Path to source config YAML")
    parser.add_argument("--id", default=None, help="Specific quarantine record ID")
    parser.add_argument("--dry-run", action="store_true", help="Validate but don't send")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    stats = reprocess(args.config, record_id=args.id, dry_run=args.dry_run)

    print(f"\n📊 Reprocess results: {stats['sent']} sent, {stats['failed']} failed, {stats['skipped']} skipped\n")


if __name__ == "__main__":
    main()
