"""Async pipeline entry point — high-throughput CSV → FHIR Observations."""

import argparse
import asyncio
import json
import logging
import time

import aiohttp

from config.schema import load_config
from engine.csv_reader import read_csv
from engine.transformer import transform_row, TransformError
from engine.fhir_assembler import assemble_observation
from engine.validator import validate_observation
from loinc.resolver import LoincResolver
from quarantine.store import QuarantineStore

logger = logging.getLogger(__name__)

DEFAULT_WORKERS = 10
DEFAULT_TIMEOUT = 30


async def send_observation_async(observation: dict, config: dict, *,
                                 session: aiohttp.ClientSession,
                                 dry_run: bool = False) -> dict:
    """Async POST of a FHIR Observation."""
    resource_id = observation.get("id", "unknown")
    base_url = config["fhir_server_url"].rstrip("/")
    url = f"{base_url}/Observation"

    if dry_run:
        return {"success": True, "resource_id": resource_id}

    headers = {"Content-Type": "application/fhir+json"}
    token = config.get("fhir_auth_token", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with session.post(url, json=observation, headers=headers,
                                timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)) as resp:
            if resp.status in (200, 201):
                logger.debug("Sent %s — HTTP %d", resource_id, resp.status)
                return {"success": True, "resource_id": resource_id}
            else:
                body = await resp.text()
                logger.error("Failed %s — HTTP %d: %s", resource_id, resp.status, body[:200])
                return {"success": False, "resource_id": resource_id}
    except Exception as exc:
        logger.error("Async error %s: %s", resource_id, exc)
        return {"success": False, "resource_id": resource_id}


async def process_row(row: dict, config: dict, resolver: LoincResolver,
                      quarantine: QuarantineStore, session: aiohttp.ClientSession,
                      semaphore: asyncio.Semaphore, stats: dict,
                      dry_run: bool = False) -> None:
    """Process a single row through the pipeline."""
    async with semaphore:
        # Transform
        try:
            transformed = transform_row(row, config)
        except TransformError as exc:
            quarantine.add(row.get("lab_name", "unknown"), row,
                           reason=f"transform_error: {exc}")
            stats["transform_errors"] += 1
            return

        # Resolve LOINC (sync — cached lookups are fast)
        lab_name = transformed.get("lab_name", "")
        result = resolver.resolve(lab_name)

        if result.quarantined:
            quarantine.add(lab_name, transformed, candidates=result.candidates)
            stats["quarantined"] += 1
            return

        # Assemble
        observation = assemble_observation(transformed, result.to_dict(), config)

        # Validate
        errors = validate_observation(observation)
        if errors:
            stats["validation_errors"] += 1
            return

        # Send
        send_result = await send_observation_async(
            observation, config, session=session, dry_run=dry_run
        )
        if send_result["success"]:
            stats["sent"] += 1
        else:
            stats["send_errors"] += 1


async def run_pipeline_async(config_path: str, input_path: str, *,
                              workers: int = DEFAULT_WORKERS,
                              dry_run: bool = False,
                              limit: int | None = None) -> dict:
    """Run the async pipeline with concurrent workers."""
    config = load_config(config_path)
    resolver = LoincResolver()
    quarantine = QuarantineStore()
    semaphore = asyncio.Semaphore(workers)

    stats = {
        "total": 0, "sent": 0, "quarantined": 0,
        "transform_errors": 0, "validation_errors": 0, "send_errors": 0,
    }

    start_time = time.time()

    rows = list(read_csv(input_path, config))
    if limit:
        rows = rows[:limit]
    stats["total"] = len(rows)

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_row(row, config, resolver, quarantine, session,
                        semaphore, stats, dry_run=dry_run)
            for row in rows
        ]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start_time
    stats["elapsed_seconds"] = round(elapsed, 2)
    stats["rows_per_second"] = round(stats["total"] / elapsed, 1) if elapsed > 0 else 0

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="FHIR Gateway (async) — high-throughput CSV to FHIR"
    )
    parser.add_argument("--config", required=True, help="Path to source config YAML")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--dry-run", action="store_true", help="Don't send to server")
    parser.add_argument("--limit", type=int, default=None, help="Max rows to process")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"\n🚀 FHIR Gateway (async, {args.workers} workers) — {mode}\n")

    stats = asyncio.run(run_pipeline_async(
        args.config, args.input,
        workers=args.workers, dry_run=args.dry_run, limit=args.limit,
    ))

    print(f"\n{'=' * 50}")
    print(f"📊 Async Pipeline Summary")
    print(f"{'=' * 50}")
    print(f"   Total rows:        {stats['total']}")
    print(f"   Sent:              {stats['sent']}")
    print(f"   Quarantined:       {stats['quarantined']}")
    print(f"   Transform errors:  {stats['transform_errors']}")
    print(f"   Validation errors: {stats['validation_errors']}")
    print(f"   Send errors:       {stats['send_errors']}")
    print(f"   Elapsed:           {stats['elapsed_seconds']}s")
    print(f"   Throughput:        {stats['rows_per_second']} rows/sec")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
