"""Async pipeline entry point — high-throughput CSV → FHIR Observations."""

import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

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
ASYNC_MAX_RETRIES = 3
ASYNC_RETRY_BACKOFF = 0.5  # seconds

HISTORY_PATH = Path(__file__).parent / "runs" / "history.jsonl"


async def send_observation_async(observation: dict, config: dict, *,
                                  session: aiohttp.ClientSession,
                                  dry_run: bool = False) -> dict:
    """Async POST of a FHIR Observation with retry on 5xx."""
    resource_id = observation.get("id", "unknown")
    base_url = config["fhir_server_url"].rstrip("/")
    url = f"{base_url}/Observation"

    if dry_run:
        return {"success": True, "resource_id": resource_id}

    headers = {"Content-Type": "application/fhir+json"}
    token = config.get("fhir_auth_token", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    last_exc = None
    for attempt in range(1, ASYNC_MAX_RETRIES + 1):
        try:
            async with session.post(url, json=observation, headers=headers,
                                    timeout=aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)) as resp:
                if resp.status in (200, 201):
                    logger.debug("Sent %s — HTTP %d", resource_id, resp.status)
                    return {"success": True, "resource_id": resource_id}

                # 4xx — don't retry, resource is wrong
                if 400 <= resp.status < 500:
                    body = await resp.text()
                    logger.error("Client error %s — HTTP %d: %s",
                                 resource_id, resp.status, body[:200])
                    return {"success": False, "resource_id": resource_id}

                # 5xx — retry with backoff
                body = await resp.text()
                wait = ASYNC_RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning("Server error %s — HTTP %d (attempt %d/%d), retrying in %.1fs",
                               resource_id, resp.status, attempt, ASYNC_MAX_RETRIES, wait)
                await asyncio.sleep(wait)

        except Exception as exc:
            last_exc = exc
            if attempt < ASYNC_MAX_RETRIES:
                wait = ASYNC_RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.warning("Async error %s (attempt %d/%d): %s — retrying in %.1fs",
                               resource_id, attempt, ASYNC_MAX_RETRIES, exc, wait)
                await asyncio.sleep(wait)
            else:
                logger.error("Async error %s after %d attempts: %s",
                             resource_id, ASYNC_MAX_RETRIES, exc)

    return {"success": False, "resource_id": resource_id}


async def process_row(row: dict, config: dict, resolver: LoincResolver,
                      quarantine: QuarantineStore, session: aiohttp.ClientSession,
                      semaphore: asyncio.Semaphore, stats: dict,
                      progress_counter: dict,
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
            _tick_progress(progress_counter, stats)
            return

        # Resolve LOINC (run in executor to avoid blocking event loop)
        lab_name = transformed.get("lab_name", "")
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, partial(resolver.resolve, lab_name))

        if result.quarantined:
            quarantine.add(lab_name, transformed,
                           candidates=result.candidates,
                           reason=_quarantine_reason(result, lab_name))
            stats["quarantined"] += 1
            _tick_progress(progress_counter, stats)
            return

        # Track resolution source
        if result.source in stats["resolution_sources"]:
            stats["resolution_sources"][result.source] += 1

        # Assemble
        observation = assemble_observation(transformed, result.to_dict(), config)

        # Validate
        errors = validate_observation(observation)
        if errors:
            stats["validation_errors"] += 1
            _tick_progress(progress_counter, stats)
            return

        # Send
        send_result = await send_observation_async(
            observation, config, session=session, dry_run=dry_run
        )
        if send_result["success"]:
            stats["sent"] += 1
        else:
            stats["send_errors"] += 1

        _tick_progress(progress_counter, stats)


def _quarantine_reason(result, lab_name: str) -> str:
    """Build a specific quarantine reason string."""
    if not lab_name.strip():
        return "empty_lab_name"
    return "no_confident_match"


def _tick_progress(counter: dict, stats: dict) -> None:
    """Log progress every 500 completed rows."""
    counter["done"] += 1
    if counter["done"] % 500 == 0:
        elapsed = time.time() - counter["start"]
        rate = counter["done"] / elapsed if elapsed > 0 else 0
        logger.info(
            "Progress: %d/%d rows (%.1f rows/sec) — sent=%d quarantined=%d errors=%d",
            counter["done"], stats["total"],
            rate, stats["sent"], stats["quarantined"],
            stats["transform_errors"] + stats["validation_errors"] + stats["send_errors"],
        )


def _save_run_history(stats: dict, config_path: str, input_path: str, dry_run: bool) -> None:
    """Append run stats to a persistent JSONL file."""
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": config_path,
        "input": input_path,
        "dry_run": dry_run,
        **stats,
    }
    try:
        with open(HISTORY_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, default=str) + "\n")
        logger.info("Run history saved to %s", HISTORY_PATH)
    except OSError as exc:
        logger.warning("Could not save run history: %s", exc)


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
        "resolution_sources": {"cache": 0, "fuzzy": 0, "api": 0},
    }

    start_time = time.time()

    rows = list(read_csv(input_path, config))
    if limit:
        rows = rows[:limit]
    stats["total"] = len(rows)

    progress_counter = {"done": 0, "start": start_time}

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_row(row, config, resolver, quarantine, session,
                        semaphore, stats, progress_counter, dry_run=dry_run)
            for row in rows
        ]
        await asyncio.gather(*tasks)

    elapsed = time.time() - start_time
    stats["elapsed_seconds"] = round(elapsed, 2)
    stats["rows_per_second"] = round(stats["total"] / elapsed, 1) if elapsed > 0 else 0

    _save_run_history(stats, config_path, input_path, dry_run)

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

    src = stats["resolution_sources"]
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
    print(f"   LOINC sources:     cache={src['cache']}  fuzzy={src['fuzzy']}  api={src['api']}")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
