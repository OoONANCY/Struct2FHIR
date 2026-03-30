"""Quarantine Store — persistent quarantine with state machine and thread safety."""

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).parent / "data" / "quarantine.json"

VALID_STATES = {
    "pending_review", "in_review", "resolved", "sent",
    "unmappable", "escalated", "reprocess_failed",
}

# Allowed state transitions
_TRANSITIONS = {
    "pending_review": {"in_review", "resolved", "unmappable", "escalated"},
    "in_review": {"resolved", "unmappable", "pending_review", "escalated"},
    "resolved": {"sent", "reprocess_failed"},
    "sent": set(),
    "unmappable": {"pending_review"},  # allow re-review
    "escalated": {"resolved", "unmappable", "pending_review"},
    "reprocess_failed": {"resolved", "pending_review"},
}


class QuarantineStore:
    """Thread-safe manager for quarantined lab records."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_PATH
        self._records: dict = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    self._records = json.load(f)
                logger.info("Loaded %d quarantine records", len(self._records))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load quarantine %s: %s", self._path, exc)
                self._records = {}
        else:
            self._records = {}

    def _save_locked(self) -> None:
        """Persist to disk — must be called inside self._lock."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._records, f, indent=2, ensure_ascii=False)

    def save(self) -> None:
        """Thread-safe persist."""
        with self._lock:
            self._save_locked()

    def add(self, lab_name: str, row: dict, candidates: list | None = None,
            reason: str = "low_confidence") -> str:
        """Add a record to quarantine. Returns the quarantine ID."""
        with self._lock:
            now = datetime.now(timezone.utc)
            qid = f"q_{now.strftime('%Y%m%d')}_{len(self._records) + 1:04d}"

            self._records[qid] = {
                "id": qid,
                "lab_name": lab_name,
                "row_data": row,
                "status": "pending_review",
                "reason": reason,
                "candidates": candidates or [],
                "resolved_loinc": None,
                "resolved_display": None,
                "reviewed_by": None,
                "failure_reason": None,
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
            self._save_locked()
        logger.info("Quarantined '%s' as %s (reason: %s)", lab_name, qid, reason)
        return qid

    def update_status(self, qid: str, new_status: str, **kwargs) -> None:
        """Transition a record to a new status.

        Raises ValueError if the transition is invalid.
        """
        with self._lock:
            record = self._records.get(qid)
            if not record:
                raise ValueError(f"Quarantine record {qid} not found")

            current = record["status"]
            if new_status not in _TRANSITIONS.get(current, set()):
                raise ValueError(
                    f"Cannot transition {qid} from '{current}' to '{new_status}'. "
                    f"Allowed: {_TRANSITIONS.get(current, set())}"
                )

            record["status"] = new_status
            record["updated_at"] = datetime.now(timezone.utc).isoformat()

            for key, val in kwargs.items():
                if key in record:
                    record[key] = val

            self._save_locked()
        logger.info("Updated %s: %s → %s", qid, current, new_status)

    def get_pending(self) -> list[dict]:
        """Return all records in pending_review status."""
        with self._lock:
            return [r for r in self._records.values() if r["status"] == "pending_review"]

    def get_resolved(self) -> list[dict]:
        """Return all records in resolved status (ready to reprocess)."""
        with self._lock:
            return [r for r in self._records.values() if r["status"] == "resolved"]

    def get_record(self, qid: str) -> dict | None:
        with self._lock:
            return self._records.get(qid)

    def get_all(self) -> dict:
        with self._lock:
            return dict(self._records)

    def stats(self) -> dict:
        """Return counts per status and total."""
        with self._lock:
            counts = {}
            for r in self._records.values():
                s = r["status"]
                counts[s] = counts.get(s, 0) + 1
            counts["total"] = len(self._records)
            return counts

    def __len__(self) -> int:
        with self._lock:
            return len(self._records)
