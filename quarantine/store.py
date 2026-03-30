"""Quarantine Store — persistent quarantine with state machine."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).parent / "data" / "quarantine.json"

VALID_STATES = {
    "pending_review", "in_review", "resolved", "sent",
    "unmappable", "reprocess_failed",
}

# Allowed state transitions
_TRANSITIONS = {
    "pending_review": {"in_review", "resolved", "unmappable"},
    "in_review": {"resolved", "unmappable", "pending_review"},
    "resolved": {"sent", "reprocess_failed"},
    "sent": set(),
    "unmappable": {"pending_review"},  # allow re-review
    "reprocess_failed": {"resolved", "pending_review"},
}


class QuarantineStore:
    """Manages quarantined lab records that couldn't be auto-resolved."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_PATH
        self._records: dict = {}
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

    def save(self) -> None:
        """Persist quarantine records to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._records, f, indent=2, ensure_ascii=False)

    def add(self, lab_name: str, row: dict, candidates: list | None = None,
            reason: str = "low_confidence") -> str:
        """Add a record to quarantine. Returns the quarantine ID."""
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
        self.save()
        logger.info("Quarantined '%s' as %s", lab_name, qid)
        return qid

    def update_status(self, qid: str, new_status: str, **kwargs) -> None:
        """Transition a record to a new status.

        Raises ValueError if the transition is invalid.
        """
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

        self.save()
        logger.info("Updated %s: %s → %s", qid, current, new_status)

    def get_pending(self) -> list[dict]:
        """Return all records in pending_review status."""
        return [r for r in self._records.values() if r["status"] == "pending_review"]

    def get_resolved(self) -> list[dict]:
        """Return all records in resolved status (ready to reprocess)."""
        return [r for r in self._records.values() if r["status"] == "resolved"]

    def get_record(self, qid: str) -> dict | None:
        return self._records.get(qid)

    def get_all(self) -> dict:
        return dict(self._records)

    def __len__(self) -> int:
        return len(self._records)
