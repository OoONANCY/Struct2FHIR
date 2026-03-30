"""LOINC Dictionary — local JSON cache with provenance tracking."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).parent / "data" / "loinc_dict.json"


class LoincDictionary:
    """Thread-safe JSON dictionary for caching LOINC mappings."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_PATH
        self._data: dict = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
                logger.info("Loaded %d entries from LOINC dictionary", len(self._data))
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not load dictionary %s: %s", self._path, exc)
                self._data = {}
        else:
            self._data = {}
            logger.info("LOINC dictionary not found at %s — starting empty", self._path)

    def save(self) -> None:
        """Persist the dictionary to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)
        logger.debug("Saved %d entries to %s", len(self._data), self._path)

    def lookup(self, lab_name: str) -> dict | None:
        """Look up a lab name in the cache.

        Returns the entry dict (with loinc, display, provenance) or None.
        Updates usage stats on hit.
        """
        key = lab_name.lower().strip()
        entry = self._data.get(key)
        if entry:
            entry["provenance"]["times_used"] = entry["provenance"].get("times_used", 0) + 1
            entry["provenance"]["last_used"] = datetime.now(timezone.utc).isoformat()
            return entry
        return None

    def add(self, lab_name: str, loinc_code: str, display: str,
            source: str, confidence: float, *,
            verified: bool = False, verified_by: str | None = None,
            raw_name: str | None = None) -> None:
        """Add or update an entry with full provenance."""
        key = lab_name.lower().strip()
        now = datetime.now(timezone.utc).isoformat()

        self._data[key] = {
            "loinc": loinc_code,
            "display": display,
            "provenance": {
                "source": source,
                "confidence": confidence,
                "verified": verified,
                "verified_by": verified_by,
                "verified_at": now if verified else None,
                "created_at": now,
                "times_used": 0,
                "last_used": now,
                "first_seen_raw": raw_name or lab_name,
            },
        }
        logger.debug("Added '%s' → %s (source=%s, confidence=%.2f)", key, loinc_code, source, confidence)

    def get_all(self) -> dict:
        """Return all entries (for audit tools)."""
        return dict(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, lab_name: str) -> bool:
        return lab_name.lower().strip() in self._data
