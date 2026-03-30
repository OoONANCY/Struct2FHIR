"""Fuzzy Matcher — RapidFuzz wrapper for matching lab names to LOINC corpus."""

import json
import logging
from pathlib import Path

from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

_DEFAULT_CORPUS = Path(__file__).parent / "data" / "loinc_corpus.json"


class FuzzyMatcher:
    """Matches lab names against a LOINC corpus using rapidfuzz WRatio."""

    def __init__(self, corpus_path: str | Path | None = None):
        self._corpus_path = Path(corpus_path) if corpus_path else _DEFAULT_CORPUS
        self._corpus: list[dict] = []
        self._names: list[str] = []
        self._code_map: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not self._corpus_path.exists():
            logger.warning(
                "LOINC corpus not found at %s — fuzzy matching disabled. "
                "Run tools/build_corpus.py to generate it.",
                self._corpus_path,
            )
            return

        with open(self._corpus_path, "r", encoding="utf-8") as f:
            self._corpus = json.load(f)

        # Build lookup structures
        self._names = [entry["display_name"] for entry in self._corpus]
        self._code_map = {
            entry["display_name"]: entry for entry in self._corpus
        }
        logger.info("Loaded LOINC corpus with %d terms", len(self._names))

    @property
    def is_loaded(self) -> bool:
        return len(self._names) > 0

    def match(self, lab_name: str, top_n: int = 5, score_cutoff: float = 60.0) -> list[dict]:
        """Find the best fuzzy matches for a lab name.

        Args:
            lab_name:     The lab name to match.
            top_n:        Number of top results to return.
            score_cutoff: Minimum score (0–100) to include.

        Returns:
            List of dicts with keys: display_name, loinc_code, score.
            Sorted by score descending. Deduplicated by LOINC code.
        """
        if not self._names:
            return []

        results = process.extract(
            lab_name,
            self._names,
            scorer=fuzz.WRatio,
            limit=top_n * 2,  # fetch extra for dedup
            score_cutoff=score_cutoff,
        )

        # Deduplicate by LOINC code, keep highest score
        seen_codes = set()
        deduped = []
        for match_name, score, _index in results:
            entry = self._code_map.get(match_name, {})
            code = entry.get("loinc_code", "")
            if code and code not in seen_codes:
                seen_codes.add(code)
                deduped.append({
                    "display_name": match_name,
                    "loinc_code": code,
                    "score": round(score, 2),
                })
            if len(deduped) >= top_n:
                break

        return deduped
