"""LOINC Resolver — orchestrates cache → fuzzy → API → quarantine."""

import logging

from loinc.dictionary import LoincDictionary
from loinc.fuzzy_matcher import FuzzyMatcher
from loinc import api_client

logger = logging.getLogger(__name__)

# Confidence thresholds
FUZZY_ACCEPT_THRESHOLD = 95.0   # Accept fuzzy match without API
FUZZY_API_THRESHOLD = 60.0      # Call API if fuzzy score in this range
API_ACCEPT_THRESHOLD = 0.80     # Accept API result above this confidence


class ResolveResult:
    """Result of a LOINC resolution attempt."""

    def __init__(self, loinc_code: str = "", display: str = "",
                 source: str = "", confidence: float = 0.0,
                 resolved: bool = False, quarantined: bool = False,
                 candidates: list | None = None):
        self.loinc = loinc_code
        self.display = display
        self.source = source
        self.confidence = confidence
        self.resolved = resolved
        self.quarantined = quarantined
        self.candidates = candidates or []

    def to_dict(self) -> dict:
        return {
            "loinc": self.loinc,
            "display": self.display,
            "source": self.source,
            "confidence": self.confidence,
            "resolved": self.resolved,
            "quarantined": self.quarantined,
        }


class LoincResolver:
    """Resolves lab names to LOINC codes using a three-tier strategy."""

    def __init__(self, dictionary: LoincDictionary | None = None,
                 fuzzy_matcher: FuzzyMatcher | None = None):
        self.dictionary = dictionary or LoincDictionary()
        self.fuzzy = fuzzy_matcher or FuzzyMatcher()

    def resolve(self, lab_name: str) -> ResolveResult:
        """Resolve a lab name to a LOINC code.

        Resolution order:
        1. Local dictionary cache (instant)
        2. Fuzzy matching against LOINC corpus
        3. NLM API fallback
        4. Quarantine if all fail

        Args:
            lab_name: The lab test name to resolve.

        Returns:
            ResolveResult with the resolution outcome.
        """
        clean_name = lab_name.strip()
        if not clean_name:
            return ResolveResult(
                quarantined=True,
                source="quarantine",
                candidates=[],
            )

        # --- Tier 1: Cache ---
        cached = self.dictionary.lookup(clean_name)
        if cached:
            logger.debug("Cache hit for '%s' → %s", clean_name, cached["loinc"])
            return ResolveResult(
                loinc_code=cached["loinc"],
                display=cached["display"],
                source="cache",
                confidence=cached["provenance"].get("confidence", 1.0),
                resolved=True,
            )

        # --- Tier 2: Fuzzy matching ---
        fuzzy_results = self.fuzzy.match(clean_name, top_n=5, score_cutoff=FUZZY_API_THRESHOLD)
        if fuzzy_results:
            best = fuzzy_results[0]
            if best["score"] >= FUZZY_ACCEPT_THRESHOLD:
                # High confidence fuzzy match — accept immediately
                self.dictionary.add(
                    clean_name,
                    best["loinc_code"],
                    best["display_name"],
                    source="fuzzy",
                    confidence=best["score"] / 100.0,
                    raw_name=lab_name,
                )
                self.dictionary.save()
                logger.info(
                    "Fuzzy match accepted for '%s' → %s (%.1f%%)",
                    clean_name, best["loinc_code"], best["score"],
                )
                return ResolveResult(
                    loinc_code=best["loinc_code"],
                    display=best["display_name"],
                    source="fuzzy",
                    confidence=best["score"] / 100.0,
                    resolved=True,
                    candidates=fuzzy_results,
                )

            # Score between 60–94%: fall through to API with fuzzy candidates
            logger.debug(
                "Fuzzy match for '%s' below threshold (%.1f%%) — trying API",
                clean_name, best["score"],
            )

        # --- Tier 3: NLM API ---
        api_results = api_client.search_loinc(clean_name)

        # Handle API unreachable (empty result with no candidates)
        if api_results is None:
            reason = f"api_unreachable: NLM API call failed for '{clean_name}'"
            logger.info("Quarantined '%s' — %s", clean_name, reason)
            return ResolveResult(
                quarantined=True,
                source="quarantine",
                candidates=fuzzy_results,
            )

        if api_results:
            best_api = api_results[0]
            if best_api["confidence"] >= API_ACCEPT_THRESHOLD:
                self.dictionary.add(
                    clean_name,
                    best_api["loinc_code"],
                    best_api["display_name"],
                    source="api",
                    confidence=best_api["confidence"],
                    raw_name=lab_name,
                )
                self.dictionary.save()
                logger.info(
                    "API match accepted for '%s' → %s (conf=%.2f)",
                    clean_name, best_api["loinc_code"], best_api["confidence"],
                )
                return ResolveResult(
                    loinc_code=best_api["loinc_code"],
                    display=best_api["display_name"],
                    source="api",
                    confidence=best_api["confidence"],
                    resolved=True,
                    candidates=fuzzy_results,
                )
            else:
                # API returned results but below threshold
                reason = (
                    f"api_confidence_below_threshold "
                    f"(best={best_api['confidence']:.2f}, "
                    f"threshold={API_ACCEPT_THRESHOLD})"
                )
                logger.info("Quarantined '%s' — %s", clean_name, reason)
                return ResolveResult(
                    quarantined=True,
                    source="quarantine",
                    candidates=fuzzy_results + api_results,
                )

        # --- Tier 4: Quarantine — no candidates at all ---
        no_fuzzy = not fuzzy_results
        reason_parts = []
        if no_fuzzy:
            reason_parts.append("no_fuzzy_candidates")
        reason_parts.append("no_api_results")
        reason = "; ".join(reason_parts)

        logger.info("Quarantined '%s' — %s", clean_name, reason)
        return ResolveResult(
            quarantined=True,
            source="quarantine",
            candidates=fuzzy_results,
        )
