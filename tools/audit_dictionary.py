"""Audit Dictionary — surfaces risky, unverified, or stale LOINC dictionary entries."""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from loinc.dictionary import LoincDictionary

logger = logging.getLogger(__name__)


def audit(low_confidence: float | None = None,
          high_risk: bool = False,
          min_uses: int = 50,
          stale_days: int | None = None,
          export_path: str | None = None) -> dict:
    """Audit the LOINC dictionary for risky entries.

    Returns:
        Summary dict with counts per category.
    """
    dictionary = LoincDictionary()
    entries = dictionary.get_all()
    now = datetime.now(timezone.utc)

    issues = {
        "low_confidence": [],
        "high_risk": [],
        "stale": [],
        "unverified": [],
    }

    for name, entry in entries.items():
        prov = entry.get("provenance", {})
        conf = prov.get("confidence", 1.0)
        verified = prov.get("verified", False)
        times_used = prov.get("times_used", 0)
        last_used = prov.get("last_used")

        # Low confidence
        if low_confidence is not None and conf < low_confidence:
            issues["low_confidence"].append({
                "name": name, "loinc": entry["loinc"],
                "confidence": conf, "verified": verified,
            })

        # High risk: unverified + heavily used
        if high_risk and not verified and times_used >= min_uses:
            issues["high_risk"].append({
                "name": name, "loinc": entry["loinc"],
                "confidence": conf, "times_used": times_used,
            })

        # Stale
        if stale_days is not None and last_used:
            try:
                last_dt = datetime.fromisoformat(last_used)
                if (now - last_dt) > timedelta(days=stale_days):
                    issues["stale"].append({
                        "name": name, "loinc": entry["loinc"],
                        "last_used": last_used,
                    })
            except (ValueError, TypeError):
                pass

        # Unverified
        if not verified:
            issues["unverified"].append({
                "name": name, "loinc": entry["loinc"],
                "confidence": conf, "source": prov.get("source", "unknown"),
            })

    # Print summary
    total = len(entries)
    print(f"\n📊 LOINC Dictionary Audit — {total} entries\n")
    print(f"   Unverified:      {len(issues['unverified'])}")

    if low_confidence is not None:
        print(f"   Low confidence (<{low_confidence}): {len(issues['low_confidence'])}")
        for e in issues["low_confidence"][:10]:
            print(f"     ⚠️  {e['name']} → {e['loinc']} (conf={e['confidence']:.2f})")

    if high_risk:
        print(f"   High risk (unverified, ≥{min_uses} uses): {len(issues['high_risk'])}")
        for e in issues["high_risk"][:10]:
            print(f"     🔴 {e['name']} → {e['loinc']} (used {e['times_used']}x)")

    if stale_days is not None:
        print(f"   Stale (>{stale_days} days): {len(issues['stale'])}")
        for e in issues["stale"][:10]:
            print(f"     💤 {e['name']} → {e['loinc']} (last: {e['last_used']})")

    # Export
    if export_path:
        report = {"summary": {k: len(v) for k, v in issues.items()}, "details": issues}
        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\n   📁 Exported to {export_path}")

    print()
    return {k: len(v) for k, v in issues.items()}


def main():
    parser = argparse.ArgumentParser(description="Audit the LOINC dictionary")
    parser.add_argument("--low-confidence", type=float, default=None,
                        help="Show entries below this confidence threshold")
    parser.add_argument("--high-risk", action="store_true",
                        help="Show unverified entries with high usage")
    parser.add_argument("--min-uses", type=int, default=50,
                        help="Min uses for high-risk filter (default: 50)")
    parser.add_argument("--stale", type=int, default=None,
                        help="Show entries not used in N days")
    parser.add_argument("--export", default=None, help="Export report to JSON file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    # Default: show overview + auto-show risky entries
    if not any([args.low_confidence, args.high_risk, args.stale]):
        audit(low_confidence=0.85, high_risk=True, min_uses=args.min_uses, export_path=args.export)
    else:
        audit(
            low_confidence=args.low_confidence,
            high_risk=args.high_risk,
            min_uses=args.min_uses,
            stale_days=args.stale,
            export_path=args.export,
        )


if __name__ == "__main__":
    main()
