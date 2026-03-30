"""Quarantine Reviewer — interactive CLI tool for reviewing quarantined records."""

import sys
import logging

from loinc.dictionary import LoincDictionary
from loinc.fuzzy_matcher import FuzzyMatcher
from loinc import api_client
from quarantine.store import QuarantineStore

logger = logging.getLogger(__name__)


def review_pending(store: QuarantineStore | None = None,
                   dictionary: LoincDictionary | None = None,
                   fuzzy: FuzzyMatcher | None = None) -> None:
    """Interactive CLI review of pending quarantine records."""
    store = store or QuarantineStore()
    dictionary = dictionary or LoincDictionary()
    fuzzy = fuzzy or FuzzyMatcher()

    pending = store.get_pending()
    if not pending:
        print("\n✅ No pending quarantine records to review.\n")
        return

    print(f"\n📋 {len(pending)} record(s) pending review\n")
    print("=" * 60)

    for record in pending:
        qid = record["id"]
        lab_name = record["lab_name"]

        store.update_status(qid, "in_review")

        print(f"\n🔍 Record: {qid}")
        print(f"   Lab name: {lab_name}")

        # Show existing candidates
        candidates = record.get("candidates", [])
        if not candidates:
            # Try fresh fuzzy match
            candidates = fuzzy.match(lab_name)

        if candidates:
            print("\n   Top candidates:")
            for i, c in enumerate(candidates, 1):
                name = c.get("display_name", c.get("display", ""))
                code = c.get("loinc_code", c.get("loinc", ""))
                score = c.get("score", c.get("confidence", 0))
                print(f"   {i}. {name} [{code}] — {score:.1f}%")

        print("\n   Actions:")
        print("   [1-N]  Accept candidate N")
        print("   [s]    Search with a different term")
        print("   [m]    Manually enter LOINC code")
        print("   [u]    Mark as unmappable")
        print("   [q]    Quit review")

        choice = input("\n   Choice: ").strip().lower()

        if choice == "q":
            store.update_status(qid, "pending_review")
            print("\n⏸  Review paused.")
            break

        elif choice == "u":
            store.update_status(qid, "unmappable")
            print(f"   ❌ Marked {qid} as unmappable")

        elif choice == "s":
            term = input("   Search term: ").strip()
            new_results = fuzzy.match(term)
            api_results = api_client.search_loinc(term)
            all_results = new_results + api_results
            if all_results:
                print("\n   Search results:")
                for i, r in enumerate(all_results, 1):
                    name = r.get("display_name", "")
                    code = r.get("loinc_code", "")
                    score = r.get("score", r.get("confidence", 0))
                    if isinstance(score, float) and score < 1.0:
                        score *= 100
                    print(f"   {i}. {name} [{code}] — {score:.1f}%")
                sub = input("\n   Accept which # (or 'n' to skip): ").strip()
                if sub.isdigit() and 1 <= int(sub) <= len(all_results):
                    hit = all_results[int(sub) - 1]
                    _accept_match(store, dictionary, qid, lab_name, hit)
                else:
                    store.update_status(qid, "pending_review")
            else:
                print("   No results found.")
                store.update_status(qid, "pending_review")

        elif choice == "m":
            code = input("   LOINC code: ").strip()
            display = input("   Display name: ").strip()
            if code:
                store.update_status(
                    qid, "resolved",
                    resolved_loinc=code,
                    resolved_display=display,
                    reviewed_by="manual",
                )
                dictionary.add(
                    lab_name, code, display,
                    source="manual", confidence=1.0,
                    verified=True, verified_by="manual_review",
                    raw_name=lab_name,
                )
                dictionary.save()
                print(f"   ✅ Resolved {qid} → {code}")
            else:
                store.update_status(qid, "pending_review")

        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(candidates):
                hit = candidates[idx]
                _accept_match(store, dictionary, qid, lab_name, hit)
            else:
                print("   Invalid candidate number.")
                store.update_status(qid, "pending_review")

        else:
            print("   Invalid choice.")
            store.update_status(qid, "pending_review")

    # Summary
    remaining = len(store.get_pending())
    print(f"\n📊 {remaining} record(s) still pending.\n")


def _accept_match(store, dictionary, qid, lab_name, hit):
    """Accept a candidate match and update store + dictionary."""
    code = hit.get("loinc_code", hit.get("loinc", ""))
    display = hit.get("display_name", hit.get("display", ""))
    confidence = hit.get("score", hit.get("confidence", 0))
    if isinstance(confidence, (int, float)) and confidence > 1.0:
        confidence /= 100.0

    store.update_status(
        qid, "resolved",
        resolved_loinc=code,
        resolved_display=display,
        reviewed_by="reviewer",
    )
    dictionary.add(
        lab_name, code, display,
        source="review", confidence=confidence,
        verified=True, verified_by="reviewer",
        raw_name=lab_name,
    )
    dictionary.save()
    print(f"   ✅ Resolved {qid} → {code} ({display})")


def main():
    logging.basicConfig(level=logging.WARNING)
    review_pending()


if __name__ == "__main__":
    main()
