"""Quarantine Reviewer — interactive CLI tool with color-coded confidence."""

import sys
import logging

from loinc.dictionary import LoincDictionary
from loinc.fuzzy_matcher import FuzzyMatcher
from loinc import api_client
from quarantine.store import QuarantineStore

logger = logging.getLogger(__name__)

# ANSI color codes
_RED = "\033[91m"
_YELLOW = "\033[93m"
_GREEN = "\033[92m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _color_confidence(score: float) -> str:
    """Return a color-coded confidence string."""
    if score >= 95:
        return f"{_GREEN}{score:.1f}%{_RESET}"
    elif score >= 80:
        return f"{_CYAN}{score:.1f}%{_RESET}"
    elif score >= 60:
        return f"{_YELLOW}{score:.1f}%{_RESET}"
    else:
        return f"{_RED}{score:.1f}%{_RESET}"


def review_pending(store: QuarantineStore | None = None,
                   dictionary: LoincDictionary | None = None,
                   fuzzy: FuzzyMatcher | None = None) -> None:
    """Interactive CLI review of pending quarantine records."""
    store = store or QuarantineStore()
    dictionary = dictionary or LoincDictionary()
    fuzzy = fuzzy or FuzzyMatcher()

    pending = store.get_pending()
    if not pending:
        print(f"\n{_GREEN}✅ No pending quarantine records to review.{_RESET}\n")
        return

    # Show backlog stats
    stats = store.stats()
    print(f"\n{_BOLD}📋 Quarantine Backlog{_RESET}")
    for status_name, count in sorted(stats.items()):
        if status_name != "total":
            print(f"   {status_name}: {count}")
    print(f"   {_BOLD}total: {stats['total']}{_RESET}")
    print(f"\n{_BOLD}{len(pending)} record(s) pending review{_RESET}")
    print("=" * 60)

    for record in pending:
        qid = record["id"]
        lab_name = record["lab_name"]
        reason = record.get("reason", "unknown")

        store.update_status(qid, "in_review")

        print(f"\n{_BOLD}🔍 Record: {qid}{_RESET}")
        print(f"   Lab name:  {_BOLD}{lab_name}{_RESET}")
        print(f"   Reason:    {reason}")

        # Show existing candidates
        candidates = record.get("candidates", [])
        if not candidates:
            candidates = fuzzy.match(lab_name)

        if candidates:
            print(f"\n   {_BOLD}Top candidates:{_RESET}")
            for i, c in enumerate(candidates, 1):
                name = c.get("display_name", c.get("display", ""))
                code = c.get("loinc_code", c.get("loinc", ""))
                score = c.get("score", c.get("confidence", 0))
                if isinstance(score, float) and score < 1.0:
                    score *= 100
                colored = _color_confidence(score)
                print(f"   {i}. {name} [{code}] — {colored}")
        else:
            print(f"\n   {_YELLOW}No candidates available.{_RESET}")

        print(f"\n   {_BOLD}Actions:{_RESET}")
        print("   [1-N]  Accept candidate N")
        print("   [s]    Search with a different term")
        print("   [m]    Manually enter LOINC code")
        print("   [e]    Escalate for expert review")
        print("   [u]    Mark as unmappable")
        print("   [q]    Quit review")

        choice = input(f"\n   {_BOLD}Choice:{_RESET} ").strip().lower()

        if choice == "q":
            store.update_status(qid, "pending_review")
            print(f"\n{_CYAN}⏸  Review paused.{_RESET}")
            break

        elif choice == "u":
            store.update_status(qid, "unmappable")
            print(f"   {_RED}❌ Marked {qid} as unmappable{_RESET}")

        elif choice == "e":
            store.update_status(qid, "escalated")
            print(f"   {_YELLOW}⬆ Escalated {qid} for expert review{_RESET}")

        elif choice == "s":
            term = input("   Search term: ").strip()
            new_results = fuzzy.match(term)
            api_results = api_client.search_loinc(term) or []
            all_results = new_results + api_results
            if all_results:
                print(f"\n   {_BOLD}Search results:{_RESET}")
                for i, r in enumerate(all_results, 1):
                    name = r.get("display_name", "")
                    code = r.get("loinc_code", "")
                    score = r.get("score", r.get("confidence", 0))
                    if isinstance(score, float) and score < 1.0:
                        score *= 100
                    colored = _color_confidence(score)
                    print(f"   {i}. {name} [{code}] — {colored}")
                sub = input("\n   Accept which # (or 'n' to skip): ").strip()
                if sub.isdigit() and 1 <= int(sub) <= len(all_results):
                    hit = all_results[int(sub) - 1]
                    _accept_match(store, dictionary, qid, lab_name, hit)
                else:
                    store.update_status(qid, "pending_review")
            else:
                print(f"   {_YELLOW}No results found.{_RESET}")
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
                print(f"   {_GREEN}✅ Resolved {qid} → {code}{_RESET}")
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
    print(f"   {_GREEN}✅ Resolved {qid} → {code} ({display}){_RESET}")


def main():
    logging.basicConfig(level=logging.WARNING)
    review_pending()


if __name__ == "__main__":
    main()
