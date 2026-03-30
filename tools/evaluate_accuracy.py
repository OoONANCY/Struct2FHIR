"""Evaluate LOINC resolver accuracy against a ground-truth labeled test set.

Usage:
    python tools/evaluate_accuracy.py --truth ground_truth.csv

The CSV must have columns: lab_name, expected_loinc
Reports Precision, Recall, F1, and per-row results.
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from loinc.resolver import LoincResolver


def evaluate(truth_path: str, *, export: str | None = None) -> dict:
    """Run the resolver against a labeled truth set and compute P/R/F1."""
    resolver = LoincResolver()

    tp = 0  # true positive: resolved AND matches expected
    fp = 0  # false positive: resolved BUT wrong LOINC
    fn = 0  # false negative: quarantined or unresolved
    total = 0
    results = []

    with open(truth_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lab_name = row.get("lab_name", "").strip()
            expected = row.get("expected_loinc", "").strip()
            if not lab_name or not expected:
                continue

            total += 1
            result = resolver.resolve(lab_name)

            if result.resolved:
                if result.loinc == expected:
                    tp += 1
                    verdict = "TP"
                else:
                    fp += 1
                    verdict = "FP"
            else:
                fn += 1
                verdict = "FN"

            results.append({
                "lab_name": lab_name,
                "expected_loinc": expected,
                "resolved_loinc": result.loinc if result.resolved else None,
                "source": result.source,
                "confidence": result.confidence,
                "verdict": verdict,
            })

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    summary = {
        "total": total,
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
    }

    if export:
        with open(export, "w", encoding="utf-8") as f:
            json.dump({"summary": summary, "results": results}, f, indent=2)
        print(f"📁 Detailed results exported to {export}")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Evaluate LOINC resolver accuracy")
    parser.add_argument("--truth", required=True,
                        help="CSV file with columns: lab_name, expected_loinc")
    parser.add_argument("--export", default=None,
                        help="Export detailed results to JSON file")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    print("\n🔬 LOINC Resolver Accuracy Evaluation\n")
    summary = evaluate(args.truth, export=args.export)

    print(f"   Total test cases:    {summary['total']}")
    print(f"   True Positives:      {summary['true_positives']}")
    print(f"   False Positives:     {summary['false_positives']}")
    print(f"   False Negatives:     {summary['false_negatives']}")
    print(f"   Precision:           {summary['precision']:.2%}")
    print(f"   Recall:              {summary['recall']:.2%}")
    print(f"   F1 Score:            {summary['f1_score']:.2%}")
    print()


if __name__ == "__main__":
    main()
