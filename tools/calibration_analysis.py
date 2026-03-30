"""Calibration analysis — checks whether confidence scores are well-calibrated.

Analyzes whether resolver confidence scores correlate with actual accuracy.
Groups resolutions into confidence buckets and compares predicted vs actual accuracy.

Usage:
    python tools/calibration_analysis.py --truth ground_truth.csv
"""

import argparse
import csv
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from loinc.resolver import LoincResolver

BUCKETS = [
    (0.50, 0.60),
    (0.60, 0.70),
    (0.70, 0.80),
    (0.80, 0.90),
    (0.90, 0.95),
    (0.95, 1.01),
]


def analyze_calibration(truth_path: str, *, export: str | None = None) -> list[dict]:
    """Run resolver and compute calibration stats per confidence bucket."""
    resolver = LoincResolver()

    bucket_data = defaultdict(lambda: {"correct": 0, "total": 0, "sum_conf": 0.0})

    with open(truth_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lab_name = row.get("lab_name", "").strip()
            expected = row.get("expected_loinc", "").strip()
            if not lab_name or not expected:
                continue

            result = resolver.resolve(lab_name)
            if not result.resolved:
                continue

            conf = result.confidence
            for low, high in BUCKETS:
                if low <= conf < high:
                    key = f"{low:.2f}-{high:.2f}"
                    bucket_data[key]["total"] += 1
                    bucket_data[key]["sum_conf"] += conf
                    if result.loinc == expected:
                        bucket_data[key]["correct"] += 1
                    break

    calibration = []
    for low, high in BUCKETS:
        key = f"{low:.2f}-{high:.2f}"
        d = bucket_data[key]
        if d["total"] > 0:
            actual_acc = d["correct"] / d["total"]
            avg_conf = d["sum_conf"] / d["total"]
            calibration.append({
                "bucket": key,
                "count": d["total"],
                "avg_confidence": round(avg_conf, 4),
                "actual_accuracy": round(actual_acc, 4),
                "gap": round(avg_conf - actual_acc, 4),
            })

    if export:
        with open(export, "w", encoding="utf-8") as f:
            json.dump(calibration, f, indent=2)
        print(f"📁 Calibration data exported to {export}")

    return calibration


def main():
    parser = argparse.ArgumentParser(description="Analyze resolver confidence calibration")
    parser.add_argument("--truth", required=True,
                        help="CSV with columns: lab_name, expected_loinc")
    parser.add_argument("--export", default=None,
                        help="Export calibration to JSON")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)

    print("\n📊 Confidence Calibration Analysis\n")
    calibration = analyze_calibration(args.truth, export=args.export)

    print(f"   {'Bucket':<14} {'Count':>6} {'Avg Conf':>10} {'Actual Acc':>12} {'Gap':>8}")
    print(f"   {'─' * 14} {'─' * 6} {'─' * 10} {'─' * 12} {'─' * 8}")
    for row in calibration:
        gap_str = f"{row['gap']:+.2%}"
        print(f"   {row['bucket']:<14} {row['count']:>6} {row['avg_confidence']:>10.2%} "
              f"{row['actual_accuracy']:>12.2%} {gap_str:>8}")
    print()

    if not calibration:
        print("   ⚠ No resolved results found — run against a larger truth set.\n")


if __name__ == "__main__":
    main()
