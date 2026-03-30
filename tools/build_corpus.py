"""Build LOINC Corpus — converts official Loinc.csv into loinc_corpus.json.

Generates multiple search terms per LOINC entry for improved fuzzy matching:
  - LONG_COMMON_NAME (primary)
  - COMPONENT (short chemical/analyte name)
  - SHORTNAME (abbreviated form)
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

OUTPUT_PATH = Path(__file__).parent.parent / "loinc" / "data" / "loinc_corpus.json"

# LOINC classes typically associated with lab tests
LAB_CLASSES = {
    "CHEM", "HEM/BC", "SERO", "UA", "MICRO", "DRUG/TOX",
    "COAG", "CELLMARK", "BLDBK", "ABGAS",
}


def build_corpus(input_path: str, output_path: str | None = None) -> int:
    """Parse Loinc.csv and extract lab-relevant terms with multiple search names.

    Args:
        input_path:  Path to official Loinc.csv.
        output_path: Output path (default: loinc/data/loinc_corpus.json).

    Returns:
        Number of unique LOINC entries extracted.
    """
    out = Path(output_path) if output_path else OUTPUT_PATH
    in_path = Path(input_path)

    if not in_path.exists():
        print(f"❌ File not found: {input_path}")
        sys.exit(1)

    corpus = []
    seen = set()

    with open(in_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            loinc_code = row.get("LOINC_NUM", "").strip()
            long_name = row.get("LONG_COMMON_NAME", "").strip()
            component = row.get("COMPONENT", "").strip()
            short_name = row.get("SHORTNAME", "").strip()
            class_type = row.get("CLASS", "").strip()
            status = row.get("STATUS", "").strip()

            # Filter: lab classes + active status + has a name
            if not loinc_code or not long_name:
                continue
            if status and status.upper() != "ACTIVE":
                continue
            if class_type and class_type not in LAB_CLASSES:
                continue
            if loinc_code in seen:
                continue

            seen.add(loinc_code)

            # Build list of unique search terms for this code
            search_terms = [long_name]
            if component and component.lower() != long_name.lower():
                search_terms.append(component)
            if short_name and short_name.lower() not in {t.lower() for t in search_terms}:
                search_terms.append(short_name)

            corpus.append({
                "loinc_code": loinc_code,
                "display_name": long_name,
                "search_terms": search_terms,
            })

    # Sort by display name for consistency
    corpus.sort(key=lambda x: x["display_name"])

    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(corpus, f, indent=2, ensure_ascii=False)

    total_terms = sum(len(e["search_terms"]) for e in corpus)
    print(f"✅ Built corpus with {len(corpus)} lab entries ({total_terms} search terms) → {out}")
    return len(corpus)


def main():
    parser = argparse.ArgumentParser(description="Build LOINC corpus from official Loinc.csv")
    parser.add_argument("--input", required=True, help="Path to Loinc.csv")
    parser.add_argument("--output", default=None, help="Output path (default: loinc/data/loinc_corpus.json)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    build_corpus(args.input, args.output)


if __name__ == "__main__":
    main()
