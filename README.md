# FHIR Gateway

A configurable gateway that converts lab data in CSV format into FHIR R4 Observation resources.

**Fixed engine, swappable config.** The engine — CSV reader, transformer, LOINC resolver, FHIR assembler, validator, HTTP sender — never changes. A config file written once per source system is the only thing that varies.

---

## Architecture

```
CSV Input
    │
    ▼
┌─────────────┐
│  CSV Reader │  Reads any CSV. Column mapping is in config.
└──────┬──────┘
       │
    ▼
┌─────────────┐
│ Transformer │  Cleans values, normalizes dates/units, applies custom rules.
└──────┬──────┘
       │
    ▼
┌──────────────────────────────────────────────────────────┐
│                    LOINC Resolver                         │
│                                                          │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐   │
│  │  Cache   │ →  │ Fuzzy Match  │ →  │  NLM API     │   │
│  │(dict.json│    │ (RapidFuzz)  │    │  (fallback)  │   │
│  └──────────┘    └──────────────┘    └──────┬───────┘   │
│                                             │           │
│                                    Low confidence?      │
│                                             │           │
│                                             ▼           │
│                                    ┌─────────────────┐  │
│                                    │   Quarantine    │  │
│                                    └─────────────────┘  │
└──────────────────────────────────────────────────────────┘
       │
    ▼
┌──────────────────┐
│  FHIR Assembler  │  Builds FHIR R4 Observation resource.
└──────┬───────────┘
       │
    ▼
┌────────────┐
│  Validator │  Structural checks before sending.
└──────┬─────┘
       │
    ▼
┌─────────────┐
│ HTTP Sender │  POSTs to target FHIR server.
└─────────────┘
```

---

## Quick Start

### 1. Install

```bash
pip install rapidfuzz requests pyyaml aiohttp pytest
```

### 2. Download the LOINC corpus

Register (free) and download `Loinc.csv` from [loinc.org/downloads](https://loinc.org/downloads/loinc/).

```bash
python tools/build_corpus.py --input ~/Downloads/Loinc.csv
# Writes loinc/data/loinc_corpus.json (~25k lab terms)
```

### 3. Create a config for your source

```bash
cp config/sources/example_lab.yaml config/sources/my_lab.yaml
# Edit my_lab.yaml — change column_map to match your CSV headers
```

### 4. Validate your config

```bash
python tools/validate_config.py --config config/sources/my_lab.yaml --csv my_data.csv
```

### 5. Run

```bash
# Dry run first (no FHIR server needed)
python main.py --config config/sources/my_lab.yaml --input my_data.csv --dry-run

# Live run
python main.py --config config/sources/my_lab.yaml --input my_data.csv
```

---

## Config File Reference

```yaml
source_name: "labcorp_feed_a"
fhir_server_url: "https://your-fhir-server.com/fhir"
fhir_auth_token: ""                          # Bearer token, leave blank if open
patient_id_system: "urn:oid:2.16.840.1.113883.3.example"

# Maps internal standard names → actual CSV column headers
column_map:
  patient_id:      "PatientID"          # required
  lab_name:        "TestName"           # required
  value:           "Result"             # required
  unit:            "Units"              # required
  collected_at:    "CollectedDate"      # required
  reference_range: "RefRange"           # optional

date_formats:
  - "%Y-%m-%d"
  - "%d/%m/%Y"
  - "%m/%d/%Y %H:%M"

delimiter: ","
encoding:  "utf-8"
skip_rows: 0    # rows to skip before the header line

transform_rules:
  unit_map:
    "MG/DL":  "mg/dL"
    "mEq/L":  "meq/L"

  custom_rules:
    - field: lab_name
      find:    "Na+"
      replace: "Sodium"
```

---

## LOINC Resolution Flow

```
Input lab name
      │
      ▼
  Local dictionary (loinc/data/loinc_dict.json)
      │ hit → return immediately (sub-millisecond)
      │ miss ↓
      ▼
  Fuzzy matching (RapidFuzz WRatio against loinc_corpus.json)
      │ score ≥ 95% → accept, save to dictionary
      │ score < 95% ↓
      ▼
  NLM LOINC API (clinicaltables.nlm.nih.gov)
      │ confidence ≥ 80% → accept, save to dictionary
      │ confidence < 80% ↓
      ▼
  Quarantine (pending human review)
```

### Confidence tiers

| Score | Action |
|---|---|
| Cache hit | Instant accept |
| Fuzzy ≥ 95% | Accept, write to dictionary |
| Fuzzy 60–94% | Call API |
| API ≥ 80% | Accept, write to dictionary |
| API < 80% | Quarantine |
| API unreachable | Quarantine |

The dictionary grows over time. Every successful resolution is cached so the next occurrence hits instantly — no fuzzy, no API call.

---

## Quarantine Workflow

Records that can't be resolved automatically go to `quarantine/data/quarantine.json`.

### Review pending records

```bash
python -m quarantine.reviewer
```

The reviewer sees:
- The original lab name
- Top fuzzy candidates with confidence scores
- Options to accept a candidate, search with a different term, type a LOINC manually, or mark as unmappable

Every confirmed mapping writes back to the dictionary as `"verified": true`.

### Re-process resolved records

After reviewing, send the resolved records as FHIR:

```bash
# Re-process all resolved records
python -m quarantine.reprocessor --config config/sources/my_lab.yaml

# Re-process a single record by ID
python -m quarantine.reprocessor --config config/sources/my_lab.yaml --id q_20240315_001
```

### Quarantine states

| Status | Meaning |
|---|---|
| `pending_review` | Awaiting human review |
| `in_review` | Currently open by a reviewer |
| `resolved` | LOINC confirmed, ready to re-process |
| `sent` | Re-processed and sent as FHIR |
| `unmappable` | Reviewed, cannot be mapped |
| `reprocess_failed` | Re-processing failed (see failure_reason) |

---

## Dictionary Provenance

Every entry in `loinc/data/loinc_dict.json` carries full provenance:

```json
{
  "serum sodium": {
    "loinc": "2951-2",
    "display": "Sodium [Moles/volume] in Serum or Plasma",
    "provenance": {
      "source": "api",
      "confidence": 0.97,
      "verified": false,
      "verified_by": null,
      "verified_at": null,
      "created_at": "2024-03-08T09:15:00Z",
      "times_used": 847,
      "last_used": "2024-03-15T08:47:00Z",
      "first_seen_raw": "Serum Sodium"
    }
  }
}
```

### Audit the dictionary

```bash
# Overview + auto-show risky entries
python tools/audit_dictionary.py

# Only show entries below 85% confidence
python tools/audit_dictionary.py --low-confidence 0.85

# Unverified entries used 50+ times (highest risk if wrong)
python tools/audit_dictionary.py --high-risk --min-uses 50

# Entries not used in 6 months
python tools/audit_dictionary.py --stale 180

# Export full report
python tools/audit_dictionary.py --export audit_$(date +%Y%m%d).json
```

---

## Async Mode (High Throughput)

The default pipeline (`main.py`) is synchronous. For large files, use the async pipeline which runs rows concurrently and delivers ~8× throughput improvement by overlapping LOINC API calls:

```bash
python main_async.py --config config/sources/my_lab.yaml \
                     --input my_data.csv \
                     --workers 20
```

Typical performance:
- Sync:  ~2–5 rows/sec (bottlenecked on API calls)
- Async: ~20–40 rows/sec with 10 workers, cached dictionary

---

## Running Tests

```bash
pytest                          # all tests
pytest tests/test_transformer.py -v
pytest tests/test_loinc_resolver.py -v
pytest -k "quarantine" -v       # all quarantine-related tests
```

---

## Project Structure

```
fhir-gateway/
│
├── main.py                      # Synchronous pipeline entry point
├── main_async.py                # Async pipeline for high throughput
│
├── engine/
│   ├── csv_reader.py            # Reads any CSV, applies column mapping
│   ├── transformer.py           # Cleans values, normalizes dates/units
│   ├── fhir_assembler.py        # Builds FHIR R4 Observation
│   ├── validator.py             # Validates FHIR before sending
│   └── http_sender.py           # POSTs to FHIR server
│
├── loinc/
│   ├── resolver.py              # Orchestrates cache → fuzzy → API → quarantine
│   ├── dictionary.py            # Local JSON cache with provenance
│   ├── fuzzy_matcher.py         # RapidFuzz wrapper, top-N deduped results
│   ├── api_client.py            # NLM LOINC API client
│   └── data/
│       ├── loinc_dict.json      # Growing cache (starts empty)
│       └── loinc_corpus.json    # Reference corpus (build with tools/build_corpus.py)
│
├── quarantine/
│   ├── store.py                 # Persistent quarantine with state machine
│   ├── reviewer.py              # Interactive CLI review tool
│   ├── reprocessor.py           # Sends resolved records as FHIR
│   └── data/
│       └── quarantine.json
│
├── config/
│   ├── schema.py                # Config loader and validator
│   └── sources/
│       └── example_lab.yaml     # Template — copy and edit per source
│
├── tools/
│   ├── build_corpus.py          # Converts Loinc.csv → loinc_corpus.json
│   ├── audit_dictionary.py      # Surfaces risky dictionary entries
│   └── validate_config.py       # Validates a config file + CSV compatibility
│
└── tests/
    ├── test_csv_reader.py
    ├── test_transformer.py
    ├── test_loinc_resolver.py
    ├── test_fhir_assembler.py
    ├── test_quarantine_store.py
    ├── test_reprocessor.py
    └── test_integration.py
```

---

## Adding a New Source System

1. Copy the example config:
   ```bash
   cp config/sources/example_lab.yaml config/sources/new_hospital.yaml
   ```

2. Edit `column_map` to match the new CSV's headers. Everything else in the engine is untouched.

3. Validate:
   ```bash
   python tools/validate_config.py --config config/sources/new_hospital.yaml --csv sample.csv
   ```

4. Run with `--dry-run` and inspect the output.

That's it. The engine handles the rest.

---

## Requirements

```
rapidfuzz>=3.6.0    # fuzzy matching
requests>=2.31.0    # HTTP (sync)
pyyaml>=6.0.1       # config parsing
aiohttp>=3.9.0      # async HTTP (main_async.py only)
pytest>=8.0.0       # tests
```
