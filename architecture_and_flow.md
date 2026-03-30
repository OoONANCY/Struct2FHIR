# Struct2FHIR — Architecture & User Flow

This document outlines the final architecture and the exact step-by-step flow a typical user takes when adopting and running the FHIR Gateway.

## 1. Final Architecture Diagram

```mermaid
flowchart TD
    %% Base styling
    classDef config fill:#f9f9f9,stroke:#333
    classDef engine fill:#e1f5fe,stroke:#0288d1,color:#01579b
    classDef loinc fill:#f3e5f5,stroke:#7b1fa2,color:#4a148c
    classDef error fill:#ffebee,stroke:#d32f2f,color:#b71c1c
    classDef external fill:#fff3e0,stroke:#e65100,color:#e65100

    UserCSV[\"Input CSV (Lab Data)"\]:::external
    Config(YAML Config):::config
    FHIR_Server[\"Target FHIR Server"\]:::external
    
    subgraph Engine["Gateway Engine"]
        Reader("CSV Reader\n(applies column mapping)"):::engine
        Transformer("Transformer\n(cleans, normalizes dates/units)"):::engine
        Assembler("FHIR Assembler\n(builds R4 Observation)"):::engine
        Validator("Validator\n(structural checks)"):::engine
        Sender("HTTP Sender\n(POST with auto-retry)"):::engine
    end

    subgraph Resolver["LOINC Resolution Engine"]
        Dict[("Local Dictionary\n(Sub-ms Cache)")]:::loinc
        Fuzzy("Fuzzy Matcher\n(>95% conf)"):::loinc
        API(("NLM Clinical API\n(>80% conf)")):::loinc
    end

    subgraph QuarantineSys["Quarantine System"]
        QStore[("Quarantine Store\n(JSON)")]:::error
        Reviewer_CLI("Reviewer CLI\n(Human Approval)"):::error
        Reprocessor("Reprocessor\n(Sends resolved)"):::error
    end
    
    %% Main flow
    UserCSV --> Reader
    Config -.-> Reader
    Config -.-> Transformer
    Reader --> Transformer
    Transformer -->|Extracted Lab Name| Resolver
    Resolver -->|Code + Display| Assembler
    Transformer --> Assembler
    Assembler --> Validator
    Validator --> Sender
    Sender -->|POST /Observation| FHIR_Server

    %% Resolution Flow
    Resolver -.-> Dict
    Dict -.->|Hit| Assembler
    Dict -.->|Miss| Fuzzy
    Fuzzy -.->|Hit| Assembler
    Fuzzy -.->|Miss| API
    API -.->|Hit| Assembler
    API -.->|Miss| QStore

    %% Quarantine Flow
    QStore -.-> Reviewer_CLI
    Reviewer_CLI -.->|Approved Match| Dict
    Reviewer_CLI -.->|Resolved| Reprocessor
    Reprocessor -.->|Re-enters pipeline| Assembler
```

---

## 2. Complete User Workflow

Below is the step-by-step lifecycle of a user adopting the tool for a new lab CSV schema.

### Phase 1: One-Time Setup

**1. Get LOINC Data**
The user registers for free at loinc.org, downloads [Loinc.csv](file:///Users/nancysmac/Coding/Struct2FHIR/Loinc_2.82/LoincTable/Loinc.csv) (~928MB), and generates the lightweight lookup corpus mapping.
```bash
python tools/build_corpus.py --input ~/Downloads/Loinc.csv
# ↳ Creates a 4.8MB loinc_corpus.json and fully deletes the rest.
```

### Phase 2: Onboarding a New CSV Source

**1. Create a Configuration**
The user creates a YAML file mapping their specific CSV columns to the standard fields.
```bash
cp config/sources/example_lab.yaml config/sources/city_hospital.yaml
# They edit city_hospital.yaml, mapping e.g., 'Test_Description' → 'lab_name'
```

**2. Validate the Setup**
The workflow verifies that the YAML syntax is valid and perfectly matches the actual headers in their data.
```bash
python tools/validate_config.py --config config/sources/city_hospital.yaml --csv data.csv
```

**3. Dry-Run Verification**
The user pushes the first 50 rows through without actually sending anything over the network, inspecting the generated FHIR JSON visually.
```bash
python main.py --config config/sources/city_hospital.yaml --input data.csv --dry-run --limit 50
```

### Phase 3: Production Processing

**1. Process the File**
For massive files, the user invokes the asynchronous pipeline, utilizing 20 concurrent workers to blitz through thousands of rows per second.
```bash
python main_async.py --config config/sources/city_hospital.yaml --input data.csv --workers 20
```

* Behind the scenes, the gateway hits the Local Dictionary instantly. If the lab test is totally new, it uses Fuzzy Matching, and if that is unsure, it queries the NLM API online.
* Successful resolutions are immediately cached back to the dictionary, making the system progressively faster every second.

### Phase 4: Exception Handling (Quarantine)

Any test names that are completely unrecognizable (e.g. `xyz123 fluid`) are silently caught and dropped into Quarantine without halting the main pipeline.

**1. Review Failures**
The user periodically pulls up the reviewer CLI.
```bash
python -m quarantine.reviewer
```
The CLI shows the obscure test name and suggests the closest fuzzy/API matches. The user types `1` to accept a match, or `m` to manually type in a LOINC code they looked up themselves.

**2. Reprocess the Fixed Data**
Once reviewed, the records are seamlessly re-injected into the FHIR pipeline and sent to the server.
```bash
python -m quarantine.reprocessor --config config/sources/city_hospital.yaml
```

**3. Future Proofing**
Because the user approved the map in Quarantine, that mapping is permanently saved to their Local Dictionary. The next time `xyz123 fluid` appears in tomorrow's CSV, it processes instantly with zero human intervention.
