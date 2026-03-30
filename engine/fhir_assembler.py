"""FHIR Assembler — builds FHIR R4 Observation resources from transformed rows."""

import logging
import uuid

logger = logging.getLogger(__name__)

UCUM_SYSTEM = "http://unitsofmeasure.org"
LOINC_SYSTEM = "http://loinc.org"
CATEGORY_SYSTEM = "http://terminology.hl7.org/CodeSystem/observation-category"


def assemble_observation(row: dict, loinc_result: dict, config: dict) -> dict:
    """Build a FHIR R4 Observation resource.

    Args:
        row:          Transformed row dict.
        loinc_result: Dict with 'loinc', 'display', 'source', 'confidence' from resolver.
        config:       Validated config dict.

    Returns:
        FHIR R4 Observation resource as a dict.
    """
    resource_id = str(uuid.uuid4())
    patient_id = row.get("patient_id", "unknown")
    patient_id_system = config.get("patient_id_system", "urn:oid:unknown")

    observation = {
        "resourceType": "Observation",
        "id": resource_id,
        "status": "final",
        "meta": _build_meta(loinc_result, row),
        "category": [
            {
                "coding": [
                    {
                        "system": CATEGORY_SYSTEM,
                        "code": "laboratory",
                        "display": "Laboratory",
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": LOINC_SYSTEM,
                    "code": loinc_result.get("loinc", ""),
                    "display": loinc_result.get("display", ""),
                }
            ],
            "text": row.get("lab_name", ""),
        },
        "subject": {
            "identifier": {
                "system": patient_id_system,
                "value": str(patient_id),
            }
        },
    }

    # effectiveDateTime
    effective_dt = row.get("effective_datetime", "")
    if effective_dt:
        observation["effectiveDateTime"] = effective_dt

    # valueQuantity or valueString
    value = row.get("value", "")
    unit = row.get("unit", "")
    if value:
        try:
            numeric_val = float(value)
            observation["valueQuantity"] = {
                "value": numeric_val,
                "unit": unit,
                "system": UCUM_SYSTEM,
            }
        except (ValueError, TypeError):
            # Non-numeric result → use valueString
            observation["valueString"] = value

    # referenceRange
    ref_range = row.get("reference_range", "")
    if ref_range:
        observation["referenceRange"] = [{"text": ref_range}]

    return observation


def _build_meta(loinc_result: dict, row: dict) -> dict:
    """Build meta.tag with resolver provenance and optional quarantine info."""
    tags = []

    # Resolution source tag
    source = loinc_result.get("source", "")
    if source:
        tags.append({
            "system": "urn:fhir-gateway:resolver-source",
            "code": source,
            "display": f"Resolved via {source}",
        })

    # Confidence tag
    confidence = loinc_result.get("confidence", 0)
    if confidence:
        tags.append({
            "system": "urn:fhir-gateway:resolver-confidence",
            "code": str(round(confidence, 4)),
            "display": f"Confidence: {confidence:.2%}",
        })

    # Quarantine tag (if reprocessed from quarantine)
    quarantine_id = row.get("_quarantine_id")
    if quarantine_id:
        tags.append({
            "system": "urn:fhir-gateway:quarantine-id",
            "code": quarantine_id,
            "display": f"Reprocessed from quarantine {quarantine_id}",
        })

    meta = {}
    if tags:
        meta["tag"] = tags
    return meta
