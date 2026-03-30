"""FHIR Assembler — builds FHIR R4 Observation resources from transformed rows."""

import logging
import uuid

logger = logging.getLogger(__name__)


def assemble_observation(row: dict, loinc_result: dict, config: dict) -> dict:
    """Build a FHIR R4 Observation resource.

    Args:
        row:          Transformed row dict.
        loinc_result: Dict with 'loinc', 'display' from the LOINC resolver.
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
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "laboratory",
                        "display": "Laboratory",
                    }
                ]
            }
        ],
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
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

    # valueQuantity
    value = row.get("value", "")
    unit = row.get("unit", "")
    if value:
        vq = {"unit": unit}
        try:
            vq["value"] = float(value)
        except (ValueError, TypeError):
            # Non-numeric result → use valueString instead
            observation["valueString"] = value
            vq = None

        if vq is not None:
            observation["valueQuantity"] = vq

    # referenceRange
    ref_range = row.get("reference_range", "")
    if ref_range:
        observation["referenceRange"] = [{"text": ref_range}]

    return observation
