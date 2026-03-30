"""Tests for engine.fhir_assembler."""

import pytest
from engine.fhir_assembler import assemble_observation


def _make_row(**overrides):
    row = {
        "patient_id": "P001",
        "lab_name": "Glucose",
        "value": "95",
        "unit": "mg/dL",
        "effective_datetime": "2024-03-15T00:00:00+00:00",
        "reference_range": "70-100",
    }
    row.update(overrides)
    return row


def _make_loinc(**overrides):
    loinc = {"loinc": "2345-7", "display": "Glucose [Mass/volume] in Serum"}
    loinc.update(overrides)
    return loinc


def _make_config(**overrides):
    cfg = {
        "source_name": "test",
        "fhir_server_url": "http://localhost/fhir",
        "patient_id_system": "urn:oid:2.16.840.1.113883.3.test",
    }
    cfg.update(overrides)
    return cfg


class TestFHIRAssembler:
    def test_resource_type(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["resourceType"] == "Observation"

    def test_status(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["status"] == "final"

    def test_loinc_code(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        coding = obs["code"]["coding"][0]
        assert coding["system"] == "http://loinc.org"
        assert coding["code"] == "2345-7"
        assert coding["display"] == "Glucose [Mass/volume] in Serum"

    def test_subject(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["subject"]["identifier"]["value"] == "P001"
        assert "urn:oid" in obs["subject"]["identifier"]["system"]

    def test_effective_datetime(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["effectiveDateTime"] == "2024-03-15T00:00:00+00:00"

    def test_value_quantity(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["valueQuantity"]["value"] == 95.0
        assert obs["valueQuantity"]["unit"] == "mg/dL"

    def test_reference_range(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["referenceRange"][0]["text"] == "70-100"

    def test_non_numeric_value(self):
        obs = assemble_observation(
            _make_row(value="positive"), _make_loinc(), _make_config()
        )
        assert "valueString" in obs
        assert obs["valueString"] == "positive"
        assert "valueQuantity" not in obs

    def test_no_datetime(self):
        obs = assemble_observation(
            _make_row(effective_datetime=""), _make_loinc(), _make_config()
        )
        assert "effectiveDateTime" not in obs

    def test_no_reference_range(self):
        obs = assemble_observation(
            _make_row(reference_range=""), _make_loinc(), _make_config()
        )
        assert "referenceRange" not in obs

    def test_category(self):
        obs = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs["category"][0]["coding"][0]["code"] == "laboratory"

    def test_unique_ids(self):
        obs1 = assemble_observation(_make_row(), _make_loinc(), _make_config())
        obs2 = assemble_observation(_make_row(), _make_loinc(), _make_config())
        assert obs1["id"] != obs2["id"]
