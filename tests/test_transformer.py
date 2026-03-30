"""Tests for engine.transformer."""

import pytest
from engine.transformer import transform_row, TransformError


def _make_config(**overrides):
    cfg = {
        "source_name": "test",
        "fhir_server_url": "http://localhost/fhir",
        "column_map": {},
        "date_formats": ["%Y-%m-%d", "%m/%d/%Y %H:%M"],
        "transform_rules": {
            "unit_map": {"MG/DL": "mg/dL", "mEq/L": "meq/L"},
            "custom_rules": [
                {"field": "lab_name", "find": "Na+", "replace": "Sodium"},
            ],
        },
    }
    cfg.update(overrides)
    return cfg


class TestTransformer:
    def test_unit_normalization(self):
        row = {"lab_name": "Glucose", "value": "95", "unit": "MG/DL"}
        result = transform_row(row, _make_config())
        assert result["unit"] == "mg/dL"

    def test_custom_rule_replace(self):
        row = {"lab_name": "Na+", "value": "140", "unit": "mEq/L"}
        result = transform_row(row, _make_config())
        assert result["lab_name"] == "Sodium"

    def test_date_parsing_standard(self):
        row = {"lab_name": "Glucose", "value": "95", "unit": "mg/dL",
               "collected_at": "2024-03-15"}
        result = transform_row(row, _make_config())
        assert result["effective_datetime"].startswith("2024-03-15")

    def test_date_parsing_alt_format(self):
        row = {"lab_name": "Glucose", "value": "95", "unit": "mg/dL",
               "collected_at": "03/17/2024 08:30"}
        result = transform_row(row, _make_config())
        assert "2024-03-17" in result["effective_datetime"]

    def test_invalid_date_raises(self):
        row = {"lab_name": "Glucose", "value": "95", "unit": "mg/dL",
               "collected_at": "invalid-date"}
        with pytest.raises(TransformError, match="Cannot parse"):
            transform_row(row, _make_config())

    def test_offset_mode(self):
        row = {"lab_name": "Glucose", "value": "95", "unit": "mg/dL",
               "_offset_minutes": "60"}
        config = _make_config(offset_column="labresultoffset",
                              reference_date="2024-01-01T00:00:00Z")
        result = transform_row(row, config)
        assert "2024-01-01T01:00:00" in result["effective_datetime"]

    def test_negative_offset(self):
        row = {"lab_name": "Glucose", "value": "95", "unit": "mg/dL",
               "_offset_minutes": "-120"}
        config = _make_config(offset_column="labresultoffset",
                              reference_date="2024-01-01T00:00:00Z")
        result = transform_row(row, config)
        assert "2023-12-31T22:00:00" in result["effective_datetime"]

    def test_value_normalization_integer(self):
        row = {"lab_name": "WBC", "value": "7.0000", "unit": "K/mcL"}
        result = transform_row(row, _make_config())
        assert result["value"] == "7"

    def test_value_normalization_float(self):
        row = {"lab_name": "Hgb", "value": "14.5000", "unit": "g/dL"}
        result = transform_row(row, _make_config())
        assert result["value"] == "14.5"

    def test_empty_value(self):
        row = {"lab_name": "Test", "value": "", "unit": "mg/dL"}
        result = transform_row(row, _make_config())
        assert result["value"] == ""
