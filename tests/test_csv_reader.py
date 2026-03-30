"""Tests for engine.csv_reader."""

import os
import tempfile
import pytest

from engine.csv_reader import read_csv


def _make_config(column_map=None, **overrides):
    cfg = {
        "source_name": "test",
        "fhir_server_url": "http://localhost/fhir",
        "column_map": column_map or {
            "patient_id": "PatientID",
            "lab_name": "TestName",
            "value": "Result",
            "unit": "Units",
            "collected_at": "CollectedDate",
            "reference_range": "RefRange",
        },
        "delimiter": ",",
        "encoding": "utf-8",
        "skip_rows": 0,
    }
    cfg.update(overrides)
    return cfg


class TestCSVReader:
    def test_basic_read(self):
        csv_path = os.path.join(os.path.dirname(__file__), "fixtures", "sample.csv")
        config = _make_config()
        rows = list(read_csv(csv_path, config))
        assert len(rows) == 10
        assert rows[0]["patient_id"] == "P001"
        assert rows[0]["lab_name"] == "Glucose"
        assert rows[0]["value"] == "95.0"

    def test_column_mapping(self):
        csv_path = os.path.join(os.path.dirname(__file__), "fixtures", "sample.csv")
        config = _make_config()
        rows = list(read_csv(csv_path, config))
        for row in rows:
            assert "patient_id" in row
            assert "lab_name" in row
            assert "value" in row
            assert "unit" in row

    def test_missing_column_raises(self):
        csv_path = os.path.join(os.path.dirname(__file__), "fixtures", "sample.csv")
        config = _make_config(column_map={
            "patient_id": "PatientID",
            "lab_name": "NonExistentColumn",
            "value": "Result",
            "unit": "Units",
        })
        with pytest.raises(ValueError, match="not found"):
            list(read_csv(csv_path, config))

    def test_skip_rows(self):
        content = "metadata line\nPatientID,TestName,Result,Units\nP001,Glucose,95,mg/dL\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            f.flush()
            config = _make_config(
                column_map={"patient_id": "PatientID", "lab_name": "TestName",
                            "value": "Result", "unit": "Units"},
                skip_rows=1,
            )
            rows = list(read_csv(f.name, config))
            assert len(rows) == 1
            assert rows[0]["patient_id"] == "P001"
        os.unlink(f.name)

    def test_file_not_found(self):
        config = _make_config()
        with pytest.raises(FileNotFoundError):
            list(read_csv("/nonexistent/file.csv", config))

    def test_delimiter(self):
        content = "PatientID\tTestName\tResult\tUnits\nP001\tGlucose\t95\tmg/dL\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            f.flush()
            config = _make_config(
                column_map={"patient_id": "PatientID", "lab_name": "TestName",
                            "value": "Result", "unit": "Units"},
                delimiter="\t",
            )
            rows = list(read_csv(f.name, config))
            assert len(rows) == 1
            assert rows[0]["value"] == "95"
        os.unlink(f.name)
