"""Integration test — end-to-end dry run with sample CSV."""

import os
import pytest
from unittest.mock import patch, MagicMock

from config.schema import load_config
from engine.csv_reader import read_csv
from engine.transformer import transform_row
from engine.fhir_assembler import assemble_observation
from engine.validator import validate_observation


FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
SAMPLE_CSV = os.path.join(FIXTURES, "sample.csv")
EXAMPLE_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config", "sources", "example_lab.yaml")


class TestIntegration:
    def test_end_to_end_dry_run(self):
        """Full pipeline: read → transform → assemble → validate."""
        config = load_config(EXAMPLE_CONFIG)

        # Mock LOINC result (since we don't have the corpus for tests)
        mock_loinc = {"loinc": "2345-7", "display": "Test Result"}

        rows_processed = 0
        valid_count = 0

        for row in read_csv(SAMPLE_CSV, config):
            transformed = transform_row(row, config)
            observation = assemble_observation(transformed, mock_loinc, config)
            errors = validate_observation(observation)

            assert observation["resourceType"] == "Observation"
            assert observation["status"] == "final"
            assert "code" in observation
            assert "subject" in observation

            if not errors:
                valid_count += 1
            rows_processed += 1

        assert rows_processed == 10
        assert valid_count == 10

    def test_custom_rules_applied(self):
        """Verify custom rules (Na+ → Sodium) are applied."""
        config = load_config(EXAMPLE_CONFIG)

        for row in read_csv(SAMPLE_CSV, config):
            transformed = transform_row(row, config)
            # Row with Na+ should be transformed to Sodium
            if "Na+" in str(row.values()):
                assert transformed["lab_name"] == "Sodium"

    def test_unit_normalization(self):
        """Verify unit map transforms are applied."""
        config = load_config(EXAMPLE_CONFIG)

        for row in read_csv(SAMPLE_CSV, config):
            transformed = transform_row(row, config)
            # MG/DL should be normalized to mg/dL
            assert transformed["unit"] != "MG/DL"
