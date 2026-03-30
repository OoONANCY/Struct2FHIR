"""Tests for loinc.resolver (mocked dependencies)."""

import pytest
from unittest.mock import MagicMock, patch

from loinc.resolver import LoincResolver, ResolveResult


class TestLoincResolver:
    def _make_resolver(self, dict_lookup=None, fuzzy_results=None, api_results=None):
        """Build a resolver with mocked dependencies."""
        dictionary = MagicMock()
        dictionary.lookup.return_value = dict_lookup

        fuzzy = MagicMock()
        fuzzy.match.return_value = fuzzy_results or []
        fuzzy.is_loaded = True

        resolver = LoincResolver(dictionary=dictionary, fuzzy_matcher=fuzzy)
        return resolver

    def test_cache_hit(self):
        resolver = self._make_resolver(dict_lookup={
            "loinc": "2345-7",
            "display": "Glucose [Mass/volume] in Serum",
            "provenance": {"confidence": 0.99, "times_used": 10, "last_used": ""},
        })
        result = resolver.resolve("Glucose")
        assert result.resolved
        assert result.loinc == "2345-7"
        assert result.source == "cache"

    def test_fuzzy_high_confidence(self):
        resolver = self._make_resolver(
            dict_lookup=None,
            fuzzy_results=[{
                "display_name": "Glucose [Mass/volume] in Serum",
                "loinc_code": "2345-7",
                "score": 97.5,
            }],
        )
        result = resolver.resolve("Glucos")
        assert result.resolved
        assert result.loinc == "2345-7"
        assert result.source == "fuzzy"

    @patch("loinc.resolver.api_client")
    def test_api_fallback(self, mock_api):
        mock_api.search_loinc.return_value = [{
            "loinc_code": "2951-2",
            "display_name": "Sodium [Moles/volume] in Serum",
            "confidence": 0.92,
        }]
        resolver = self._make_resolver(
            dict_lookup=None,
            fuzzy_results=[{
                "display_name": "Some match",
                "loinc_code": "9999-9",
                "score": 70.0,
            }],
        )
        result = resolver.resolve("Sodium level")
        assert result.resolved
        assert result.loinc == "2951-2"
        assert result.source == "api"

    @patch("loinc.resolver.api_client")
    def test_quarantine_when_all_fail(self, mock_api):
        mock_api.search_loinc.return_value = [{
            "loinc_code": "9999-9",
            "display_name": "Unknown Test",
            "confidence": 0.3,
        }]
        resolver = self._make_resolver(dict_lookup=None, fuzzy_results=[])
        result = resolver.resolve("XYZ Unknown Test")
        assert result.quarantined
        assert not result.resolved

    def test_empty_lab_name(self):
        resolver = self._make_resolver()
        result = resolver.resolve("")
        assert result.quarantined

    @patch("loinc.resolver.api_client")
    def test_api_unreachable(self, mock_api):
        mock_api.search_loinc.return_value = []
        resolver = self._make_resolver(dict_lookup=None, fuzzy_results=[])
        result = resolver.resolve("Some Lab Test")
        assert result.quarantined
