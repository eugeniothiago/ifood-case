"""Tests for the versioned YAML data contract."""

from pathlib import Path

import yaml

from src.config import (
    COMMUNITY_BRONZE_TABLE,
    COMMUNITY_GOLD_TABLE,
    COMMUNITY_SILVER_TABLE,
    DEFAULT_BRONZE_TABLE,
    DEFAULT_GOLD_TABLE,
    DEFAULT_SILVER_TABLE,
)
from src.data_quality import get_data_quality_expectations
from src.schemas import REQUIRED_COLUMNS


CONTRACT_PATH = Path(__file__).parent.parent / "data_contracts" / "yellow_tripdata_contract.yaml"


def _contract() -> dict:
    """Load the repository contract for each focused assertion."""
    with CONTRACT_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def test_contract_version() -> None:
    """The contract version must remain at the documented release."""
    assert _contract()["version"] == "1.0.0"


def test_contract_status_active() -> None:
    """The current contract must be active."""
    assert _contract()["status"] == "active"


def test_contract_storage_format_delta() -> None:
    """The lake storage format must be Delta."""
    assert _contract()["storage"]["format"] == "delta"


def test_contract_schema_has_19_source_columns() -> None:
    """The schema list must contain 19 source columns plus lineage fields."""
    schema = _contract()["schema"]
    source_columns = [entry for entry in schema if not entry["name"].startswith("_")]
    assert len(source_columns) == 19


def test_contract_consumption_required_columns() -> None:
    """YAML consumption requirements must match the Python tuple contract."""
    assert tuple(_contract()["consumption_schema"]["required_columns"]) == REQUIRED_COLUMNS


def test_contract_quality_rules_count() -> None:
    """The contract must define five named data-quality rules."""
    assert len(_contract()["quality_rules"]) == 5


def test_contract_quality_rule_names() -> None:
    """YAML quality rule names must match the Python expectation names."""
    names = {rule["name"] for rule in _contract()["quality_rules"]}
    assert names == set(get_data_quality_expectations())


def test_contract_partitioning_column() -> None:
    """The contract partition key must be pickup_date."""
    assert _contract()["partitioning"]["column"] == "pickup_date"


def test_contract_partitioning_granularity() -> None:
    """The contract partition granularity must be daily."""
    assert _contract()["partitioning"]["granularity"] == "daily"


def test_contract_table_names_match_config() -> None:
    """Unity Catalog table names must match Python defaults."""
    storage = _contract()["storage"]
    assert storage["bronze_table"] == DEFAULT_BRONZE_TABLE
    assert storage["silver_table"] == DEFAULT_SILVER_TABLE
    assert storage["gold_table"] == DEFAULT_GOLD_TABLE


def test_contract_community_fallback_names() -> None:
    """Community fallback names must match Python constants."""
    fallback = _contract()["storage"]["community_edition_fallback"]
    assert fallback == {
        "bronze_table": COMMUNITY_BRONZE_TABLE,
        "silver_table": COMMUNITY_SILVER_TABLE,
        "gold_table": COMMUNITY_GOLD_TABLE,
    }
