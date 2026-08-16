"""Unit tests for Gold validation and contract constants."""

from unittest.mock import MagicMock

import pytest

from src.gold import GOLD_PARTITION_COLUMN, GOLD_WRITE_MODE, get_gold_summary, model_gold


def test_model_gold_rejects_empty_silver_table() -> None:
    """Gold modeling must reject a blank Silver identifier."""
    with pytest.raises(ValueError):
        model_gold(MagicMock(), "", "gold.table")


def test_model_gold_rejects_empty_gold_table() -> None:
    """Gold modeling must reject a blank Gold identifier."""
    with pytest.raises(ValueError):
        model_gold(MagicMock(), "silver.table", "")


def test_get_gold_summary_rejects_empty_table() -> None:
    """Gold summaries must reject a blank table identifier."""
    with pytest.raises(ValueError):
        get_gold_summary(MagicMock(), "")


def test_gold_partition_column() -> None:
    """Gold must publish the daily pickup_date partition key."""
    assert GOLD_PARTITION_COLUMN == "pickup_date"


def test_gold_write_mode_is_overwrite() -> None:
    """Gold publication must use deterministic overwrite mode."""
    assert GOLD_WRITE_MODE == "overwrite"
