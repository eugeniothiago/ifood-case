"""Unit tests for analysis input validation and date-bound constants."""

from unittest.mock import MagicMock, patch

import pytest

from analysis.q1_monthly_avg_amount import answer_q1, answer_q1_with_optimization
from analysis.q2_avg_passengers_per_hour import (
    JUNE_START,
    MAY_START,
    answer_q2,
    answer_q2_with_optimization,
    _may_filter,
)


def test_answer_q1_rejects_empty_gold_table() -> None:
    """Q1 must reject a blank Gold identifier before Spark access."""
    with pytest.raises(ValueError):
        answer_q1(MagicMock(), "")


def test_answer_q1_optimized_rejects_empty_gold_table() -> None:
    """Optimized Q1 must reject a blank Gold identifier before Spark access."""
    with pytest.raises(ValueError):
        answer_q1_with_optimization(MagicMock(), "")


def test_answer_q2_rejects_empty_gold_table() -> None:
    """Q2 must reject a blank Gold identifier before Spark access."""
    with pytest.raises(ValueError):
        answer_q2(MagicMock(), "")


def test_answer_q2_optimized_rejects_empty_gold_table() -> None:
    """Optimized Q2 must reject a blank Gold identifier before Spark access."""
    with pytest.raises(ValueError):
        answer_q2_with_optimization(MagicMock(), "")


def test_may_start_constant() -> None:
    """Q2 must start at the first day of May 2023."""
    assert MAY_START == "2023-05-01"


def test_june_start_constant() -> None:
    """Q2 must use June 1 as its exclusive upper bound."""
    assert JUNE_START == "2023-06-01"


def test_answer_q1_rejects_whitespace_gold_table() -> None:
    """Q1 must reject a whitespace-only Gold identifier."""
    with pytest.raises(ValueError):
        answer_q1(MagicMock(), "   ")


def test_may_filter_returns_column() -> None:
    """The May filter must build both partition and timestamp bounds without Spark."""

    class Expression:
        def __ge__(self, other: object) -> "Expression":
            return self

        def __lt__(self, other: object) -> "Expression":
            return self

        def __and__(self, other: object) -> "Expression":
            return self

    expression = Expression()
    with patch("analysis.q2_avg_passengers_per_hour.F.col", return_value=expression) as col, patch(
        "analysis.q2_avg_passengers_per_hour.F.to_date", return_value=expression
    ) as to_date, patch("analysis.q2_avg_passengers_per_hour.F.lit", return_value=expression) as lit:
        result = _may_filter()

    assert result is expression
    assert [call.args[0] for call in col.call_args_list] == [
        "tpep_pickup_datetime",
        "pickup_date",
        "pickup_date",
    ]
    assert to_date.call_count == 2
    assert lit.call_count == 4
