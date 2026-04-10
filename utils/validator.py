"""
Data quality validation utilities used across test files.
"""
import pandas as pd
import numpy as np
from typing import List, Optional


def check_no_nulls(df: pd.DataFrame, columns: List[str]) -> dict:
    """Return null counts for specified columns."""
    return {col: int(df[col].isnull().sum()) for col in columns}


def check_unique(df: pd.DataFrame, column: str) -> bool:
    """Return True if all values in column are unique."""
    return df[column].nunique() == len(df)


def check_no_duplicates(df: pd.DataFrame, subset: List[str]) -> int:
    """Return count of duplicate rows based on subset of columns."""
    return int(df.duplicated(subset=subset).sum())


def check_value_range(
    df: pd.DataFrame, column: str, min_val: float, max_val: float
) -> dict:
    """Return count of values outside expected range."""
    below = int((df[column] < min_val).sum())
    above = int((df[column] > max_val).sum())
    return {"below_min": below, "above_max": above}


def check_column_exists(df: pd.DataFrame, columns: List[str]) -> List[str]:
    """Return list of columns that are missing from the DataFrame."""
    return [col for col in columns if col not in df.columns]


def check_data_types(df: pd.DataFrame, expected_types: dict) -> dict:
    """Return mismatched column types."""
    mismatches = {}
    for col, expected in expected_types.items():
        if col in df.columns:
            actual = str(df[col].dtype)
            if expected not in actual:
                mismatches[col] = {"expected": expected, "actual": actual}
    return mismatches


def check_referential_integrity(
    df_child: pd.DataFrame,
    df_parent: pd.DataFrame,
    child_key: str,
    parent_key: str,
) -> int:
    """Return count of orphaned child records."""
    orphans = ~df_child[child_key].isin(df_parent[parent_key])
    return int(orphans.sum())


def check_row_count(df: pd.DataFrame, expected: int, tolerance: float = 0.05) -> bool:
    """Return True if row count is within tolerance of expected."""
    lower = expected * (1 - tolerance)
    upper = expected * (1 + tolerance)
    return lower <= len(df) <= upper
