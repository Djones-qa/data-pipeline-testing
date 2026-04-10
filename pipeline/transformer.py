"""
Transforms raw extracted data into clean, enriched datasets.
"""
import pandas as pd
import numpy as np


def transform_user_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich user data."""
    result = df.copy()
    result["full_name"] = result["first_name"] + " " + result["last_name"]
    result["email"] = result["email"].str.lower().str.strip()
    result["age_group"] = pd.cut(
        result["age"],
        bins=[0, 25, 40, 60, 100],
        labels=["young", "adult", "middle_aged", "senior"],
    )
    result["is_high_value"] = result["account_balance"] > 10000
    result["signup_date"] = pd.to_datetime(result["signup_date"])
    return result


def transform_transaction_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich transaction data."""
    result = df.copy()
    result["created_at"] = pd.to_datetime(result["created_at"])
    result["net_amount"] = result.apply(
        lambda r: r["amount"] - r["fee"]
        if r["transaction_type"] == "debit"
        else r["amount"] + r["fee"],
        axis=1,
    )
    result["net_amount"] = result["net_amount"].round(2)
    result["is_large_transaction"] = result["amount"] > 1000
    result["month"] = result["created_at"].dt.month
    result["year"] = result["created_at"].dt.year
    return result


def transform_product_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich product data."""
    result = df.copy()
    result["savings"] = (result["price"] - result["final_price"]).round(2)
    result["savings_pct"] = (
        (result["savings"] / result["price"]) * 100
    ).round(2)
    result["price_tier"] = pd.cut(
        result["price"],
        bins=[0, 50, 200, 500, float("inf")],
        labels=["budget", "mid_range", "premium", "luxury"],
    )
    result["in_stock"] = result["stock_quantity"] > 0
    return result


def aggregate_user_transactions(
    users: pd.DataFrame, transactions: pd.DataFrame
) -> pd.DataFrame:
    """Join users with their transaction summary."""
    completed = transactions[transactions["status"] == "completed"]
    summary = (
        completed.groupby("user_id")
        .agg(
            total_transactions=("transaction_id", "count"),
            total_spent=("amount", "sum"),
            avg_transaction=("amount", "mean"),
            max_transaction=("amount", "max"),
        )
        .reset_index()
    )
    summary["total_spent"] = summary["total_spent"].round(2)
    summary["avg_transaction"] = summary["avg_transaction"].round(2)
    summary["max_transaction"] = summary["max_transaction"].round(2)
    return users.merge(summary, on="user_id", how="left")
