"""
Transformation accuracy and business logic tests.
"""
import pytest
import pandas as pd
import numpy as np
from pipeline.extractor import extract_user_data, extract_transaction_data, extract_product_data
from pipeline.transformer import (
    transform_user_data,
    transform_transaction_data,
    transform_product_data,
    aggregate_user_transactions,
)


@pytest.fixture(scope="module")
def raw_users():
    return extract_user_data(100)

@pytest.fixture(scope="module")
def raw_transactions():
    return extract_transaction_data(200)

@pytest.fixture(scope="module")
def raw_products():
    return extract_product_data(50)

@pytest.fixture(scope="module")
def t_users(raw_users):
    return transform_user_data(raw_users)

@pytest.fixture(scope="module")
def t_transactions(raw_transactions):
    return transform_transaction_data(raw_transactions)

@pytest.fixture(scope="module")
def t_products(raw_products):
    return transform_product_data(raw_products)


class TestUserTransformations:

    def test_full_name_concatenated_correctly(self, t_users, raw_users):
        expected = raw_users["first_name"] + " " + raw_users["last_name"]
        assert (t_users["full_name"] == expected).all(), \
            "full_name not concatenated correctly"

    def test_email_is_lowercase(self, t_users):
        assert (t_users["email"] == t_users["email"].str.lower()).all(), \
            "Email addresses are not all lowercase"

    def test_age_groups_are_valid(self, t_users):
        valid_groups = {"young", "adult", "middle_aged", "senior"}
        actual = set(t_users["age_group"].dropna().astype(str).unique())
        assert actual.issubset(valid_groups), \
            f"Invalid age groups found: {actual - valid_groups}"

    def test_high_value_flag_is_correct(self, t_users):
        expected = t_users["account_balance"] > 10000
        assert (t_users["is_high_value"] == expected).all(), \
            "is_high_value flag not calculated correctly"

    def test_signup_date_is_datetime(self, t_users):
        assert pd.api.types.is_datetime64_any_dtype(t_users["signup_date"]), \
            "signup_date not converted to datetime"


class TestTransactionTransformations:

    def test_net_amount_calculated_for_debits(self, t_transactions):
        debits = t_transactions[t_transactions["transaction_type"] == "debit"]
        expected = (debits["amount"] - debits["fee"]).round(2)
        assert (debits["net_amount"] == expected).all(), \
            "net_amount not calculated correctly for debits"

    def test_large_transaction_flag_is_correct(self, t_transactions):
        expected = t_transactions["amount"] > 1000
        assert (t_transactions["is_large_transaction"] == expected).all(), \
            "is_large_transaction flag not calculated correctly"

    def test_month_extracted_from_created_at(self, t_transactions):
        assert t_transactions["month"].between(1, 12).all(), \
            "Month values outside valid range 1-12"

    def test_year_extracted_from_created_at(self, t_transactions):
        assert t_transactions["year"].between(2020, 2026).all(), \
            "Year values outside expected range"

    def test_created_at_is_datetime(self, t_transactions):
        assert pd.api.types.is_datetime64_any_dtype(t_transactions["created_at"]), \
            "created_at not converted to datetime"


class TestProductTransformations:

    def test_savings_calculated_correctly(self, t_products, raw_products):
        expected = (raw_products["price"] - raw_products["final_price"]).round(2)
        assert (t_products["savings"] == expected).all(), \
            "savings not calculated correctly"

    def test_savings_pct_within_range(self, t_products):
        assert t_products["savings_pct"].between(0, 100).all(), \
            "savings_pct values outside 0-100 range"

    def test_in_stock_flag_correct(self, t_products, raw_products):
        expected = raw_products["stock_quantity"] > 0
        assert (t_products["in_stock"] == expected).all(), \
            "in_stock flag not calculated correctly"

    def test_price_tiers_are_valid(self, t_products):
        valid = {"budget", "mid_range", "premium", "luxury"}
        actual = set(t_products["price_tier"].dropna().astype(str).unique())
        assert actual.issubset(valid), \
            f"Invalid price tiers found: {actual - valid}"


class TestAggregation:

    def test_user_transaction_aggregation_joins_correctly(self, raw_users, raw_transactions):
        result = aggregate_user_transactions(raw_users, raw_transactions)
        assert len(result) == len(raw_users), \
            "Aggregation changed user row count"

    def test_total_spent_is_positive_for_active_users(self, raw_users, raw_transactions):
        result = aggregate_user_transactions(raw_users, raw_transactions)
        active = result[result["total_spent"].notna()]
        assert (active["total_spent"] >= 0).all(), \
            "Negative total_spent values found"

    def test_avg_transaction_is_positive(self, raw_users, raw_transactions):
        result = aggregate_user_transactions(raw_users, raw_transactions)
        active = result[result["avg_transaction"].notna()]
        assert (active["avg_transaction"] > 0).all(), \
            "Non-positive avg_transaction values found"
