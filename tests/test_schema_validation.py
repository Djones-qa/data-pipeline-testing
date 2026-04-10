"""
Schema and structure validation tests for all pipeline datasets.
"""
import pytest
import pandas as pd
from pipeline.extractor import (
    extract_user_data,
    extract_transaction_data,
    extract_product_data,
)
from pipeline.transformer import (
    transform_user_data,
    transform_transaction_data,
    transform_product_data,
)
from utils.validator import check_column_exists


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
def transformed_users(raw_users):
    return transform_user_data(raw_users)

@pytest.fixture(scope="module")
def transformed_transactions(raw_transactions):
    return transform_transaction_data(raw_transactions)

@pytest.fixture(scope="module")
def transformed_products(raw_products):
    return transform_product_data(raw_products)


class TestUserSchema:

    def test_raw_user_has_required_columns(self, raw_users):
        required = ["user_id","first_name","last_name","email",
                    "age","country","signup_date","account_balance","is_active"]
        missing = check_column_exists(raw_users, required)
        assert not missing, f"Missing columns: {missing}"

    def test_transformed_user_has_enriched_columns(self, transformed_users):
        required = ["full_name","age_group","is_high_value"]
        missing = check_column_exists(transformed_users, required)
        assert not missing, f"Missing enriched columns: {missing}"

    def test_user_id_is_integer(self, raw_users):
        assert pd.api.types.is_integer_dtype(raw_users["user_id"]), \
            "user_id must be integer type"

    def test_account_balance_is_numeric(self, raw_users):
        assert pd.api.types.is_numeric_dtype(raw_users["account_balance"]), \
            "account_balance must be numeric"

    def test_is_active_is_boolean(self, raw_users):
        assert pd.api.types.is_bool_dtype(raw_users["is_active"]), \
            "is_active must be boolean"


class TestTransactionSchema:

    def test_raw_transaction_has_required_columns(self, raw_transactions):
        required = ["transaction_id","user_id","amount","currency",
                    "transaction_type","status","created_at","fee"]
        missing = check_column_exists(raw_transactions, required)
        assert not missing, f"Missing columns: {missing}"

    def test_transformed_transaction_has_net_amount(self, transformed_transactions):
        assert "net_amount" in transformed_transactions.columns, \
            "net_amount column missing after transformation"

    def test_transformed_transaction_has_time_columns(self, transformed_transactions):
        missing = check_column_exists(transformed_transactions, ["month","year"])
        assert not missing, f"Missing time columns: {missing}"

    def test_amount_is_numeric(self, raw_transactions):
        assert pd.api.types.is_numeric_dtype(raw_transactions["amount"]), \
            "amount must be numeric"


class TestProductSchema:

    def test_raw_product_has_required_columns(self, raw_products):
        required = ["product_id","product_name","category","price",
                    "stock_quantity","discount_pct","final_price","is_available"]
        missing = check_column_exists(raw_products, required)
        assert not missing, f"Missing columns: {missing}"

    def test_transformed_product_has_savings_columns(self, transformed_products):
        required = ["savings","savings_pct","price_tier","in_stock"]
        missing = check_column_exists(transformed_products, required)
        assert not missing, f"Missing savings columns: {missing}"

    def test_price_is_numeric(self, raw_products):
        assert pd.api.types.is_numeric_dtype(raw_products["price"]), \
            "price must be numeric"
