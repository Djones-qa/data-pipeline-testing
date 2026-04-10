"""
Boundary, range, referential integrity, and pipeline performance tests.
"""
import pytest
import time
import pandas as pd
from pipeline.extractor import extract_user_data, extract_transaction_data, extract_product_data
from pipeline.transformer import transform_user_data, transform_transaction_data, transform_product_data
from utils.validator import check_value_range, check_referential_integrity


@pytest.fixture(scope="module")
def users():
    return extract_user_data(100)

@pytest.fixture(scope="module")
def transactions():
    return extract_transaction_data(200)

@pytest.fixture(scope="module")
def products():
    return extract_product_data(50)


class TestBoundaryValidation:

    def test_user_age_within_valid_range(self, users):
        result = check_value_range(users, "age", 18, 100)
        assert result["below_min"] == 0, f"{result['below_min']} users under age 18"
        assert result["above_max"] == 0, f"{result['above_max']} users over age 100"

    def test_account_balance_is_non_negative(self, users):
        result = check_value_range(users, "account_balance", 0, 1_000_000)
        assert result["below_min"] == 0, \
            f"{result['below_min']} users with negative balance"

    def test_transaction_amount_is_positive(self, transactions):
        result = check_value_range(transactions, "amount", 0.01, 100_000)
        assert result["below_min"] == 0, \
            f"{result['below_min']} transactions with non-positive amount"

    def test_transaction_fee_is_non_negative(self, transactions):
        result = check_value_range(transactions, "fee", 0, 10_000)
        assert result["below_min"] == 0, \
            f"{result['below_min']} transactions with negative fee"

    def test_product_price_is_positive(self, products):
        result = check_value_range(products, "price", 0.01, 100_000)
        assert result["below_min"] == 0, \
            f"{result['below_min']} products with non-positive price"

    def test_discount_pct_within_range(self, products):
        result = check_value_range(products, "discount_pct", 0, 1)
        assert result["below_min"] == 0, "Negative discount percentages found"
        assert result["above_max"] == 0, "Discount percentages above 100% found"

    def test_stock_quantity_is_non_negative(self, products):
        result = check_value_range(products, "stock_quantity", 0, 10_000)
        assert result["below_min"] == 0, \
            f"{result['below_min']} products with negative stock"


class TestReferentialIntegrity:

    def test_transaction_user_ids_exist_in_users(self, users, transactions):
        orphans = check_referential_integrity(
            transactions, users, "user_id", "user_id"
        )
        assert orphans == 0, \
            f"{orphans} transactions reference non-existent user_ids"

    def test_all_countries_are_valid(self, users):
        valid_countries = {"US", "UK", "CA", "AU", "DE"}
        invalid = ~users["country"].isin(valid_countries)
        assert invalid.sum() == 0, \
            f"Invalid country codes found: {users[invalid]['country'].unique()}"

    def test_transaction_status_values_are_valid(self, transactions):
        valid_statuses = {"completed", "pending", "failed"}
        invalid = ~transactions["status"].isin(valid_statuses)
        assert invalid.sum() == 0, \
            f"Invalid status values: {transactions[invalid]['status'].unique()}"

    def test_transaction_type_values_are_valid(self, transactions):
        valid_types = {"credit", "debit"}
        invalid = ~transactions["transaction_type"].isin(valid_types)
        assert invalid.sum() == 0, \
            f"Invalid transaction types: {transactions[invalid]['transaction_type'].unique()}"

    def test_currency_values_are_valid(self, transactions):
        valid_currencies = {"USD", "GBP", "EUR", "CAD"}
        invalid = ~transactions["currency"].isin(valid_currencies)
        assert invalid.sum() == 0, \
            f"Invalid currencies: {transactions[invalid]['currency'].unique()}"


class TestPipelinePerformance:

    def test_user_extraction_completes_within_threshold(self):
        start = time.time()
        extract_user_data(1000)
        duration = time.time() - start
        assert duration < 5.0, \
            f"User extraction took {duration:.2f}s — exceeds 5s threshold"

    def test_transaction_extraction_completes_within_threshold(self):
        start = time.time()
        extract_transaction_data(2000)
        duration = time.time() - start
        assert duration < 5.0, \
            f"Transaction extraction took {duration:.2f}s — exceeds 5s threshold"

    def test_user_transformation_completes_within_threshold(self):
        df = extract_user_data(1000)
        start = time.time()
        transform_user_data(df)
        duration = time.time() - start
        assert duration < 3.0, \
            f"User transformation took {duration:.2f}s — exceeds 3s threshold"

    def test_transaction_transformation_completes_within_threshold(self):
        df = extract_transaction_data(2000)
        start = time.time()
        transform_transaction_data(df)
        duration = time.time() - start
        assert duration < 3.0, \
            f"Transaction transformation took {duration:.2f}s — exceeds 3s threshold"

    def test_full_pipeline_completes_within_threshold(self):
        start = time.time()
        users = extract_user_data(100)
        transactions = extract_transaction_data(200)
        products = extract_product_data(50)
        transform_user_data(users)
        transform_transaction_data(transactions)
        transform_product_data(products)
        duration = time.time() - start
        assert duration < 10.0, \
            f"Full pipeline took {duration:.2f}s — exceeds 10s threshold"
