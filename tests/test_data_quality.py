"""
Data quality tests — nulls, duplicates, uniqueness, row counts.
"""
import pytest
from pipeline.extractor import (
    extract_user_data,
    extract_transaction_data,
    extract_product_data,
)
from utils.validator import (
    check_no_nulls,
    check_unique,
    check_no_duplicates,
    check_row_count,
)


@pytest.fixture(scope="module")
def users():
    return extract_user_data(100)

@pytest.fixture(scope="module")
def transactions():
    return extract_transaction_data(200)

@pytest.fixture(scope="module")
def products():
    return extract_product_data(50)


class TestNullChecks:

    def test_user_critical_fields_have_no_nulls(self, users):
        nulls = check_no_nulls(users, ["user_id","email","account_balance","is_active"])
        assert all(v == 0 for v in nulls.values()), \
            f"Null values found in critical user fields: {nulls}"

    def test_transaction_critical_fields_have_no_nulls(self, transactions):
        nulls = check_no_nulls(transactions, ["transaction_id","user_id","amount","status"])
        assert all(v == 0 for v in nulls.values()), \
            f"Null values found in critical transaction fields: {nulls}"

    def test_product_critical_fields_have_no_nulls(self, products):
        nulls = check_no_nulls(products, ["product_id","product_name","price","category"])
        assert all(v == 0 for v in nulls.values()), \
            f"Null values found in critical product fields: {nulls}"


class TestUniqueness:

    def test_user_ids_are_unique(self, users):
        assert check_unique(users, "user_id"), \
            "Duplicate user_id values found"

    def test_transaction_ids_are_unique(self, transactions):
        assert check_unique(transactions, "transaction_id"), \
            "Duplicate transaction_id values found"

    def test_product_ids_are_unique(self, products):
        assert check_unique(products, "product_id"), \
            "Duplicate product_id values found"

    def test_user_emails_are_unique(self, users):
        assert check_unique(users, "email"), \
            "Duplicate email addresses found in user data"


class TestDuplicates:

    def test_no_duplicate_user_records(self, users):
        dupes = check_no_duplicates(users, ["user_id","email"])
        assert dupes == 0, f"{dupes} duplicate user records found"

    def test_no_duplicate_transaction_records(self, transactions):
        dupes = check_no_duplicates(transactions, ["transaction_id"])
        assert dupes == 0, f"{dupes} duplicate transaction records found"

    def test_no_duplicate_product_records(self, products):
        dupes = check_no_duplicates(products, ["product_id"])
        assert dupes == 0, f"{dupes} duplicate product records found"


class TestRowCounts:

    def test_user_row_count_matches_expected(self, users):
        assert check_row_count(users, 100), \
            f"Expected ~100 user rows, got {len(users)}"

    def test_transaction_row_count_matches_expected(self, transactions):
        assert check_row_count(transactions, 200), \
            f"Expected ~200 transaction rows, got {len(transactions)}"

    def test_product_row_count_matches_expected(self, products):
        assert check_row_count(products, 50), \
            f"Expected ~50 product rows, got {len(products)}"
