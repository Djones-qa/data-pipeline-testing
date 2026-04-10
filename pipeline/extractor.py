"""
Simulates extracting raw data from a source system.
In production this would connect to a real database or API.
"""
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


def extract_user_data(num_records: int = 100) -> pd.DataFrame:
    """Simulate extracting user records from a source system."""
    records = []
    for i in range(1, num_records + 1):
        records.append({
            "user_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "age": random.randint(18, 75),
            "country": random.choice(["US", "UK", "CA", "AU", "DE"]),
            "signup_date": fake.date_between(start_date="-2y", end_date="today"),
            "account_balance": round(random.uniform(0, 50000), 2),
            "is_active": random.choice([True, False]),
        })
    return pd.DataFrame(records)


def extract_transaction_data(num_records: int = 200) -> pd.DataFrame:
    """Simulate extracting transaction records from a source system."""
    records = []
    for i in range(1, num_records + 1):
        amount = round(random.uniform(1, 5000), 2)
        records.append({
            "transaction_id": i,
            "user_id": random.randint(1, 100),
            "amount": amount,
            "currency": random.choice(["USD", "GBP", "EUR", "CAD"]),
            "transaction_type": random.choice(["credit", "debit"]),
            "status": random.choice(["completed", "pending", "failed"]),
            "created_at": fake.date_time_between(
                start_date="-1y", end_date="now"
            ).isoformat(),
            "fee": round(amount * 0.02, 2),
        })
    return pd.DataFrame(records)


def extract_product_data(num_records: int = 50) -> pd.DataFrame:
    """Simulate extracting product catalog records."""
    categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
    records = []
    for i in range(1, num_records + 1):
        price = round(random.uniform(5, 999), 2)
        records.append({
            "product_id": i,
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "price": price,
            "stock_quantity": random.randint(0, 500),
            "discount_pct": round(random.uniform(0, 0.40), 2),
            "final_price": round(price * (1 - random.uniform(0, 0.40)), 2),
            "is_available": random.choice([True, False]),
            "created_at": fake.date_between(
                start_date="-3y", end_date="today"
            ).isoformat(),
        })
    return pd.DataFrame(records)
