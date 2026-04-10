# Data Pipeline Testing

A Python test suite validating schema, data quality, transformations, and pipeline integrity for a simulated ETL pipeline.

## Project Structure

```
├── pipeline/
│   ├── extractor.py       # Simulated data extraction (users, transactions, products)
│   └── transformer.py     # Data enrichment and transformation logic
├── utils/
│   └── validator.py       # Reusable data quality validation helpers
├── tests/
│   ├── test_schema_validation.py   # Column presence and type checks
│   ├── test_data_quality.py        # Nulls, duplicates, uniqueness, row counts
│   ├── test_transformations.py     # Business logic and transformation accuracy
│   └── test_pipeline_integrity.py  # Boundary, referential integrity, performance
├── conftest.py
├── pytest.ini
└── requirements.txt
```

## Setup

```powershell
pip install -r requirements.txt
```

## Running Tests

```powershell
python -m pytest tests/
```

With verbose output:

```powershell
python -m pytest tests/ -v
```

## CI

Tests run automatically on push and pull requests to `main` via GitHub Actions.
