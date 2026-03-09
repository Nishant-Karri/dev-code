# dev-code

AWS Glue ETL job that reads JSON from S3, selects key columns, writes Parquet output, and refreshes Athena table partitions.

---

## Repository Structure

```
dev-code/
├── glue_json_to_parquet_v3.py        # Production-ready ETL (id, name)
├── glue_json_to_parquet_v4.py        # Latest ETL — adds phone_number, address
├── QA_Test_Plan_glue_json_to_parquet_v4.md  # Full QA test plan & task checklist
└── test_data/
    ├── sample_data_TC-01.json        # TC-01: Full valid records (1000)
    ├── sample_data_TC-02.json        # TC-02: Records with extra columns
    ├── sample_data_TC-03.json        # TC-03: Column order verification
    ├── sample_data_TC-04.json        # TC-04: Missing phone_number & address
    ├── sample_data_TC-05.json        # TC-05: Missing phone_number
    ├── sample_data_TC-06.json        # TC-06: Missing address
    ├── sample_data_TC-07.json        # TC-07: 3-record row count check
    ├── sample_data_TC-08.json        # TC-08: Known fixed values
    ├── sample_data_TC-09.json        # TC-09: Null phone_number
    ├── sample_data_TC-10.json        # TC-10: Null address
    ├── sample_data_TC-11.json        # TC-11: Empty dataset
    ├── sample_data_TC-12.json        # TC-12: Single record
    ├── sample_data_TC-13.json        # TC-13: Special characters in address
    ├── sample_data_TC-14.json        # TC-14: Various phone number formats
    └── sample_data_TC-15 to TC-23   # Athena, schema, and large dataset tests
```

---

## Branches

| Branch | Purpose |
|--------|---------|
| `main` | Stable base — `glue_json_to_parquet_v3.py` |
| `dev` | Active development — v4 glue code, QA doc, test datasets |
| `production` | Production release — `glue_json_to_parquet_v3.py` only |

---

## Glue Job — v4 (Latest)

**File:** `glue_json_to_parquet_v4.py`

Reads JSON from S3 and extracts the following columns into Parquet:

| Column | Type | Description |
|--------|------|-------------|
| `id` | string | Unique record identifier |
| `name` | string | Full name |
| `phone_number` | string | Contact phone number |
| `address` | string | Full address |

### Job Parameters

| Parameter | Example | Description |
|-----------|---------|-------------|
| `--source_s3_path` | `s3://my-bucket/raw/data.json` | S3 path to input JSON |
| `--target_s3_path` | `s3://my-bucket/processed/` | S3 path for Parquet output |
| `--athena_database` | `my_database` | Athena database name |
| `--athena_table` | `my_table` | Athena table name |
| `--athena_output_s3` | `s3://my-bucket/athena-results/` | S3 path for Athena query results |

### What it does

1. Reads JSON (multi-line or JSON-Lines) from S3
2. Selects `id`, `name`, `phone_number`, `address` columns
3. Writes Parquet with Snappy compression to target S3
4. Runs `MSCK REPAIR TABLE` on Athena to refresh partition metadata

---

## QA Test Plan

**File:** `QA_Test_Plan_glue_json_to_parquet_v4.md`

Covers 23 test cases across 4 sections and 15 QA tasks:

| Section | Test Cases |
|---------|-----------|
| Column Selection | TC-01 to TC-06 |
| Data Integrity | TC-07 to TC-14 |
| Athena MSCK REPAIR TABLE | TC-15 to TC-19 |
| Schema | TC-20 to TC-23 |

---

## Test Data

23 individual JSON datasets in `test_data/`, one per test case. Each file targets a specific scenario (nulls, missing columns, special characters, phone formats, empty input, etc.).

---

## Pull Requests

| PR | Title | From | To | Status |
|----|-------|------|----|--------|
| [#1](https://github.com/Nishant-Karri/dev-code/pull/1) | feat: add phone_number and address columns (v4) | `dev` | `main` | Open |
