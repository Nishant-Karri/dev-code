# QA Test Plan — `glue_json_to_parquet_v4.py`

**Version:** v4
**Date:** 2026-03-08
**Author:** Nishant-Karri
**Status:** In Review

---

## Overview

This document covers the test cases and QA task checklist for the AWS Glue ETL job `glue_json_to_parquet_v4.py`.
The job reads JSON from S3, selects `id`, `name`, `phone_number`, and `address` columns, writes Parquet output to a target S3 location, and runs `MSCK REPAIR TABLE` on the Athena target table.

---

## Test Environment

| Item | Details |
|------|---------|
| Runtime | AWS Glue 3.0+ (PySpark) |
| Python | 3.9+ |
| Test Framework | `pytest` + `unittest.mock` |
| Local Spark | `pyspark` (local mode) |
| AWS Mocking | `unittest.mock.patch("boto3.client")` |
| Run Command | `pytest test_glue_json_to_parquet_v4.py -v` |

---

## Section 1 — Column Selection Tests

### TC-01: Selected columns exist in output
- **Description:** Output DataFrame must contain exactly `id`, `name`, `phone_number`, `address`
- **Input:** Full valid records with all four columns
- **Expected:** `set(df.columns) == {"id", "name", "phone_number", "address"}`
- **Status:** ⬜ Not Run

---

### TC-02: Extra source columns are dropped
- **Description:** Columns beyond the four required (e.g. `email`, `age`) must not appear in output
- **Input:** Records with additional columns `email` and `age`
- **Expected:** `"email" not in df.columns` and `"age" not in df.columns`
- **Status:** ⬜ Not Run

---

### TC-03: Column order is correct
- **Description:** Output columns must follow the defined order
- **Input:** Full valid records
- **Expected:** `df.columns == ["id", "name", "phone_number", "address"]`
- **Status:** ⬜ Not Run

---

### TC-04: Missing required column raises error
- **Description:** Selecting against a DataFrame missing any required column must fail
- **Input:** DataFrame with only `id` and `name`
- **Expected:** `pyspark.sql.utils.AnalysisException` raised
- **Status:** ⬜ Not Run

---

### TC-05: Missing `phone_number` raises error
- **Description:** DataFrame missing `phone_number` must raise AnalysisException
- **Input:** DataFrame with `id`, `name`, `address` only
- **Expected:** `AnalysisException` raised
- **Status:** ⬜ Not Run

---

### TC-06: Missing `address` raises error
- **Description:** DataFrame missing `address` must raise AnalysisException
- **Input:** DataFrame with `id`, `name`, `phone_number` only
- **Expected:** `AnalysisException` raised
- **Status:** ⬜ Not Run

---

## Section 2 — Data Integrity Tests

### TC-07: Row count preserved after select
- **Description:** Row count must not change after column selection
- **Input:** 3 valid records
- **Expected:** `df.count() == 3`
- **Status:** ⬜ Not Run

---

### TC-08: Values preserved after select
- **Description:** Values in selected columns must match source data exactly
- **Input:** Records with known values
- **Expected:** Each field value matches the source record
- **Status:** ⬜ Not Run

---

### TC-09: Null `phone_number` is retained
- **Description:** Null values in `phone_number` must be preserved as `None`, not dropped
- **Input:** Record with `phone_number = None`
- **Expected:** `row["phone_number"] is None`
- **Status:** ⬜ Not Run

---

### TC-10: Null `address` is retained
- **Description:** Null values in `address` must be preserved as `None`, not dropped
- **Input:** Record with `address = None`
- **Expected:** `row["address"] is None`
- **Status:** ⬜ Not Run

---

### TC-11: Empty DataFrame produces correct schema
- **Description:** Empty input must produce zero-row output with correct column schema
- **Input:** Empty DataFrame with schema `id string, name string, phone_number string, address string`
- **Expected:** `df.count() == 0` and all four columns present
- **Status:** ⬜ Not Run

---

### TC-12: Single-row input produces single-row output
- **Description:** Single record input must produce exactly one output row with correct values
- **Input:** One record
- **Expected:** `result.count() == 1`, values match
- **Status:** ⬜ Not Run

---

### TC-13: Special characters in address are preserved
- **Description:** Addresses containing special characters must be stored as-is
- **Input:** `"Apt #4B, 100 O'Brien St., São Paulo — BR"`
- **Expected:** Value unchanged in output
- **Status:** ⬜ Not Run

---

### TC-14: Various phone number formats are preserved
- **Description:** Phone numbers in different formats must be stored without modification
- **Input:** `+1-555-010-1000`, `(555) 010-2000`, `5550103000`
- **Expected:** Each stored exactly as provided
- **Status:** ⬜ Not Run

---

## Section 3 — Athena MSCK REPAIR TABLE Tests

### TC-15: Correct SQL query is sent to Athena
- **Description:** `start_query_execution` must be called with the correct MSCK REPAIR TABLE SQL and parameters
- **Input:** `database = "my_db"`, `table = "my_table"`, `output_s3 = "s3://bucket/results/"`
- **Expected:** Query string contains `MSCK REPAIR TABLE \`my_db\`.\`my_table\``, correct database and output location
- **Status:** ⬜ Not Run

---

### TC-16: Polling continues until SUCCEEDED
- **Description:** `get_query_execution` must be called repeatedly until the state reaches a terminal value
- **Input:** Mock responses: `RUNNING` → `RUNNING` → `SUCCEEDED`
- **Expected:** `get_query_execution` called 3 times, final state is `SUCCEEDED`
- **Status:** ⬜ Not Run

---

### TC-17: RuntimeError raised on FAILED state
- **Description:** When Athena query state is `FAILED`, a `RuntimeError` must be raised with the reason
- **Input:** Mock response with state `FAILED`, reason `"Syntax error"`
- **Expected:** `RuntimeError` raised matching `"FAILED"`
- **Status:** ⬜ Not Run

---

### TC-18: RuntimeError raised on CANCELLED state
- **Description:** When Athena query state is `CANCELLED`, a `RuntimeError` must be raised
- **Input:** Mock response with state `CANCELLED`, reason `"User cancelled"`
- **Expected:** `RuntimeError` raised matching `"CANCELLED"`
- **Status:** ⬜ Not Run

---

### TC-19: Correct QueryExecutionId used for polling
- **Description:** The `QueryExecutionId` returned from `start_query_execution` must be used in all `get_query_execution` calls
- **Input:** Mock returns `QueryExecutionId = "exec-xyz-789"`
- **Expected:** `get_query_execution` called with `QueryExecutionId="exec-xyz-789"`
- **Status:** ⬜ Not Run

---

## Section 4 — Schema Tests

### TC-20: `id` column is StringType
- **Description:** The `id` column dtype must be `string`
- **Input:** Full valid records
- **Expected:** `dict(df.dtypes)["id"] == "string"`
- **Status:** ⬜ Not Run

---

### TC-21: `phone_number` column is StringType
- **Description:** The `phone_number` column dtype must be `string`
- **Input:** Full valid records
- **Expected:** `dict(df.dtypes)["phone_number"] == "string"`
- **Status:** ⬜ Not Run

---

### TC-22: `address` column is StringType
- **Description:** The `address` column dtype must be `string`
- **Input:** Full valid records
- **Expected:** `dict(df.dtypes)["address"] == "string"`
- **Status:** ⬜ Not Run

---

### TC-23: Large dataset row count preserved
- **Description:** Row count must be preserved for datasets with 1000+ rows
- **Input:** 1000 generated records
- **Expected:** `result.count() == 1000`
- **Status:** ⬜ Not Run

---

## QA Tasks

| Task ID | Task | Owner | Priority | Status |
|---------|------|-------|----------|--------|
| QA-01 | Set up local PySpark test environment and install `pytest` | QA Engineer | High | ⬜ To Do |
| QA-02 | Execute all 23 test cases in Section 1–4 and record results | QA Engineer | High | ⬜ To Do |
| QA-03 | Verify `phone_number` and `address` fields exist in source JSON test data | QA Engineer | High | ⬜ To Do |
| QA-04 | Test with real S3 source JSON containing all four required fields | QA Engineer | High | ⬜ To Do |
| QA-05 | Validate Parquet output in target S3 contains `phone_number` and `address` columns | QA Engineer | High | ⬜ To Do |
| QA-06 | Confirm Athena table schema reflects `phone_number` and `address` after MSCK REPAIR | QA Engineer | High | ⬜ To Do |
| QA-07 | Test with source JSON missing `phone_number` — confirm job fails gracefully | QA Engineer | Medium | ⬜ To Do |
| QA-08 | Test with source JSON missing `address` — confirm job fails gracefully | QA Engineer | Medium | ⬜ To Do |
| QA-09 | Test with null/empty `phone_number` and `address` values in source JSON | QA Engineer | Medium | ⬜ To Do |
| QA-10 | Verify Parquet compression is Snappy and Glue Parquet writer is used | QA Engineer | Medium | ⬜ To Do |
| QA-11 | Run job end-to-end in AWS Glue dev environment with sample dataset | QA Lead | High | ⬜ To Do |
| QA-12 | Confirm row counts match between source JSON and output Parquet | QA Lead | High | ⬜ To Do |
| QA-13 | Validate `MSCK REPAIR TABLE` refreshes Athena partitions correctly | QA Lead | Medium | ⬜ To Do |
| QA-14 | Performance test: run with large JSON file (>100k rows) and check completion time | QA Lead | Low | ⬜ To Do |
| QA-15 | Review and sign off on all test results before PR merge to main | QA Lead | High | ⬜ To Do |

---

## Test Status Summary

| Section | Total | Passed | Failed | Not Run |
|---------|-------|--------|--------|---------|
| Column Selection | 6 | 0 | 0 | 6 |
| Data Integrity | 8 | 0 | 0 | 8 |
| Athena MSCK REPAIR | 5 | 0 | 0 | 5 |
| Schema | 4 | 0 | 0 | 4 |
| **Total** | **23** | **0** | **0** | **23** |

---

## Sign-off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| QA Engineer | | | |
| QA Lead | | | |
| Developer | | | |
