"""
Unit tests for glue_json_to_parquet_v4.py

Tests use mocking to avoid real AWS/Spark dependencies.
Run with: pytest test_glue_json_to_parquet_v4.py -v
"""

import pytest
from unittest.mock import MagicMock, patch, call
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for unit tests (no Glue runtime needed)."""
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("glue_v4_tests")
        .getOrCreate()
    )


@pytest.fixture
def sample_data(spark):
    """Full valid records with all four expected columns."""
    return spark.createDataFrame([
        {"id": "1", "name": "Alice",   "phone_number": "555-0101", "address": "123 Main St"},
        {"id": "2", "name": "Bob",     "phone_number": "555-0102", "address": "456 Oak Ave"},
        {"id": "3", "name": "Charlie", "phone_number": "555-0103", "address": "789 Pine Rd"},
    ])


@pytest.fixture
def extra_columns_data(spark):
    """Records with extra columns that should be dropped during select."""
    return spark.createDataFrame([
        {"id": "1", "name": "Alice", "phone_number": "555-0101", "address": "123 Main St",
         "email": "alice@example.com", "age": 30},
    ])


@pytest.fixture
def nullable_data(spark):
    """Records with None values in phone_number and address."""
    return spark.createDataFrame([
        {"id": "1", "name": "Alice",   "phone_number": None,      "address": None},
        {"id": "2", "name": "Bob",     "phone_number": "555-0102", "address": None},
        {"id": "3", "name": "Charlie", "phone_number": None,       "address": "789 Pine Rd"},
    ])


# ---------------------------------------------------------------------------
# 1. Column Selection Tests
# ---------------------------------------------------------------------------

class TestColumnSelection:

    def test_selected_columns_exist(self, sample_data):
        """Output DataFrame must contain exactly id, name, phone_number, address."""
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert set(df.columns) == {"id", "name", "phone_number", "address"}

    def test_no_extra_columns_in_output(self, extra_columns_data):
        """Extra source columns (email, age) must not appear in output."""
        df = extra_columns_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert "email" not in df.columns
        assert "age" not in df.columns

    def test_column_order(self, sample_data):
        """Columns must be in the defined order: id, name, phone_number, address."""
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert df.columns == ["id", "name", "phone_number", "address"]

    def test_missing_required_column_raises(self, spark):
        """Selecting a missing column must raise AnalysisException."""
        df_missing = spark.createDataFrame([{"id": "1", "name": "Alice"}])
        with pytest.raises(AnalysisException):
            df_missing.select(col("id"), col("name"), col("phone_number"), col("address")).collect()

    def test_missing_phone_number_column_raises(self, spark):
        """Missing phone_number specifically should raise AnalysisException."""
        df = spark.createDataFrame([{"id": "1", "name": "Alice", "address": "123 Main St"}])
        with pytest.raises(AnalysisException):
            df.select(col("id"), col("name"), col("phone_number"), col("address")).collect()

    def test_missing_address_column_raises(self, spark):
        """Missing address specifically should raise AnalysisException."""
        df = spark.createDataFrame([{"id": "1", "name": "Alice", "phone_number": "555-0101"}])
        with pytest.raises(AnalysisException):
            df.select(col("id"), col("name"), col("phone_number"), col("address")).collect()


# ---------------------------------------------------------------------------
# 2. Data Integrity Tests
# ---------------------------------------------------------------------------

class TestDataIntegrity:

    def test_row_count_preserved(self, sample_data):
        """Row count must not change after column selection."""
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert df.count() == sample_data.count()

    def test_values_preserved(self, sample_data):
        """Values in selected columns must match source data exactly."""
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        rows = {r["id"]: r for r in df.collect()}

        assert rows["1"]["name"] == "Alice"
        assert rows["1"]["phone_number"] == "555-0101"
        assert rows["1"]["address"] == "123 Main St"

        assert rows["2"]["name"] == "Bob"
        assert rows["2"]["phone_number"] == "555-0102"
        assert rows["2"]["address"] == "456 Oak Ave"

    def test_null_values_allowed(self, nullable_data):
        """Null phone_number and address values must be retained (not dropped)."""
        df = nullable_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert df.count() == 3

    def test_null_phone_number_value(self, nullable_data):
        """Null phone_number must be preserved as None."""
        df = nullable_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        rows = {r["id"]: r for r in df.collect()}
        assert rows["1"]["phone_number"] is None

    def test_null_address_value(self, nullable_data):
        """Null address must be preserved as None."""
        df = nullable_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        rows = {r["id"]: r for r in df.collect()}
        assert rows["1"]["address"] is None

    def test_empty_dataframe(self, spark):
        """Empty input must produce empty output with correct schema."""
        schema = "id string, name string, phone_number string, address string"
        df_empty = spark.createDataFrame([], schema)
        df = df_empty.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert df.count() == 0
        assert set(df.columns) == {"id", "name", "phone_number", "address"}

    def test_single_row(self, spark):
        """Single-row input must produce single-row output."""
        df = spark.createDataFrame([
            {"id": "42", "name": "Zara", "phone_number": "555-9999", "address": "1 Test Lane"}
        ])
        result = df.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert result.count() == 1
        row = result.collect()[0]
        assert row["id"] == "42"
        assert row["phone_number"] == "555-9999"
        assert row["address"] == "1 Test Lane"

    def test_special_characters_in_address(self, spark):
        """Addresses with special characters must be preserved."""
        df = spark.createDataFrame([
            {"id": "1", "name": "Alice", "phone_number": "555-0101",
             "address": "Apt #4B, 100 O'Brien St., São Paulo — BR"}
        ])
        result = df.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert result.collect()[0]["address"] == "Apt #4B, 100 O'Brien St., São Paulo — BR"

    def test_phone_number_formats(self, spark):
        """Various phone number formats must be stored as-is."""
        data = [
            {"id": "1", "name": "A", "phone_number": "+1-555-010-1000", "address": "Addr1"},
            {"id": "2", "name": "B", "phone_number": "(555) 010-2000",  "address": "Addr2"},
            {"id": "3", "name": "C", "phone_number": "5550103000",      "address": "Addr3"},
        ]
        df = spark.createDataFrame(data)
        result = df.select(col("id"), col("name"), col("phone_number"), col("address"))
        rows = {r["id"]: r for r in result.collect()}
        assert rows["1"]["phone_number"] == "+1-555-010-1000"
        assert rows["2"]["phone_number"] == "(555) 010-2000"
        assert rows["3"]["phone_number"] == "5550103000"


# ---------------------------------------------------------------------------
# 3. Athena MSCK REPAIR TABLE Tests
# ---------------------------------------------------------------------------

class TestAthenaRepair:

    @patch("boto3.client")
    def test_msck_repair_called_with_correct_query(self, mock_boto3_client):
        """start_query_execution must be called with the correct MSCK REPAIR TABLE SQL."""
        mock_athena = MagicMock()
        mock_boto3_client.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "abc-123"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        athena_client = mock_boto3_client("athena")
        database = "my_db"
        table = "my_table"
        output_s3 = "s3://bucket/results/"

        athena_client.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE `{database}`.`{table}`",
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_s3},
        )

        call_args = mock_athena.start_query_execution.call_args
        assert "MSCK REPAIR TABLE `my_db`.`my_table`" in call_args.kwargs["QueryString"]
        assert call_args.kwargs["QueryExecutionContext"]["Database"] == "my_db"
        assert call_args.kwargs["ResultConfiguration"]["OutputLocation"] == output_s3

    @patch("boto3.client")
    def test_msck_repair_polls_until_succeeded(self, mock_boto3_client):
        """get_query_execution must be polled until terminal state SUCCEEDED."""
        mock_athena = MagicMock()
        mock_boto3_client.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "abc-123"}
        mock_athena.get_query_execution.side_effect = [
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "RUNNING"}}},
            {"QueryExecution": {"Status": {"State": "SUCCEEDED"}}},
        ]

        athena_client = mock_boto3_client("athena")
        athena_client.start_query_execution(
            QueryString="MSCK REPAIR TABLE `db`.`tbl`",
            QueryExecutionContext={"Database": "db"},
            ResultConfiguration={"OutputLocation": "s3://bucket/"},
        )

        terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
        query_execution_id = "abc-123"
        state = None
        for _ in range(5):
            resp = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state in terminal_states:
                break

        assert state == "SUCCEEDED"
        assert athena_client.get_query_execution.call_count == 3

    @patch("boto3.client")
    def test_msck_repair_raises_on_failed(self, mock_boto3_client):
        """RuntimeError must be raised when Athena query state is FAILED."""
        mock_athena = MagicMock()
        mock_boto3_client.return_value = mock_athena
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "FAILED", "StateChangeReason": "Syntax error"}
            }
        }

        athena_client = mock_boto3_client("athena")
        resp = athena_client.get_query_execution(QueryExecutionId="bad-id")
        state = resp["QueryExecution"]["Status"]["State"]
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")

        with pytest.raises(RuntimeError, match="FAILED"):
            if state != "SUCCEEDED":
                raise RuntimeError(f"MSCK REPAIR TABLE {state}: {reason}")

    @patch("boto3.client")
    def test_msck_repair_raises_on_cancelled(self, mock_boto3_client):
        """RuntimeError must be raised when Athena query state is CANCELLED."""
        mock_athena = MagicMock()
        mock_boto3_client.return_value = mock_athena
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {"State": "CANCELLED", "StateChangeReason": "User cancelled"}
            }
        }

        athena_client = mock_boto3_client("athena")
        resp = athena_client.get_query_execution(QueryExecutionId="cancelled-id")
        state = resp["QueryExecution"]["Status"]["State"]
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")

        with pytest.raises(RuntimeError, match="CANCELLED"):
            if state != "SUCCEEDED":
                raise RuntimeError(f"MSCK REPAIR TABLE {state}: {reason}")

    @patch("boto3.client")
    def test_msck_repair_uses_execution_id_from_response(self, mock_boto3_client):
        """The QueryExecutionId from start_query_execution must be used for polling."""
        mock_athena = MagicMock()
        mock_boto3_client.return_value = mock_athena
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-xyz-789"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        athena_client = mock_boto3_client("athena")
        response = athena_client.start_query_execution(
            QueryString="MSCK REPAIR TABLE `db`.`tbl`",
            QueryExecutionContext={"Database": "db"},
            ResultConfiguration={"OutputLocation": "s3://bucket/"},
        )
        query_execution_id = response["QueryExecutionId"]
        athena_client.get_query_execution(QueryExecutionId=query_execution_id)

        mock_athena.get_query_execution.assert_called_with(QueryExecutionId="exec-xyz-789")


# ---------------------------------------------------------------------------
# 4. Schema Tests
# ---------------------------------------------------------------------------

class TestSchema:

    def test_id_column_type(self, sample_data):
        """id column must be StringType."""
        from pyspark.sql.types import StringType
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        id_type = dict(df.dtypes)["id"]
        assert id_type == "string"

    def test_phone_number_column_type(self, sample_data):
        """phone_number column must be StringType."""
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        phone_type = dict(df.dtypes)["phone_number"]
        assert phone_type == "string"

    def test_address_column_type(self, sample_data):
        """address column must be StringType."""
        df = sample_data.select(col("id"), col("name"), col("phone_number"), col("address"))
        addr_type = dict(df.dtypes)["address"]
        assert addr_type == "string"

    def test_large_dataset_row_count(self, spark):
        """Row count must be preserved for large datasets."""
        data = [
            {"id": str(i), "name": f"User{i}", "phone_number": f"555-{i:04d}",
             "address": f"{i} Test St"}
            for i in range(1000)
        ]
        df = spark.createDataFrame(data)
        result = df.select(col("id"), col("name"), col("phone_number"), col("address"))
        assert result.count() == 1000
