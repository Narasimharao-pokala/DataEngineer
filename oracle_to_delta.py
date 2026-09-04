"""
Oracle to Delta Lake PySpark Ingestion Pipeline.

Supports:
- Optimized parallel JDBC extraction from Oracle with custom fetchsize and partition slicing.
- Flexible write modes: Overwrite, Append, and Upsert (Delta MERGE).
- Databricks Secret Scope credential retrieval with environment variable fallbacks.
- Audit columns (_ingested_at, _source_table).
- Executable via Databricks Workflows, Jobs, or locally via Databricks Connect.
"""

import argparse
from datetime import datetime
import os
import sys
from typing import Dict, List, Optional
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

load_dotenv()


def get_spark_session(app_name: str = "OracleToDeltaPipeline") -> SparkSession:
    """Initializes SparkSession via Databricks Connect or standard cluster session."""
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.appName(app_name).getOrCreate()
    except Exception:
        return SparkSession.builder.appName(app_name).getOrCreate()


def get_secret(spark: SparkSession, scope: str, key: str, fallback_env_var: Optional[str] = None) -> str:
    """
    Retrieves secret from Databricks Secrets scope, falling back to os.environ.
    """
    try:
        # Check if dbutils is available in Spark runtime
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        pass

    # Try databricks.sdk / dbutils fallback
    if fallback_env_var and os.getenv(fallback_env_var):
        return os.environ[fallback_env_var]

    raise ValueError(
        f"Could not retrieve secret key '{key}' from scope '{scope}' "
        f"and fallback env var '{fallback_env_var}' is not set."
    )


def build_oracle_jdbc_url(host: str, port: int, service_name: Optional[str] = None, sid: Optional[str] = None) -> str:
    """Constructs Oracle thin JDBC connection URL."""
    if service_name:
        return f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    elif sid:
        return f"jdbc:oracle:thin:@{host}:{port}:{sid}"
    else:
        raise ValueError("Either 'service_name' or 'sid' must be provided for Oracle JDBC connection.")


def read_from_oracle(
    spark: SparkSession,
    jdbc_url: str,
    user: str,
    password: str,
    source_table_or_query: str,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: int = 10,
    fetch_size: int = 50000,
    custom_options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """
    Reads data from Oracle database using PySpark JDBC reader with high-throughput optimizations.
    """
    print(f"[INFO] Reading from Oracle using JDBC URL: {jdbc_url.split('@')[0]}@...")

    reader = (
        spark.read.format("jdbc")
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("url", jdbc_url)
        .option("user", user)
        .option("password", password)
        .option("fetchsize", str(fetch_size))
        .option("sessionInitStatement", "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
    )

    # Determine whether reading a table or custom query
    clean_source = source_table_or_query.strip()
    if clean_source.upper().startswith("SELECT"):
        reader = reader.option("dbtable", f"({clean_source}) src_subq")
    else:
        reader = reader.option("dbtable", clean_source)

    # Configure parallel partition reading if partition parameters are provided
    if partition_column and lower_bound is not None and upper_bound is not None:
        print(
            f"[INFO] Enabling parallel read on column '{partition_column}' "
            f"range [{lower_bound} to {upper_bound}] across {num_partitions} partitions."
        )
        reader = (
            reader.option("partitionColumn", partition_column)
            .option("lowerBound", str(lower_bound))
            .option("upperBound", str(upper_bound))
            .option("numPartitions", str(num_partitions))
        )

    if custom_options:
        for k, v in custom_options.items():
            reader = reader.option(k, v)

    df = reader.load()
    return df


def add_audit_metadata(df: DataFrame, source_name: str) -> DataFrame:
    """Enriches the DataFrame with ingestion audit columns."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(source_name))
    )


def write_to_delta(
    df: DataFrame,
    target_table_or_path: str,
    mode: str = "append",
    primary_keys: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
    is_path: bool = False,
    spark: Optional[SparkSession] = None,
) -> None:
    """
    Writes DataFrame to Delta Lake with support for Append, Overwrite, and Upsert (MERGE).
    """
    mode = mode.lower()
    print(f"[INFO] Writing to Delta Lake: '{target_table_or_path}' with mode '{mode}'")

    if mode == "merge":
        if not primary_keys:
            raise ValueError("Primary keys must be specified when write mode is 'merge'.")

        merge_to_delta(
            df=df,
            target_table_or_path=target_table_or_path,
            primary_keys=primary_keys,
            is_path=is_path,
            spark=spark,
            partition_by=partition_by,
        )
        return

    # Standard append or overwrite
    writer = (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if is_path:
        writer.save(target_table_or_path)
    else:
        writer.saveAsTable(target_table_or_path)

    print(f"[SUCCESS] Data successfully written to Delta table '{target_table_or_path}'.")


def merge_to_delta(
    df: DataFrame,
    target_table_or_path: str,
    primary_keys: List[str],
    is_path: bool = False,
    spark: Optional[SparkSession] = None,
    partition_by: Optional[List[str]] = None,
) -> None:
    """
    Performs an upsert (MERGE INTO) on the target Delta table.
    If the target table does not exist, creates it as the initial load.
    """
    from delta.tables import DeltaTable

    active_spark = spark or df.sparkSession
    table_exists = False

    try:
        if is_path:
            target_table = DeltaTable.forPath(active_spark, target_table_or_path)
            table_exists = True
        else:
            target_table = DeltaTable.forName(active_spark, target_table_or_path)
            table_exists = True
    except Exception:
        print(f"[INFO] Target Delta table '{target_table_or_path}' does not exist. Performing initial load...")
        writer = df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if is_path:
            writer.save(target_table_or_path)
        else:
            writer.saveAsTable(target_table_or_path)
        print(f"[SUCCESS] Created target Delta table '{target_table_or_path}'.")
        return

    # Build merge condition on primary keys: "target.id = source.id AND target.dept = source.dept"
    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_keys])
    print(f"[INFO] Merging on condition: {merge_condition}")

    (
        target_table.alias("target")
        .merge(df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[SUCCESS] Upsert (MERGE) completed successfully on '{target_table_or_path}'.")


def run_pipeline(
    host: str,
    port: int,
    user: str,
    password: str,
    source_table_or_query: str,
    target_delta_table: str,
    service_name: Optional[str] = None,
    sid: Optional[str] = None,
    mode: str = "append",
    primary_keys: Optional[List[str]] = None,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: int = 10,
    fetch_size: int = 50000,
    is_delta_path: bool = False,
) -> None:
    """Executes the end-to-end Oracle to Delta Lake pipeline."""
    spark = get_spark_session(app_name=f"OracleToDelta_{target_delta_table}")

    jdbc_url = build_oracle_jdbc_url(host=host, port=port, service_name=service_name, sid=sid)

    # 1. Extract from Oracle
    source_df = read_from_oracle(
        spark=spark,
        jdbc_url=jdbc_url,
        user=user,
        password=password,
        source_table_or_query=source_table_or_query,
        partition_column=partition_column,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        num_partitions=num_partitions,
        fetch_size=fetch_size,
    )

    # 2. Enrich with audit metadata
    enriched_df = add_audit_metadata(source_df, source_name=source_table_or_query)

    # 3. Load into Delta Lake
    write_to_delta(
        df=enriched_df,
        target_table_or_path=target_delta_table,
        mode=mode,
        primary_keys=primary_keys,
        is_path=is_delta_path,
        spark=spark,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Oracle to Delta Lake PySpark Ingestion Pipeline")
    parser.add_argument("--host", default=os.getenv("ORACLE_HOST", "localhost"), help="Oracle DB Host")
    parser.add_argument("--port", type=int, default=int(os.getenv("ORACLE_PORT", "1521")), help="Oracle Port")
    parser.add_argument("--service-name", default=os.getenv("ORACLE_SERVICE_NAME"), help="Oracle Service Name")
    parser.add_argument("--sid", default=os.getenv("ORACLE_SID"), help="Oracle SID")
    parser.add_argument("--user", default=os.getenv("ORACLE_USER"), help="Oracle Username")
    parser.add_argument("--password", default=os.getenv("ORACLE_PASSWORD"), help="Oracle Password")
    parser.add_argument("--source", required=True, help="Oracle source table (e.g. HR.EMPLOYEES) or SQL query")
    parser.add_argument("--target", required=True, help="Target Delta table name or path")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite", "merge"], help="Write mode")
    parser.add_argument("--primary-keys", nargs="*", default=None, help="Primary key columns for merge mode")
    parser.add_argument("--partition-col", default=None, help="Oracle column for parallel JDBC partitioning")
    parser.add_argument("--lower-bound", type=int, default=None, help="Lower bound value for partition column")
    parser.add_argument("--upper-bound", type=int, default=None, help="Upper bound value for partition column")
    parser.add_argument("--num-partitions", type=int, default=10, help="Number of parallel JDBC connections")
    parser.add_argument("--fetch-size", type=int, default=50000, help="JDBC fetch size row buffer")
    parser.add_argument("--is-path", action="store_true", help="Treat target as storage path instead of catalog table")

    args = parser.parse_args()

    run_pipeline(
        host=args.host,
        port=args.port,
        service_name=args.service_name,
        sid=args.sid,
        user=args.user,
        password=args.password,
        source_table_or_query=args.source,
        target_delta_table=args.target,
        mode=args.mode,
        primary_keys=args.primary_keys,
        partition_column=args.partition_col,
        lower_bound=args.lower_bound,
        upper_bound=args.upper_bound,
        num_partitions=args.num_partitions,
        fetch_size=args.fetch_size,
        is_delta_path=args.is_path,
    )
