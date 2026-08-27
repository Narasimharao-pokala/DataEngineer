"""Load a file into a Databricks Delta table.

Update SOURCE_PATH and TARGET_TABLE before running this as a Databricks job.
Supported input formats: csv, json, parquet.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SOURCE_PATH = "/Volumes/catalog/schema/volume/emp.csv"
SOURCE_FORMAT = "csv"
TARGET_TABLE = "catalog.schema.emp_bronze"
WRITE_MODE = "overwrite"
INFER_SCHEMA = False


EMPLOYEE_SCHEMA = StructType(
    [
        StructField("EMP_ID", IntegerType(), nullable=False),
        StructField("FIRST_NAME", StringType(), nullable=True),
        StructField("LAST_NAME", StringType(), nullable=True),
        StructField("DEPT", StringType(), nullable=True),
        StructField("SAL", IntegerType(), nullable=True),
    ]
)


def read_source_file(spark: SparkSession) -> DataFrame:
    """Read the configured source file into a DataFrame."""
    reader = spark.read.format(SOURCE_FORMAT)

    if SOURCE_FORMAT == "csv":
        reader = reader.option("header", "true").option("nullValue", "NULL")

    if INFER_SCHEMA:
        reader = reader.option("inferSchema", "true")
    else:
        reader = reader.schema(EMPLOYEE_SCHEMA)

    return reader.load(SOURCE_PATH)


def main() -> None:
    spark = SparkSession.builder.appName("LoadFileToDatabricks").getOrCreate()

    try:
        source_df = read_source_file(spark)
        source_df.write.format("delta").mode(WRITE_MODE).option(
            "mergeSchema", "true"
        ).saveAsTable(TARGET_TABLE)

        print(f"Loaded {source_df.count()} records into {TARGET_TABLE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
