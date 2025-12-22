from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, trim, to_date, arrays_zip, posexplode, array, lit, monotonically_increasing_id
)
from pyspark.sql.types import StringType, DoubleType
import duckdb
import os

# Spark Session
@task(cache_policy=NO_CACHE)
def get_spark():
    return (
        SparkSession.builder
        .appName("Gold Reserves ETL")
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    
    
    # Run
if __name__ == "__main__":
    gold_reserves_etl(
        csv_path="data/gold_reserves (1).csv",
        parquet_path="data/gold_reserves (1).parquet",
        json_path="data/gold_reserves (1).json"
    )
    