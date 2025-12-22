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
    
# Extractors
@task(cache_policy=NO_CACHE)
def extract_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)

@task(cache_policy=NO_CACHE)
def extract_parquet(spark, path):
    return spark.read.parquet(path)

@task(cache_policy=NO_CACHE)
def extract_json(spark, path):
    return spark.read.option("multiline", True).json(path)

# Transform JSON - Special handling for struct format
@task(cache_policy=NO_CACHE)
def transform_json(df):
    """
    Transform JSON data where all columns are structs with numeric keys.
    Each struct contains 102 values (0-101), representing different records.
    """
    print("Transforming JSON data with struct format...")
    print(f"Total rows in JSON: {df.count()}")
    
    # Add an index to keep track of original rows
    df = df.withColumn("row_id", monotonically_increasing_id())
    
    # For each column, we need to explode the struct
    # We'll create arrays from the struct fields and then explode them
    country_fields = sorted([f for f in df.select("country.*").columns], key=lambda x: int(x))
    last_fields = sorted([f for f in df.select("last.*").columns], key=lambda x: int(x))
    previous_fields = sorted([f for f in df.select("previous.*").columns], key=lambda x: int(x))
    referance_fields = sorted([f for f in df.select("referance.*").columns], key=lambda x: int(x))
    unit_fields = sorted([f for f in df.select("unit.*").columns], key=lambda x: int(x))
    
    print(f"Found {len(country_fields)} fields in each struct")
    
    # Create arrays from struct fields
    df = df.withColumn(
        "country_array", 
        array(*[col(f"country.{f}") for f in country_fields])
    ).withColumn(
        "last_array", 
        array(*[col(f"last.{f}") for f in last_fields])
    ).withColumn(
        "previous_array", 
        array(*[col(f"previous.{f}") for f in previous_fields])
    ).withColumn(
        "referance_array", 
        array(*[col(f"referance.{f}") for f in referance_fields])
    ).withColumn(
        "unit_array", 
        array(*[col(f"unit.{f}") for f in unit_fields])
    )
    
    # Explode the arrays using posexplode to get index
    df = df.select(
        "row_id",
        posexplode(
            arrays_zip(
                "country_array", 
                "last_array", 
                "previous_array", 
                "referance_array", 
                "unit_array"
            )
        ).alias("index", "data")
    ).select(
        "row_id",
        "index",
        col("data.country_array").alias("country"),
        col("data.last_array").alias("last"),
        col("data.previous_array").alias("previous"),
        col("data.referance_array").alias("referance"),
        col("data.unit_array").alias("unit")
    )
    
    # Filter out null rows
    df = df.filter(col("country").isNotNull())
    
    print(f"After exploding: {df.count()} rows")
    
    # Now apply the same transformations as other data sources
    df = df.withColumnRenamed("last", "current_value") \
           .withColumnRenamed("previous", "previous_value")

    df = df.withColumn("current_value", col("current_value").cast("double")) \
           .withColumn("previous_value", col("previous_value").cast("double")) \
           .fillna({"current_value": 0, "previous_value": 0}) \
           .withColumn("change", col("current_value") - col("previous_value")) \
           .withColumn(
               "trend",
               when(col("change") > 0, "UP")
               .when(col("change") < 0, "DOWN")
               .otherwise("NO_CHANGE")
           ) \
           .withColumn("country", upper(trim(col("country")))) \
           .withColumn("reference_date", to_date(col("referance"), "yyyy-MM")) \
           .filter(col("current_value") > 0) \
           .dropDuplicates(["country", "referance"]) \
           .drop("row_id", "index")
    
    return df

# Transform CSV and Parquet
@task(cache_policy=NO_CACHE)
def transform_regular(df):
    """
    Transform CSV and Parquet data (regular format).
    """
    df = df.withColumnRenamed("last", "current_value") \
           .withColumnRenamed("previous", "previous_value")

    df = df.withColumn("current_value", col("current_value").cast("double")) \
           .withColumn("previous_value", col("previous_value").cast("double")) \
           .fillna({"current_value": 0, "previous_value": 0}) \
           .withColumn("change", col("current_value") - col("previous_value")) \
           .withColumn(
               "trend",
               when(col("change") > 0, "UP")
               .when(col("change") < 0, "DOWN")
               .otherwise("NO_CHANGE")
           ) \
           .withColumn("country", upper(trim(col("country")))) \
           .withColumn("reference_date", to_date(col("referance"), "yyyy-MM")) \
           .filter(col("current_value") > 0) \
           .dropDuplicates(["country", "referance"])
    
    return df

# Simple JSON transform (if data is actually just 1 record per row)
@task(cache_policy=NO_CACHE)
def transform_json_simple(df):
    """
    Simple transform assuming only the first field (0) contains data.
    This is based on your sample showing 'ABD' in country.0
    """
    print("Using simple JSON transform (extracting first field only)")
    
    # Extract the first field from each struct
    df = df.withColumn("country", col("country.0")) \
           .withColumn("last", col("last.0")) \
           .withColumn("previous", col("previous.0")) \
           .withColumn("referance", col("referance.0")) \
           .withColumn("unit", col("unit.0"))
    
    # Drop the original struct columns
    df = df.select("country", "last", "previous", "referance", "unit")
    
    # Now apply regular transform
    df = df.withColumnRenamed("last", "current_value") \
           .withColumnRenamed("previous", "previous_value")

    df = df.withColumn("current_value", col("current_value").cast("double")) \
           .withColumn("previous_value", col("previous_value").cast("double")) \
           .fillna({"current_value": 0, "previous_value": 0}) \
           .withColumn("change", col("current_value") - col("previous_value")) \
           .withColumn(
               "trend",
               when(col("change") > 0, "UP")
               .when(col("change") < 0, "DOWN")
               .otherwise("NO_CHANGE")
           ) \
           .withColumn("country", upper(trim(col("country")))) \
           .withColumn("reference_date", to_date(col("referance"), "yyyy-MM")) \
           .filter(col("current_value") > 0) \
           .dropDuplicates(["country", "referance"])
    
    return df

# Integrate
@task(cache_policy=NO_CACHE)
def integrate(csv_df, parquet_df, json_df):
    # Ensure all DataFrames have the same columns in the same order
    cols = ["country", "current_value", "previous_value", "referance", "unit", "change", "trend", "reference_date"]
    
    # Select only the columns we need
    csv_df = csv_df.select(cols)
    parquet_df = parquet_df.select(cols)
    json_df = json_df.select(cols)
    
    return csv_df.unionByName(parquet_df).unionByName(json_df)


    # Run

if __name__ == "__main__":
    gold_reserves_etl(
        csv_path="data/gold_reserves (1).csv",
        parquet_path="data/gold_reserves (1).parquet",
        json_path="data/gold_reserves (1).json"
    )
    