# Databricks Notebook source
from pyspark import pipelines as dp
import pyspark.sql.functions as F

PIPELINE_NAME = spark.conf.get("pipeline_name")

@dp.materialized_view(
    name="MOTOR.GOLD.INJURY_DETAILS",
    comment="Aggregated injury counts by borough, location, and zip",
    table_properties={"quality": "gold", 
                      'delta.enableDeletionVectors' : 'true', 
                      'delta.enableRowTracking' : 'true',
                      'delta.enableChangeDataFeed' : 'true'}
)
def injury_details():
    # Read silver table
    accidents_injury_df = spark.read.table("MOTOR.SILVER.VEHICLE_ACCIDENTS_TRANSFORMED")

    # Group, aggregate, filter (HAVING), and order
    accidents_injury_agg_df = (
        accidents_injury_df.groupBy("BOROUGH", "LOCATION", "ZIP_CODE")
            .agg(
                F.count("NUMBER_OF_CYCLIST_INJURED").alias("NUMBER_OF_CYCLIST_INJURED"),
                F.count("NUMBER_OF_PEDESTRIANS_INJURED").alias("NUMBER_OF_PEDESTRIANS_INJURED"),
                F.count("NUMBER_OF_PERSONS_INJURED").alias("NUMBER_OF_PERSONS_INJURED")
            )
            .filter(
                (F.col("NUMBER_OF_CYCLIST_INJURED") > 1) &
                (F.col("NUMBER_OF_PEDESTRIANS_INJURED") > 1) &
                (F.col("NUMBER_OF_PERSONS_INJURED") > 1)
            )
            .orderBy("BOROUGH", "LOCATION")
    )

    return accidents_injury_agg_df

   

# Databricks Notebook source
from pyspark import pipelines as dp
import pyspark.sql.functions as F

PIPELINE_NAME = spark.conf.get("pipeline_name")

@dp.materialized_view(
    name="MOTOR.GOLD.KILLED_DETAILS",
    comment="Aggregated killed counts by borough, location, and zip",
    table_properties={"quality": "gold", 
                      'delta.enableDeletionVectors' : 'true', 
                      'delta.enableRowTracking' : 'true',
                      'delta.enableChangeDataFeed' : 'true'}
)
def killed_details():
    # Read silver table
    accidents_killed_df = spark.read.table("MOTOR.SILVER.VEHICLE_ACCIDENTS_TRANSFORMED")

    # Group, aggregate, filter (HAVING), and order
    accidents_killed_agg_df = (
        accidents_killed_df.groupBy("BOROUGH", "LOCATION", "ZIP_CODE")
            .agg(
                F.count("NUMBER_OF_CYCLIST_KILLED").alias("NUMBER_OF_CYCLIST_KILLED"),
                F.count("NUMBER_OF_PEDESTRIANS_KILLED").alias("NUMBER_OF_PEDESTRIANS_KILLED"),
                F.count("NUMBER_OF_PERSONS_KILLED").alias("NUMBER_OF_PERSONS_KILLED")
            )
            .filter(
                (F.col("NUMBER_OF_CYCLIST_KILLED") > 1) &
                (F.col("NUMBER_OF_PEDESTRIANS_KILLED") > 1) &
                (F.col("NUMBER_OF_PERSONS_KILLED") > 1)
            )
            .orderBy("BOROUGH", "LOCATION")
    )

    return accidents_killed_agg_df
