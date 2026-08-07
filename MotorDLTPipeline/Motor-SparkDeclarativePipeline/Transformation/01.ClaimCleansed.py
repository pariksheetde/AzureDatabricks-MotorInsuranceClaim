# Databricks Notebook source
from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, to_timestamp, col

# Pipeline name from configuration
PIPELINE_NAME = spark.conf.get("pipeline_name")

# Expectations for data quality (use transformed column names!)
expectations = {
    "valid_COLLISION_ID": "COLLISION_ID IS NOT NULL",
    "valid_CLAIM_AMOUNT": "CLAIM_AMOUNT > 0"
    # "valid_CLAIM_DATE_TIMESTAMP": "CLAIM_DATE_TIMESTAMP IS NOT NULL"
}

@dp.table(
    name="MOTOR.SILVER.INSURANCE_CLAIM_TRANSFORMED",
    comment="TRANSFORMED VEHICLE ACCIDENTS DATA",
    table_properties={"quality": "silver"}
)
@dp.expect_all_or_drop(expectations)
def vehicle_accidents_cleansed_stream():
    # Read from bronze streaming table
    df = spark.readStream.table("MOTOR.BRONZE.INSURANCE_CLAIM_STREAM")

    # Transformations
    trans_df = (
        df.withColumn("COLLISION_ID", col("CollisionID").cast("int"))
        #   .withColumn("CLAIM_DATE_TIMESTAMP", to_timestamp(col("ClaimDateTime"), "yyyy-MM-dd HH:mm:ss"))
          .withColumnRenamed('ClaimDateTime',"CLAIM_DATE_TIMESTAMP")
          .withColumn("CLAIM_AMOUNT", col("ClaimAmount").cast("float"))
          .withColumn("INGESTION_LOAD_TIMESTAMP", current_timestamp())
          .withColumn("PIPELINE_NAME", lit(PIPELINE_NAME))
          .filter(col("ClaimDateTime").isNotNull())
    )

    # Drop original raw columns if they exist
    cols_to_drop = [c for c in ["CollisionID", "ClaimAmount", "ClaimDateTime"] if c in df.columns]
    trans_df = trans_df.drop(*cols_to_drop)

    # Final selection of columns for silver table
    loaded_claim_df = trans_df.select(
        "COLLISION_ID",
        "CLAIM_AMOUNT",
        "CLAIM_DATE_TIMESTAMP",
        "INPUT_FILE_NAME",
        "INGESTION_LOAD_TIMESTAMP",
        "PIPELINE_NAME"
    )

    return loaded_claim_df