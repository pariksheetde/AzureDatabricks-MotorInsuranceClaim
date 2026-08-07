# Databricks Notebook source
from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
# from utilities.Crash_Schemas import bronze_schema,silver_schema,bronze_schema_sql

PIPELINE_NAME = spark.conf.get('pipeline_name')
file_path = '/Volumes/motor/landing/operational/claim/'

@dp.table(
    name = 'MOTOR.BRONZE.INSURANCE_CLAIM_STREAM',
    comment = 'RAW INSURANCE CLAIM DATA',
    table_properties = {'quality' : 'bronze'},
    # schema = bronze_schema,
    private = False
)

def bronze_vehicle_crashes():
    df = (
        spark.readStream
             .format('cloudFiles')
             .option("cloudFiles.format", "csv")
             .option('header','true')
             .option("recursiveFileLookup", "true")
             .load(file_path)
             .select("*")
             .withColumn('INPUT_FILE_NAME', F.col('_metadata.file_path'))
             .withColumn('INGESTION_LOAD_TIMESTAMP', F.current_timestamp())
             .withColumn('PIPELINE_NAME', F.lit(PIPELINE_NAME))
    )
    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace(" ", "_"))
    return df