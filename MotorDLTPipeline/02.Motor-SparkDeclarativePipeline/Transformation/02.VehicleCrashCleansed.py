# Databricks Notebook source
from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.functions import lit, current_timestamp, to_timestamp, concat, col
from pyspark.sql.types import StructType, StructField, StringType

PIPELINE_NAME = spark.conf.get('pipeline_name')

expectations = {
    'valid_BOROUGH' : 'BOROUGH IS NOT NULL',
    'valid_ZIP_CODE' : 'ZIP_CODE IS NOT NULL',
     'valid_LOCATIONE' : 'LOCATION IS NOT NULL'
}

reverse_qa_condition="!("+" AND ".join(expectations.values())+")"

@dp.table(
    name='MOTOR.SILVER.VEHICLE_ACCIDENTS_TRANSFORMED',
    comment = 'TRANSFORMED VEHICLE ACCIDENTS DATA',
    table_properties = {'quality' : 'silver'}
    )
@dp.expect_all_or_drop(expectations)
def vehicle_accidents_cleansed_stream():
    df = spark.readStream.table('MOTOR.BRONZE.VEHICLE_ACCIDENTS_STREAM')
    # for clm in df.schema:
    #     col_name=clm.name
    #     col_type=silver_schema[col_name].dataType
    #     df = df.withColumn(col_name,F.col(col_name).cast(col_type))

    trans_df = (df.withColumn('ACCIDENT_DATE_TIME', to_timestamp(concat('CRASH_DATE', lit(' '), 'CRASH_TIME'), 'M/d/yyyy h:mm:ss a'))
                .drop('LATITUDE','LONGITUDE','CRASH_DATE','CRASH_TIME')
          .withColumn('INGESTION_LOAD_TIMESTAMP', current_timestamp())
          .withColumn('PIPELINE_NAME', lit(PIPELINE_NAME))
          .filter(F.col('ACCIDENT_DATE_TIME').isNotNull())
        )
    
    return trans_df

@dp.table(name='MOTOR.SILVER.VEHICLE_ACCIDENTS_QUARANTEED')
# @dp.expect_all_or_drop(reverse_qa_condition)
def vehicle_accidents_quaranteed():
    vehicle_qa_df = spark.readStream.table('MOTOR.BRONZE.VEHICLE_ACCIDENTS_STREAM')
    vehicle_qa_trans_df = (
        vehicle_qa_df.withColumn('ACCIDENT_DATE_TIME', to_timestamp(concat('CRASH_DATE', lit(' '), 'CRASH_TIME'), 'M/d/yyyy h:mm:ss a'))
        .drop('LATITUDE','LONGITUDE','CRASH_DATE','CRASH_TIME')
        .filter(F.col('ACCIDENT_DATE_TIME').isNotNull())
        .withColumn('is_quaranteed',F.expr(reverse_qa_condition))
        )
    return vehicle_qa_trans_df

@dp.table(name='MOTOR.SILVER.VEHICLE_ACCIDENTS_IS_QUARANTEED')
def vehicle_accidents_is_quaranteed():
    return spark.readStream.table('MOTOR.SILVER.VEHICLE_ACCIDENTS_QUARANTEED')\
        .filter(~F.col('is_quaranteed'))\
        .drop('is_quaranteed')

@dp.table(name='MOTOR.SILVER.VEHICLE_ACCIDENTS_IS_NOT_QUARANTEED')
def vehicle_accidents_is_not_quaranteed():
    return spark.readStream.table('MOTOR.SILVER.VEHICLE_ACCIDENTS_QUARANTEED')\
        .filter(F.col('is_quaranteed'))\
        .drop('is_quaranteed') 