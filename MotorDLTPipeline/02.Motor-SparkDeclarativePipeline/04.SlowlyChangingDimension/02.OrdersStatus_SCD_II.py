from pyspark import pipelines as dp
import pyspark.sql.functions as F

# IMPLEMENTING SLOWLY CHANGING DIMENSION TYPE 2
dp.create_streaming_table('MOTOR.SILVER.ORDER_STATUS_SCD_2')
dp.create_auto_cdc_flow(name = 'ORDERS_STATUS_SCD_2',
                        target = 'MOTOR.SILVER.ORDER_STATUS_SCD_2',
                        source = 'MOTOR.SILVER.ORDER_STATUS_STREAMING_SCD_2',
                        keys = ['OrderID'],
                        sequence_by = F.col('_commit_version'),
                        apply_as_deletes = F.expr("_change_type = 'delete'"),
                        except_column_list = ['_change_type', '_commit_version', '_commit_timestamp'],
                        stored_as_scd_type = 2,
                        track_history_column_list = ['Status', 'TotalAmount']

)