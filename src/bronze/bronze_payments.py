from pyspark.sql.functions import lit, current_timestamp, to_date, col
from utils.spark_session import get_spark_session
from utils.logger import get_logger
from utils.schema import PAYMENT_SCHEMA

def main():
    spark = get_spark_session("Bronze Payments Ingestion")
    logger = get_logger("Bronze Payments Ingestion")
    try:
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS bronze_payments (
                        payment_id       STRING,
                        order_id         STRING,
                        payment_method   STRING,
                        payment_status   STRING,
                        amount           DOUBLE,
                        payment_date     DATE,
                        ingestion_time   TIMESTAMP,
                        data_source      STRING
                    )
                    USING DELTA
                    PARTITIONED BY (payment_date)
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/bronze/payments'
                """)

        payments_df = spark.read.format("json").option("multiline", "true").schema(PAYMENT_SCHEMA)\
            .load("/C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/data/raw/payments/")
        
        payments_df.printSchema()

        payments_df = payments_df.withColumn("payment_date",to_date(col("payment_date"), "dd-MM-yyyy"))\
            .withColumn("ingestion_time",current_timestamp())\
            .withColumn("data_source",lit("ecommerce_platform"))
        
        payments_df.show()
        payments_df.printSchema()
        
        payments_df.write.format("delta").mode("append").partitionBy("payment_date").saveAsTable("bronze_payments")

        logger.info("Payments data written successfully to Bronze layer.")
    except Exception as e:
        logger.error(f"Error in Bronze Payments Ingestion: {e}")    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()