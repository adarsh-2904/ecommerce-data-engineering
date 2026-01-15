from utils.spark_session import get_spark_session
from utils.logger import get_logger
from pyspark.sql.window import Window
from pyspark.sql.functions import col, current_timestamp, row_number

def main():
    spark = get_spark_session("Silver Payments Ingestion")
    logger = get_logger("Silver Payments Ingestion")
    try:
        spark.sql("""
                    CREATE TABLE IF NOT EXISTS silver_payments(
                        payment_id STRING,
                        order_id STRING,
                        payment_method STRING,
                        payment_status STRING,
                        amount DOUBLE,
                        payment_date DATE,
                        ingestion_time TIMESTAMP,
                        silver_processed_at TIMESTAMP
                  )
                    USING DELTA
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/silver/payments'
                """)
        
        logger.info("Silver Payments table created or already exists.") 

        #read bronze payments data
        bronze_payments_df = spark.table("bronze_payments")

        #transform
        window_spec = Window.partitionBy("payment_id").orderBy(col("payment_date").desc())

        silver_payments_df = (
            bronze_payments_df
            .filter(col("payment_id").isNotNull())
            .withColumn("row_num",row_number().over(window_spec))
            .filter(col("row_num")==1)
            .drop("row_num")
            .withColumn("silver_processed_at", current_timestamp())
        )

        silver_payments_df = silver_payments_df.drop(col("data_source"))

        silver_payments_df.show(5)
        #write to silver layer      
        silver_payments_df.write.format("delta").mode("overwrite").saveAsTable("silver_payments")
        
        #for debugging
        print(f"Current catalog: {spark.catalog.currentCatalog()}") 
        print(f"Current database: {spark.catalog.currentDatabase()}")
        spark.sql("SHOW TABLES").show()

        logger.info("Payments data written successfully to Silver layer.")
    except Exception as e:
        logger.error(f"Error in Silver Payments Ingestion: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()