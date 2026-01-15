from pyspark.sql.functions import col, current_timestamp, lit, to_date, row_number
from pyspark.sql.window import Window
from utils.spark_session import get_spark_session
from utils.logger import get_logger 

def main():
    spark = get_spark_session("Silver Orders Ingestion")
    logger = get_logger("Silver Orders Ingestion")

    try:
        
        #spark.sql("select * from spark_catalog.default.bronze_orders").show(5)
        spark.sql("""
                    CREATE TABLE IF NOT EXISTS silver_orders(
                        order_id STRING,
                        order_date DATE,
                        customer_id STRING,
                        product_id STRING,
                        quantity INT,
                        unit_price DOUBLE,
                        order_status STRING,
                        ingestion_time TIMESTAMP,
                        silver_processed_at TIMESTAMP
                  )
                    USING DELTA
                    PARTITIONED BY (order_date)
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/silver/orders'
                  """)
        logger.info("Silver Orders table created or already exists.")

        #read bronze order data
        bronze_orders_df = spark.table("bronze_orders")

        #transform
        window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_time").desc())

        silver_orders_df = (
            bronze_orders_df
            .filter(col("order_id").isNotNull())
            .withColumn("row_num", row_number().over(window_spec))   #assign row numbers to each order_id partition ordered by ingestion_time desc (for deduplication)
            .filter(col("row_num")==1)
            .drop("row_num")
            .withColumn("silver_processed_at", current_timestamp())
        )

        silver_orders_df = silver_orders_df.drop(col("data_source"))
        silver_orders_df.show(5)
        #write to silver layer
        silver_orders_df.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable("silver_orders")
        
        #for debugging
        print(f"Current catalog: {spark.catalog.currentCatalog()}")
        print(f"Current database: {spark.catalog.currentDatabase()}")
        spark.sql("SHOW TABLES").show()

        logger.info("Orders data written successfully to Silver layer.")
    except Exception as e:
        logger.error(f"Error in Silver Orders Ingestion: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()