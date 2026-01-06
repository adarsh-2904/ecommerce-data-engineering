from pyspark.sql.functions import lit, current_timestamp, col, to_date, to_timestamp
from utils.spark_session import get_spark_session
from utils.logger import get_logger

def main():
    spark = get_spark_session("Bronze Orders Ingestion")
    logger = get_logger("bronze_orders")
    try:
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS bronze_orders (
                        order_id        STRING,
                        order_date      DATE,
                        customer_id     STRING,
                        product_id      STRING,
                        quantity        INT,
                        unit_price      DOUBLE,
                        order_status    STRING,
                        created_at      TIMESTAMP,
                        ingestion_time  TIMESTAMP,
                        data_source     STRING
                    )
                    USING DELTA
                    PARTITIONED BY (order_date)
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/bronze/orders'
                """)
        
        logger.info("Bronze Orders table created or already exists.")

        orders_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true")\
            .load("/C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/data/raw/orders/")
        

        logger.info("Orders data read successfully from raw layer.")

        orders_df = orders_df.withColumn("order_date", to_date(col("order_date"), "dd-MM-yyyy"))\
            .withColumn("created_at", to_timestamp(col("created_at"), "dd-MM-yyyy HH:mm"))\
            .withColumn("ingestion_time",current_timestamp())\
            .withColumn("data_source",lit("ecommerce_platform"))
        
       

        orders_df.write.format("delta").mode("append").partitionBy("order_date").saveAsTable("bronze_orders")

      

        logger.info("Orders data written successfully to Bronze layer.")

    except Exception as e:
        logger.error(f"Error in Bronze Orders Ingestion: {e}")

    finally:
        spark.stop()
if __name__ == "__main__":
    main()