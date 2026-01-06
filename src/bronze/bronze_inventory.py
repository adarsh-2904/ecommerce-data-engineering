from pyspark.sql.functions import lit, current_timestamp, to_date, col
from utils.spark_session import get_spark_session
from utils.logger import get_logger

def main():
    spark = get_spark_session("Bronze Inventory Ingestion")
    logger = get_logger("Bronze Inventory Ingestion")
    try:
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS bronze_inventory (
                        product_id        STRING,
                        warehouse_id      STRING,
                        stock_available   STRING,
                        snapshot_date     DATE,
                        ingestion_time    TIMESTAMP,
                        data_source       STRING
                    )
                    USING DELTA
                    PARTITIONED BY (snapshot_date)
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/bronze/inventory'
                """)
        inventory_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true")\
            .load("/C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/data/raw/inventory/")
        
        inventory_df = inventory_df.withColumn("snapshot_date",to_date(col("snapshot_date"), "dd-MM-yyyy"))\
            .withColumn("ingestion_time",current_timestamp())\
            .withColumn("data_source",lit("ecommerce_platform"))
        
        

        inventory_df.write.format("delta").mode("append").partitionBy("snapshot_date").saveAsTable("bronze_inventory")

        logger.info("Inventory data written successfully to Bronze layer.")
    except Exception as e:
        logger.error(f"Error in Bronze Inventory Ingestion: {e}")
if __name__ == "__main__":
    main()