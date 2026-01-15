from utils.spark_session import get_spark_session
from utils.logger import get_logger
from pyspark.sql.window import Window   
from pyspark.sql.functions import col, current_timestamp, row_number

def main():
    spark = get_spark_session("Silver Inventory Ingestion")
    logger = get_logger("Silver Inventory Ingestion")
    try:
        spark.sql("""
                    CREATE TABLE IF NOT EXISTS silver_inventory(
                        product_id STRING,
                        warehouse_id STRING,
                        stock_available STRING,
                        snapshot_date DATE,
                        ingestion_time TIMESTAMP,
                        silver_processed_at TIMESTAMP
                  )
                    USING DELTA
                    PARTITIONED BY (snapshot_date)
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/silver/inventory'
                  """)
        logger.info("Silver Inventory table created or already exists.")

        #read bronze inventory data
        bronze_inventory_df = spark.table("bronze_inventory")

        #transform
        window_spec = Window.partitionBy("product_id", "warehouse_id").orderBy(col("ingestion_time").desc())

        silver_inventory_df = (
            bronze_inventory_df
            .filter(col("product_id").isNotNull() & col("warehouse_id").isNotNull())
            .withColumn("row_num", row_number().over(window_spec))   #assign row numbers to each product_id and warehouse_id partition ordered by ingestion_time desc (for deduplication)
            .filter(col("row_num")==1)
            .drop("row_num")
            .withColumn("silver_processed_at", current_timestamp())
        )

        silver_inventory_df = silver_inventory_df.drop(col("data_source"))
        silver_inventory_df.show(5)
        #write to silver layer
        silver_inventory_df.write.format("delta").mode("overwrite").partitionBy("snapshot_date").saveAsTable("silver_inventory")
        
        #for debugging
        print(f"Current catalog: {spark.catalog.currentCatalog()}")
        print(f"Current database: {spark.catalog.currentDatabase()}")
        spark.sql("SHOW TABLES").show()

        logger.info("Inventory data written successfully to Silver layer.")
    except Exception as e:
        logger.error(f"Error in Silver Inventory Ingestion: {e}")   
    finally:
        spark.stop()
    
if __name__ == "__main__":
    main()