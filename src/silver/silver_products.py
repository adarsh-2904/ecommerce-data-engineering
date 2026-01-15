from utils.spark_session import get_spark_session
from utils.logger import get_logger
from pyspark.sql.window import Window
from pyspark.sql.functions import col, current_timestamp, row_number    

def main():
    spark = get_spark_session("Silver Products Ingestion")
    logger = get_logger("Silver Products Ingestion")
    try:
        spark.sql("""
                    CREATE TABLE IF NOT EXISTS silver_products(
                        product_id STRING,
                        category STRING,
                        cost_price DOUBLE,
                        selling_price DOUBLE,
                        ingestion_time TIMESTAMP,
                        silver_processed_at TIMESTAMP
                  )
                    USING DELTA
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/silver/products'
                  """)
        logger.info("Silver Products table created or already exists.")

        #read bronze products data
        bronze_products_df = spark.table("bronze_products")

        #transform
        window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_time").desc())

        silver_products_df = (
            bronze_products_df
            .filter(col("product_id").isNotNull())
            .withColumn("row_num",row_number().over(window_spec))
            .filter(col("row_num")==1)
            .drop("row_num")
            .withColumn("silver_processed_at", current_timestamp())
        )

        silver_products_df = silver_products_df.drop(col("data_source"))
        silver_products_df.show(5)
        #write to silver layer
        silver_products_df.write.format("delta").mode("overwrite").saveAsTable("silver_products")
        
        #for debugging
        print(f"Current catalog: {spark.catalog.currentCatalog()}")
        print(f"Current database: {spark.catalog.currentDatabase()}")
        spark.sql("SHOW TABLES").show()

        logger.info("Products data written successfully to Silver layer.")
    except Exception as e:
        logger.error(f"Error in Silver Products Ingestion: {e}")   
    finally:
        spark.stop()
    
if __name__ == "__main__":
    main()