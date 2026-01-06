from pyspark.sql.functions import lit, current_timestamp, to_date, col
from utils.spark_session import get_spark_session
from utils.logger import get_logger 

def main():
    spark  = get_spark_session("Bronze Products Ingestion")
    logger = get_logger("Bronze Products Ingestion")

    try:
        spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS bronze_products (
                        product_id        STRING,
                        category          STRING,
                        cost_price        DOUBLE,
                        selling_price     DOUBLE,
                        ingestion_time    TIMESTAMP,
                        data_source       STRING
                    )
                    USING DELTA
                    LOCATION 'C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse/bronze/products'
                """)
        
        products_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true")\
            .load("/C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/data/raw/products/")

        products_df = products_df.withColumn("cost_price", col("cost_price").cast("double"))\
            .withColumn("selling_price", col("selling_price").cast("double"))\
            .withColumn("ingestion_time", current_timestamp())\
            .withColumn("data_source", lit("ecommerce_platform"))
        
       

        products_df.write.format("delta").mode("append").saveAsTable("bronze_products")

        logger.info("Products data written successfully to Bronze layer.")
    except Exception as e:
        logger.error(f"Error in Bronze Products Ingestion: {e}")
    finally:
        spark.stop()   

if __name__ == "__main__":
    main()