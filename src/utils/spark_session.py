from pyspark.sql import SparkSession


def get_spark_session(app_name:str)-> SparkSession:
    """
    Create and return a SparkSession with the given application name.

    Parameters:
    app_name (str): The name of the Spark application.

    Returns:
    SparkSession: An instance of SparkSession.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            "spark.jars",
            "C:/Users/Exavalu/OneDrive - exavalu/jar/postgresql-42.7.3.jar"
        )
        .config("spark.sql.warehouse.dir", "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/ecommerce-data-engineering/src/data_warehouse")
        .enableHiveSupport()    #enabled Hive suport to store metadata in a metastore. It uses the derby database by default.
        .getOrCreate()
    )
    return spark
