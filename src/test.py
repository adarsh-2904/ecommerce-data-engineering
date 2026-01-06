from utils.spark_session import get_spark_session

spark = get_spark_session("Utils Test")
spark.sql("SELECT 1").show()
spark.stop()