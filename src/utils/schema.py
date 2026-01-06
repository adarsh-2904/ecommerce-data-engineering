from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType,TimestampType, DoubleType

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(),False),
    StructField("order_date", DateType(),True),
    StructField("customer_id",StringType(),True),
    StructField("product_id",StringType(),True),
    StructField("quantity", IntegerType(),True),
    StructField("unit_price", FloatType(),True),
    StructField("order_status", StringType(),True),
    StructField("created_at", TimestampType(),True)
])

INVENTORY_SCHEMA = StructType([
    StructField("product_id",StringType(),True),
    StructField("warehouse_id",StringType(),True),
    StructField("stock_available",StringType(),True),
    StructField("snapshot_date",DateType(),True)
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id",StringType(),False),
    StructField("category",StringType(),True),
    StructField("cost_price",DoubleType(),True),
    StructField("selling_price",DoubleType(),True)
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_date", DateType(), True)
])