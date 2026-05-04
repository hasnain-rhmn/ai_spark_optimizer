from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr, current_timestamp, round, when

# We remove .master() so it's flexible for Docker or Local execution
spark = SparkSession.builder \
    .appName("Generate_Delta_Data_With_Skew") \
    .config("spark.executor.memory", "512m") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

DELTA_PATH = "/opt/spark/datasets/delta_tables"

# 1. Store Dimension (Small)
print("Writing store_dim as Delta...")
spark.range(1, 101).selectExpr(
    "id as store_id",
    "concat('Store_', cast(id as string)) as store_name",
    "case when id % 4 = 0 then 'North' else 'South' end as region"
).write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/store_dim")

# 2. Customer Dimension (Large)
print("Writing customer_dim as Delta...")
spark.range(1, 2000001).selectExpr(
    "id as customer_id",
    "concat('Customer_', cast(id as string)) as customer_name",
    "cast(rand() * 100 as int) as loyalty_score"
).write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/customer_dim")

# 3. Sales Fact (Heavy + SKEWED)
print("Writing sales_fact as Delta with skew...")
sales_df = spark.range(1, 50000001).select(
    expr("id as transaction_id"),
    current_timestamp().alias("transaction_time"),
    expr("cast(rand() * 2000000 + 1 as int) as customer_id"),
    expr("cast(rand() * 100 + 1 as int) as store_id"),
    # INTRODUCING SKEW: 
    # 80% of rows will have promotion_id = 1 (The Skew)
    # 20% will be randomly distributed between 2 and 1000
    when(rand() < 0.8, 1)
    .otherwise(expr("cast(rand() * 999 + 2 as int)"))
    .alias("promotion_id"),
    round(expr("rand() * 100 + 10"), 2).alias("amount")
)

# Writing as Delta
# We use 16 partitions to ensure the skew shows up clearly in the Task metrics
sales_df.repartition(16).write.format("delta").mode("overwrite").save(f"{DELTA_PATH}/sales_fact")

print(f"Success! Delta tables with skew are located in: {DELTA_PATH}")
spark.stop()