from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, expr, current_timestamp, round
import os
# Initialize Spark
# Note: We keep the Delta configs because we still need the Delta engine 
# for the "Optimiser" phase, but we write to CSV for the "Raw" phase.

# This allows the script to use the master passed by spark-submit, 
# or default to localhost if run from your Mac IDE.
#master_url = os.environ.get("SPARK_MASTER_URL", "spark://localhost:7077")

spark = SparkSession.builder \
    .appName("Generate_Raw_Data") \
    .config("spark.executor.memory", "512m") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

RAW_PATH = "/opt/spark/datasets/raw_data"

# 1. Store Dimension (Small - for Broadcast Join testing)
print("Generating raw store data...")
spark.range(1, 101).selectExpr(
    "id as store_id",
    "concat('Store_', cast(id as string)) as store_name",
    "case when id % 4 = 0 then 'North' else 'South' end as region"
).write.mode("overwrite").option("header", "true").csv(f"{RAW_PATH}/store_dim")

# 2. Customer Dimension (Large - for Shuffle Join testing)
print("Generating raw customer data...")
spark.range(1, 2000001).selectExpr(
    "id as customer_id",
    "concat('Customer_', cast(id as string)) as customer_name",
    "cast(rand() * 100 as int) as loyalty_score"
).write.mode("overwrite").option("header", "true").csv(f"{RAW_PATH}/customer_dim")

# 3. Sales Fact (Heavy - to stress the cluster)
print("Generating raw sales data (this will take a moment)...")
# We generate 50M rows. This will likely trigger spills/GC with 512MB RAM.
sales_df = spark.range(1, 50000001).select(
    expr("id as transaction_id"),
    current_timestamp().alias("transaction_time"),
    expr("cast(rand() * 2000000 + 1 as int) as customer_id"),
    expr("cast(rand() * 100 + 1 as int) as store_id"),
    round(expr("rand() * 100 + 10"), 2).alias("amount")
)

# Repartition to 16 to simulate a distributed write
sales_df.repartition(16).write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"{RAW_PATH}/sales_fact")

print(f"Success! Raw CSVs are located in: {RAW_PATH}")
spark.stop()