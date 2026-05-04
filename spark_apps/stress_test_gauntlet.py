import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- CONFIGURATION ---
DELTA_PATH = "/opt/spark/datasets/delta_tables"
SALES_PATH = f"{DELTA_PATH}/sales_fact"

def create_heavy_session(app_name):
    """Configured to force spills by limiting memory and disabling optimizations."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def run_job_1_massive_sort_spill():
    """CASE 1: The Disk Crusher.
    We sort 50 million rows on a random float with only 4 partitions. 
    Each partition will hold ~12.5M rows, forcing a massive Disk Spill."""
    spark = create_heavy_session("STRESS_01_Sort_Spill")
    df = spark.read.format("delta").load(SALES_PATH)
    
    print("🚀 Sorting 50M rows on 4 partitions (Guaranteed Spill)...")
    df.orderBy(F.rand()).write.format("noop").mode("overwrite").save()
    spark.stop()

def run_job_2_exploding_skew_join():
    """CASE 2: The Skewed Join Hell.
    Joining the 50M rows to itself on 'promotion_id' (where 80% is 1).
    This will create a 'Super-Partition' that will likely crash or spill 10GB+."""
    spark = create_heavy_session("STRESS_02_Skewed_Join_Spill")
    df = spark.read.format("delta").load(SALES_PATH)
    
    print("🚀 Joining 50M rows on skewed key 'promotion_id'...")
    # Select only a few columns to avoid instant OOM, but enough to force spill
    df_left = df.select("transaction_id", "promotion_id", "amount")
    df_right = df.select("transaction_id", "promotion_id")
    
    df_left.join(df_right, "promotion_id").count()
    spark.stop()

def run_job_3_high_cardinality_agg():
    """CASE 3: Aggregation Buffer Stress.
    Grouping by transaction_id (unique) and a random string.
    Forces Spark to maintain a massive in-memory hash map."""
    spark = create_heavy_session("STRESS_03_Agg_Memory_Pressure")
    df = spark.read.format("delta").load(SALES_PATH)
    
    print("🚀 High cardinality GroupBy on 50M rows...")
    df.withColumn("rand_str", F.expr("uuid()")) \
      .groupBy("transaction_id", "rand_str") \
      .agg(F.avg("amount"), F.max("amount")) \
      .count()
    spark.stop()

def run_job_4_cartesian_explosion():
    """CASE 4: The Cartesian Product.
    Cross joining 20,000 customers with 20,000 customers.
    Result: 400,000,000 rows. This will hammer the Shuffle Write metrics."""
    spark = create_heavy_session("STRESS_04_Cartesian_Explosion")
    customers = spark.read.format("delta").load(f"{DELTA_PATH}/customer_dim").limit(20000)
    
    print("🚀 Cross Join: 20k x 20k rows...")
    customers.crossJoin(customers.withColumnRenamed("customer_id", "cid2")).count()
    spark.stop()

def run_job_5_heavy_udf_serialization():
    """CASE 5: Serialization bottleneck + Python RSS.
    Pumping 50M rows through a complex Python UDF."""
    spark = create_heavy_session("STRESS_05_Python_Serialization")
    
    @F.udf(returnType="string")
    def heavy_transform(id, amt):
        return f"ID:{id}_AMT:{amt}" * 10 # Create large strings to bloat memory

    df = spark.read.format("delta").load(SALES_PATH)
    print("🚀 Running Heavy Python UDF on 50M rows...")
    df.withColumn("complex_str", heavy_transform("transaction_id", "amount")).count()
    spark.stop()

if __name__ == "__main__":
    jobs = [
        run_job_1_massive_sort_spill,
        run_job_2_exploding_skew_join,
        run_job_3_high_cardinality_agg,
        run_job_4_cartesian_explosion,
        run_job_5_heavy_udf_serialization
    ]
    
    for job in jobs:
        print(f"\n--- EXECUTION: {job.__name__} ---")
        try:
            job()
        except Exception as e:
            print(f"🔥 Job failed as expected under stress: {e}")
        time.sleep(10)