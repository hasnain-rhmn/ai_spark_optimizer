import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# --- CONFIGURATION ---
DELTA_PATH = "/opt/spark/datasets/delta_tables"
STORE_PATH = f"{DELTA_PATH}/store_dim"
CUSTOMER_PATH = f"{DELTA_PATH}/customer_dim"
SALES_PATH = f"{DELTA_PATH}/sales_fact"

def create_session(app_name):
    """Boots a fresh Spark Session configured for Delta Lake."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .getOrCreate()

def run_job_1_baseline():
    """CASE 1: Healthy Baseline. Fast, distributed read and filter on Delta."""
    spark = create_session("01_Healthy_Baseline_Scan")
    df = spark.read.format("delta").load(SALES_PATH)
    df.filter(F.col("amount") > 50).count()
    spark.stop()

def run_job_2_skewed_agg():
    """CASE 2: Data Skew. Aggregating on the promotion_id (where 80% is '1'). 
    AI Signal: task_duration_max >> task_duration_median."""
    spark = create_session("02_Severe_Data_Skew_Agg")
    df = spark.read.format("delta").load(SALES_PATH)
    # The skew is highly concentrated on promotion_id = 1
    df.groupBy("promotion_id").agg(F.sum("amount"), F.count("*")).collect()
    spark.stop()

def run_job_3_sort_merge_spill():
    """CASE 3: The Slow Join. SortMergeJoin on the massive 50M row fact table.
    AI Signal: BlockManager_disk_diskSpaceUsed_MB > 0 and plan_text shows SortMergeJoin."""
    spark = create_session("03_SortMerge_Join_Spill")
    sales = spark.read.format("delta").load(SALES_PATH)
    customers = spark.read.format("delta").load(CUSTOMER_PATH)
    # Auto-broadcast is off, so this 50M to 2M join will force a massive shuffle and likely spill
    sales.join(customers, "customer_id").count()
    spark.stop()

def run_job_4_broadcast_fix():
    """CASE 4: The Optimized Join. Forces Broadcast on the dimension table.
    AI Signal: Fast execution, high input bytes, plan_text shows BroadcastHashJoin."""
    spark = create_session("04_Broadcast_Join_Optimal")
    sales = spark.read.format("delta").load(SALES_PATH)
    stores = spark.read.format("delta").load(STORE_PATH)
    # Broadcasting the tiny 100-row store table completely eliminates shuffle
    sales.join(F.broadcast(stores), "store_id").count()
    spark.stop()

def run_job_5_python_oom():
    """CASE 5: PySpark Python Memory Hog. Uses a standard Python UDF.
    AI Signal: ProcessTreePythonRSSMemory spikes massively."""
    spark = create_session("05_Python_UDF_Memory_Hog")
    
    @F.udf(returnType=StringType())
    def bad_python_memory_udf(val):
        # Create a massive useless string in Python memory
        useless_string = "X" * 100000 
        return str(val) + useless_string[:1]

    customers = spark.read.format("delta").load(CUSTOMER_PATH)
    customers.withColumn("bad_col", bad_python_memory_udf("customer_name")).count()
    spark.stop()

def run_job_6_over_caching():
    """CASE 6: RAM Exhaustion. Caching the 50M row fact table multiple times.
    AI Signal: BlockManager_memory_memUsed_MB hits the absolute limit."""
    spark = create_session("06_Over_Caching_RAM_Spike")
    sales = spark.read.format("delta").load(SALES_PATH)
    
    df1 = sales.filter(F.col("promotion_id") == 1).cache()
    df1.count()
    
    df2 = sales.filter(F.col("promotion_id") != 1).cache()
    df2.count()
    spark.stop()

def run_job_7_global_sort():
    """CASE 7: The Concurrency Killer. Global order by.
    AI Signal: fact_stages shows num_tasks = 1 during the final stage."""
    spark = create_session("07_Global_Sort_Bottleneck")
    sales = spark.read.format("delta").load(SALES_PATH)
    # Global sort forces all 50M rows onto a single Executor partition
    sales.orderBy("amount").count()
    spark.stop()

def run_job_8_cross_join():
    """CASE 8: Cartesian Product. Data explosion.
    AI Signal: Massive shuffle_write_mb and BroadcastNestedLoopJoin in plan."""
    spark = create_session("08_Cross_Join_Explosion")
    customers = spark.read.format("delta").load(CUSTOMER_PATH).limit(1000)
    stores = spark.read.format("delta").load(STORE_PATH)
    # Cross join! 1000 customers x 100 stores = 100,000 rows instantly
    customers.crossJoin(stores).count()
    spark.stop()

def run_job_9_gc_thrashing():
    """CASE 9: Garbage Collection Hell. Creating temporary Java objects.
    AI Signal: ExecutorMetrics_TotalGCTime spikes aggressively."""
    spark = create_session("09_GC_Thrashing_Explode")
    sales = spark.read.format("delta").load(SALES_PATH).limit(500000)
    # Explode an array of 200 items for every single row
    sales.withColumn("dummy_array", F.array([F.lit(i) for i in range(200)])) \
         .withColumn("exploded", F.explode("dummy_array")) \
         .count()
    spark.stop()

def run_job_10_window_without_partition():
    """CASE 10: Driver/Executor Overload. Window function without partitionBy.
    AI Signal: Single partition processing massive state."""
    spark = create_session("10_Window_No_Partition_OOM")
    sales = spark.read.format("delta").load(SALES_PATH).limit(1000000)
    # Window ordered by time but NOT partitioned by anything = 1 massive chunk
    window_spec = Window.orderBy("transaction_time")
    sales.withColumn("cumulative_sum", F.sum("amount").over(window_spec)).count()
    spark.stop()

if __name__ == "__main__":
    jobs = [
        run_job_1_baseline,
        run_job_2_skewed_agg,
        run_job_3_sort_merge_spill,
        run_job_4_broadcast_fix,
        run_job_5_python_oom,
        run_job_6_over_caching,
        run_job_7_global_sort,
        run_job_8_cross_join,
        run_job_9_gc_thrashing,
        run_job_10_window_without_partition
    ]
    
    print("🚀 Starting the Delta Telemetry Gauntlet...")
    for idx, job in enumerate(jobs):
        print(f"\n--- Running Job {idx+1}/{len(jobs)}: {job.__name__} ---")
        try:
            job()
            print("✅ Complete.")
        except Exception as e:
            print(f"❌ Job Failed (This is expected for some tests!): {e}")
        
        # Give Prometheus 5 seconds to flush the final metrics
        print("⏳ Waiting 5 seconds for Prometheus to flush metrics...")
        time.sleep(5)
        
    print("\n🎉 Gauntlet Finished! Run your `incremental_telemetry_etl.py` script now.")