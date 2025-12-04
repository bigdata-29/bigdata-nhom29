# ==============================
# PHASE 2: CACHING & OPTIMIZATION DEMONSTRATION (FIXED)
# ==============================
from pyspark import StorageLevel

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, functions as F, types as T
import re
from pyspark.sql.functions import sha2, concat_ws, col
import json
import time

# ============================
# SPARK SETUP
# ============================

print("Spark session started!")

spark = (
    SparkSession.builder
    .appName("CachingDemo")
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.python.worker.faulthandler.enabled", "true")
    .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    .config("spark.python.worker.reuse", "true")
    .getOrCreate()
)


print("="*60)
print("PHASE 2: CACHING & OPTIMIZATION DEMONSTRATION (FIXED)")
print("="*60)

# Ensure output_path is defined (change path as you need)
output_path = "preprocessed_output_json"  # <-- chỉnh theo ý bạn
_temp_parquet = "tmp_final_materialized_parquet"

# 1) Materialize final (write -> read) to remove Python UDF from plan before caching
print("\nMaterializing DataFrame (write/read parquet) to remove UDFs from the logical plan...")
df = spark.read.json("../preprocess_data/Batch_process/preprocessed_preview.json")

print("Loaded final preprocessed dataset:", df.count())

final=df


# Write parquet to materialize
df.write.mode("overwrite").parquet(_temp_parquet)

# Read back native Spark parquet
final_mat = spark.read.parquet(_temp_parquet)
print(f"✓ Materialized DataFrame loaded. Count = {final_mat.count()}")

# Read back the materialized native Spark dataframe
final_mat = spark.read.parquet(_temp_parquet)
print(f"✓ Materialized DataFrame loaded. Count = {final_mat.count()}")

# 2) Set a few safety configs (faulthandler and worker reuse) to help debugging / stability


# 3) Demo WITHOUT caching first (on materialized DF)
print("\n--- SCENARIO 1: WITHOUT CACHING (materialized DF) ---")
start_time = time.time()

analysis1_start = time.time()
avg_salary_by_city = final_mat.filter(col("Lương (số)").isNotNull()) \
    .groupBy("Thành phố") \
    .agg(
        F.avg("Lương (số)").alias("Lương TB"),
        F.count("*").alias("Số lượng job")
    ) \
    .orderBy(F.desc("Lương TB"))

result1 = avg_salary_by_city.collect()
analysis1_time = time.time() - analysis1_start
print(f"✓ Completed Analysis1 in {analysis1_time:.2f} seconds")
print("\nTop 3 cities by average salary:")
for row in avg_salary_by_city.limit(3).collect():
    try:
        print(f"  {row['Thành phố']}: {row['Lương TB']:,.0f} VND ({row['Số lượng job']} jobs)")
    except:
        print(f"  {row['Thành phố']}: {row['Lương TB']} VND ({row['Số lượng job']} jobs)")

analysis2_start = time.time()
job_count_by_level = final_mat.groupBy("Cấp bậc") \
    .agg(F.count("*").alias("Số lượng")) \
    .orderBy(F.desc("Số lượng"))
result2 = job_count_by_level.limit(20).collect()
analysis2_time = time.time() - analysis2_start
print(f"✓ Completed Analysis2 in {analysis2_time:.2f} seconds")

total_time_no_cache = time.time() - start_time
print(f"\n⏱️  TOTAL TIME WITHOUT CACHE: {total_time_no_cache:.2f} seconds")

# 4) Now caching (persist in MEMORY_AND_DISK) on the materialized DataFrame
print("\n" + "="*60)
print("--- SCENARIO 2: WITH PERSIST(MEMORY_AND_DISK) ---")
print("Persisting the materialized DataFrame and repeating analyses...\n")

final_cached = final_mat.persist(StorageLevel.MEMORY_AND_DISK)
# Trigger materialization of the cache
print("Triggering persist...")
final_cached.count()
print("✓ DataFrame persisted to MEMORY_AND_DISK\n")

start_time_cached = time.time()

# Analysis 1 with cache
print("Analysis 1: Calculating average salary by city (PERSISTED)...")
analysis1_cached_start = time.time()
avg_salary_by_city_cached = final_cached.filter(col("Lương (số)").isNotNull()) \
    .groupBy("Thành phố") \
    .agg(
        F.avg("Lương (số)").alias("Lương TB"),
        F.count("*").alias("Số lượng job")
    ) \
    .orderBy(F.desc("Lương TB"))

result1_cached = avg_salary_by_city_cached.collect()
analysis1_cached_time = time.time() - analysis1_cached_start
print(f"✓ Completed in {analysis1_cached_time:.2f} seconds")

# Analysis 2 with cache
print("\nAnalysis 2: Counting jobs by level (PERSISTED)...")
analysis2_cached_start = time.time()
job_count_by_level_cached = final_cached.groupBy("Cấp bậc") \
    .agg(F.count("*").alias("Số lượng")) \
    .orderBy(F.desc("Số lượng"))

result2_cached = job_count_by_level_cached.collect()
analysis2_cached_time = time.time() - analysis2_cached_start
print(f"✓ Completed in {analysis2_cached_time:.2f} seconds")

total_time_cached = time.time() - start_time_cached
print(f"\n⏱️  TOTAL TIME WITH PERSIST: {total_time_cached:.2f} seconds")

# BONUS ANALYSES (using persisted data)
print("\n" + "-"*60)
print("BONUS ANALYSES (using persisted data):")
print("-"*60)

print("\nAnalysis 3: Work mode distribution...")
work_mode_dist = final_cached.groupBy("Hình thức làm việc") \
    .agg(F.count("*").alias("Số lượng")) \
    .orderBy(F.desc("Số lượng"))
for row in work_mode_dist.limit(20).collect():
    print(f"  {row['Hình thức làm việc']}: {row['Số lượng']} jobs")

print("\nAnalysis 4: Top 5 companies by number of job postings...")
top_companies = final_cached.groupBy("Tên công ty") \
    .agg(F.count("*").alias("Số lượng job")) \
    .orderBy(F.desc("Số lượng job")) \
    .limit(5)
for row in top_companies.collect():
    print(f"  {row['Tên công ty']}: {row['Số lượng job']} jobs")

# Performance comparison
print("\n" + "="*60)
print("PERFORMANCE COMPARISON")
print("="*60)
speedup = (total_time_no_cache / total_time_cached) if total_time_cached > 0 else 0
print(f"Without materialized persist: {total_time_no_cache:.2f}s")
print(f"With persisted materialized DF:    {total_time_cached:.2f}s")
print(f"Speedup:         {speedup:.2f}x faster")
print(f"Time saved:      {total_time_no_cache - total_time_cached:.2f}s\n")
