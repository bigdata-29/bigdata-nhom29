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


# ... (Phần code đo đạc hiệu năng ở trên giữ nguyên)

print(f"Speedup:       {speedup:.2f}x faster")

# ==========================================
# GIỮ SPARK UI MỞ ĐỂ CHỤP ẢNH
# ==========================================
print("\n" + "="*60)
print("SPARK UI ĐANG CHẠY!")
print("Truy cập ngay: http://localhost:4040")
print("Vào tab 'Storage' để xem dữ liệu đã được Cache vào RAM.")
print("Vào tab 'Stages' để xem biểu đồ DAG.")
print("="*60)

# Chương trình sẽ dừng ở đây cho đến khi bạn bấm Enter ở terminal
input(">>> BẤM PHÍM ENTER ĐỂ TẮT SPARK VÀ KẾT THÚC CHƯƠNG TRÌNH...")

# Dọn dẹp
spark.stop()


# # ==============================
# # PHASE 2: CACHING & OPTIMIZATION DEMONSTRATION (FIXED & ROBUST) for CLEANED_RECRUITMENT_DATA.JSON
# # ==============================
# import os
# import time
# import shutil
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.functions import col
# from pyspark import StorageLevel

# # ============================
# # SPARK SETUP
# # ============================

# print("Spark session started!")

# spark = (
#     SparkSession.builder
#     .appName("CachingDemo")
#     .config("spark.sql.shuffle.partitions", 8)
#     .config("spark.python.worker.faulthandler.enabled", "true")
#     .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
#     # Cấu hình bỏ qua lỗi checksum nếu file bị lỗi nhẹ do copy
#     .config("spark.hadoop.fs.file.impl.disable.cache", "true")
#     .getOrCreate()
# )

# # Tắt check checksum ở mức Hadoop Context (an toàn khi chạy local)
# spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
# spark.sparkContext.setLocalProperty("spark.hadoop.io.skip.checksum.errors", "true")

# print("="*60)
# print("PHASE 2: CACHING & OPTIMIZATION DEMONSTRATION (FIXED)")
# print("="*60)

# # Đường dẫn file (sửa lại đường dẫn file input của bạn nếu cần)
# input_file = "../preprocess_data/cleaned_recruitment_data.json" 
# _temp_parquet = "tmp_final_materialized_parquet"

# # 1) Materialize final (write -> read) to remove UDFs from the logical plan
# print(f"\n[1] Reading JSON from: {input_file}")

# try:
#     df = spark.read.json(input_file)
# except Exception as e:
#     print(f"Lỗi đọc file JSON: {e}")
#     exit(1)

# # ====================================================
# # BƯỚC MỚI: CHUẨN HÓA TÊN CỘT (HANDLE DYNAMIC SCHEMA)
# # ====================================================
# print("[2] Standardizing columns (Xử lý tên cột khác nhau)...")
# current_cols = df.columns

# # Xử lý: Thành phố vs Địa chỉ
# if "Thành phố" not in current_cols:
#     if "Địa chỉ" in current_cols:
#         print(" -> Detect 'Địa chỉ', renaming to 'Thành phố'")
#         df = df.withColumnRenamed("Địa chỉ", "Thành phố")
#     elif "location" in current_cols:
#          df = df.withColumnRenamed("location", "Thành phố")
#     else:
#         print(" WARNING: Không tìm thấy cột thông tin Thành phố/Địa chỉ!")

# # Xử lý: Cấp bậc vs cap_bac
# if "Cấp bậc" not in current_cols:
#     if "cap_bac" in current_cols:
#         print(" -> Detect 'cap_bac', renaming to 'Cấp bậc'")
#         df = df.withColumnRenamed("cap_bac", "Cấp bậc")
#     elif "level" in current_cols:
#         df = df.withColumnRenamed("level", "Cấp bậc")

# # Xử lý: Lương (số) vs luong_min/max
# if "Lương (số)" not in current_cols:
#     if "luong_min" in current_cols and "luong_max" in current_cols:
#         print(" -> Detect 'luong_min/max', calculating 'Lương (số)'")
#         # Tính lương trung bình, ép kiểu về Long để tránh lỗi null
#         df = df.withColumn("Lương (số)", (col("luong_min").cast("long") + col("luong_max").cast("long")) / 2)
#     elif "salary" in current_cols:
#         df = df.withColumnRenamed("salary", "Lương (số)")

# # Fill null cho các cột quan trọng để tránh lỗi khi agg
# df = df.na.fill(0, ["Lương (số)"]).na.fill("Unknown", ["Thành phố", "Cấp bậc", "Tên công ty"])

# print(f"Loaded and standardized dataset: {df.count()} rows")

# # Xóa thư mục temp cũ nếu tồn tại để tránh lỗi
# if os.path.exists(_temp_parquet):
#     shutil.rmtree(_temp_parquet, ignore_errors=True)

# # Write parquet to materialize (Bước này giúp Clean Plan & Cache hiệu quả hơn)
# print(f"Writing temporary parquet to {_temp_parquet}...")
# df.write.mode("overwrite").parquet(_temp_parquet)

# # Read back native Spark parquet
# final_mat = spark.read.parquet(_temp_parquet)
# print(f"✓ Materialized DataFrame loaded from Parquet. Count = {final_mat.count()}")

# # ====================================================
# # BẮT ĐẦU DEMO CACHING
# # ====================================================

# # 3) Demo WITHOUT caching first
# print("\n--- SCENARIO 1: WITHOUT CACHING (Reading from Disk each time) ---")
# start_time = time.time()

# # Analysis 1
# analysis1_start = time.time()
# avg_salary_by_city = final_mat.filter(col("Lương (số)") > 0) \
#     .groupBy("Thành phố") \
#     .agg(
#         F.avg("Lương (số)").alias("Lương TB"),
#         F.count("*").alias("Số lượng job")
#     ) \
#     .orderBy(F.desc("Lương TB"))

# result1 = avg_salary_by_city.collect()
# analysis1_time = time.time() - analysis1_start
# print(f"✓ Completed Analysis 1 (Salary) in {analysis1_time:.4f} seconds")

# # In kết quả top 3
# print("\nTop 3 cities by average salary:")
# for row in avg_salary_by_city.limit(3).collect():
#     print(f"  {row['Thành phố']}: {row['Lương TB']:,.0f} VND ({row['Số lượng job']} jobs)")

# # Analysis 2
# analysis2_start = time.time()
# job_count_by_level = final_mat.groupBy("Cấp bậc") \
#     .agg(F.count("*").alias("Số lượng")) \
#     .orderBy(F.desc("Số lượng"))
# result2 = job_count_by_level.collect()
# analysis2_time = time.time() - analysis2_start
# print(f"✓ Completed Analysis 2 (Level) in {analysis2_time:.4f} seconds")

# total_time_no_cache = time.time() - start_time
# print(f"\n⏱️  TOTAL TIME WITHOUT CACHE: {total_time_no_cache:.4f} seconds")


# # 4) Now caching (persist in MEMORY_AND_DISK)
# print("\n" + "="*60)
# print("--- SCENARIO 2: WITH PERSIST(MEMORY_AND_DISK) ---")
# print("Persisting the DataFrame into RAM...\n")

# # .unpersist() để đảm bảo sạch sẽ trước khi test
# final_mat.unpersist()
# final_cached = final_mat.persist(StorageLevel.MEMORY_AND_DISK)

# # Trigger Action để Spark thực sự load dữ liệu vào Cache
# t_cache = time.time()
# count_check = final_cached.count() 
# print(f"Triggering Cache (Count action): {count_check} rows. Time: {time.time() - t_cache:.2f}s")
# print("✓ DataFrame persisted to MEMORY_AND_DISK\n")

# start_time_cached = time.time()

# # Analysis 1 with cache
# print("Analysis 1: Calculating average salary (CACHED)...")
# analysis1_cached_start = time.time()
# avg_salary_by_city_cached = final_cached.filter(col("Lương (số)") > 0) \
#     .groupBy("Thành phố") \
#     .agg(
#         F.avg("Lương (số)").alias("Lương TB"),
#         F.count("*").alias("Số lượng job")
#     ) \
#     .orderBy(F.desc("Lương TB"))

# result1_cached = avg_salary_by_city_cached.collect()
# analysis1_cached_time = time.time() - analysis1_cached_start
# print(f"✓ Completed in {analysis1_cached_time:.4f} seconds")

# # Analysis 2 with cache
# print("Analysis 2: Counting jobs by level (CACHED)...")
# analysis2_cached_start = time.time()
# job_count_by_level_cached = final_cached.groupBy("Cấp bậc") \
#     .agg(F.count("*").alias("Số lượng")) \
#     .orderBy(F.desc("Số lượng"))

# result2_cached = job_count_by_level_cached.collect()
# analysis2_cached_time = time.time() - analysis2_cached_start
# print(f"✓ Completed in {analysis2_cached_time:.4f} seconds")

# total_time_cached = time.time() - start_time_cached
# print(f"\n⏱️  TOTAL TIME WITH PERSIST: {total_time_cached:.4f} seconds")

# # Performance comparison
# print("\n" + "="*60)
# print("PERFORMANCE COMPARISON")
# print("="*60)
# # Tránh chia cho 0
# if total_time_cached < 0.001: total_time_cached = 0.001

# speedup = total_time_no_cache / total_time_cached
# print(f"Without Cache: {total_time_no_cache:.4f}s")
# print(f"With Cache:    {total_time_cached:.4f}s")
# print(f"Speedup:       {speedup:.2f}x faster")




# # ... (Phần code đo đạc hiệu năng ở trên giữ nguyên)

# print(f"Speedup:       {speedup:.2f}x faster")

# # ==========================================
# # GIỮ SPARK UI MỞ ĐỂ CHỤP ẢNH
# # ==========================================
# print("\n" + "="*60)
# print("SPARK UI ĐANG CHẠY!")
# print("Truy cập ngay: http://localhost:4040")
# print("Vào tab 'Storage' để xem dữ liệu đã được Cache vào RAM.")
# print("Vào tab 'Stages' để xem biểu đồ DAG.")
# print("="*60)

# # Chương trình sẽ dừng ở đây cho đến khi bạn bấm Enter ở terminal
# input(">>> BẤM PHÍM ENTER ĐỂ TẮT SPARK VÀ KẾT THÚC CHƯƠNG TRÌNH...")

# # Dọn dẹp
# spark.stop()