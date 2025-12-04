
# # ==============================
# # PHASE 2: CACHING DEMONSTRATION
# # ==============================
# print("="*60)
# print("PHASE 2: CACHING & OPTIMIZATION DEMONSTRATION")
# print("="*60)

# # Demo WITHOUT caching first
# print("\n--- SCENARIO 1: WITHOUT CACHING ---")
# print("Performing multiple analyses on the same dataset...\n")

# start_time = time.time()

# # Analysis 1: Average salary by city
# print("Analysis 1: Calculating average salary by city...")
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("CachingDemo") \
#     .config("spark.python.worker.faulthandler.enabled", "true") \
#     .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
#     .getOrCreate()

# analysis1_start = time.time()
# avg_salary_by_city = final.filter(col("Lương (số)").isNotNull()) \
#     .groupBy("Thành phố") \
#     .agg(
#         F.avg("Lương (số)").alias("Lương TB"),
#         F.count("*").alias("Số lượng job")
#     ) \
#     .orderBy(F.desc("Lương TB"))

# result1 = avg_salary_by_city.collect()
# analysis1_time = time.time() - analysis1_start
# print(f"✓ Completed in {analysis1_time:.2f} seconds")
# print("\nTop 3 cities by average salary:")
# for row in avg_salary_by_city.limit(3).collect():
#     print(f"  {row['Thành phố']}: {row['Lương TB']:,.0f} VND ({row['Số lượng job']} jobs)")

# # Analysis 2: Job count by level
# print("\nAnalysis 2: Counting jobs by level...")
# analysis2_start = time.time()
# job_count_by_level = final.groupBy("Cấp bậc") \
#     .agg(F.count("*").alias("Số lượng")) \
#     .orderBy(F.desc("Số lượng"))

# result2 = job_count_by_level.limit(20).collect()

# analysis2_time = time.time() - analysis2_start
# print(f"✓ Completed in {analysis2_time:.2f} seconds")
# print("\nJob distribution by level:")
# for row in result2:
#     print(f"  {row['Cấp bậc']}: {row['Số lượng']} jobs")

# total_time_no_cache = time.time() - start_time
# print(f"\n⏱️  TOTAL TIME WITHOUT CACHE: {total_time_no_cache:.2f} seconds")

# # Now with caching
# print("\n" + "="*60)
# print("--- SCENARIO 2: WITH CACHING ---")
# print("Caching the DataFrame and repeating analyses...\n")

# # Cache the DataFrame
# print("Caching DataFrame...")
# final_cached = final.cache()
# final_cached.count()  # Trigger caching
# print("✓ DataFrame cached in memory\n")

# start_time_cached = time.time()

# # Analysis 1 with cache
# print("Analysis 1: Calculating average salary by city (CACHED)...")
# analysis1_cached_start = time.time()
# avg_salary_by_city_cached = final_cached.filter(col("Lương (số)").isNotNull()) \
#     .groupBy("Thành phố") \
#     .agg(
#         F.avg("Lương (số)").alias("Lương TB"),
#         F.count("*").alias("Số lượng job")
#     ) \
#     .orderBy(F.desc("Lương TB"))

# result1_cached = avg_salary_by_city_cached.collect()
# analysis1_cached_time = time.time() - analysis1_cached_start
# print(f"✓ Completed in {analysis1_cached_time:.2f} seconds")

# # Analysis 2 with cache
# print("\nAnalysis 2: Counting jobs by level (CACHED)...")
# analysis2_cached_start = time.time()
# job_count_by_level_cached = final_cached.groupBy("Cấp bậc") \
#     .agg(F.count("*").alias("Số lượng")) \
#     .orderBy(F.desc("Số lượng"))

# result2_cached = job_count_by_level_cached.collect()
# analysis2_cached_time = time.time() - analysis2_cached_start
# print(f"✓ Completed in {analysis2_cached_time:.2f} seconds")

# total_time_cached = time.time() - start_time_cached
# print(f"\n⏱️  TOTAL TIME WITH CACHE: {total_time_cached:.2f} seconds")

# # Additional analyses to demonstrate cache benefits
# print("\n" + "-"*60)
# print("BONUS ANALYSES (using cached data):")
# print("-"*60)

# # Analysis 3: Work mode distribution
# print("\nAnalysis 3: Work mode distribution...")
# work_mode_dist = final_cached.groupBy("Hình thức làm việc") \
#     .agg(F.count("*").alias("Số lượng")) \
#     .orderBy(F.desc("Số lượng"))
# print("Work mode distribution:")
# for row in work_mode_dist.limit(20).collect():
#     print(f"  {row['Hình thức làm việc']}: {row['Số lượng']} jobs")

# # Analysis 4: Top companies by job postings
# print("\nAnalysis 4: Top 5 companies by number of job postings...")
# top_companies = final_cached.groupBy("Tên công ty") \
#     .agg(F.count("*").alias("Số lượng job")) \
#     .orderBy(F.desc("Số lượng job")) \
#     .limit(5)
# print("Top companies:")
# for row in top_companies.collect():
#     print(f"  {row['Tên công ty']}: {row['Số lượng job']} jobs")

# # Performance comparison
# print("\n" + "="*60)
# print("PERFORMANCE COMPARISON")
# print("="*60)
# speedup = (total_time_no_cache / total_time_cached) if total_time_cached > 0 else 0
# print(f"Without caching: {total_time_no_cache:.2f}s")
# print(f"With caching:    {total_time_cached:.2f}s")
# print(f"Speedup:         {speedup:.2f}x faster")
# print(f"Time saved:      {total_time_no_cache - total_time_cached:.2f}s")

# print("\n" + "="*60)
# print("LESSONS LEARNED")
# print("="*60)
# print("""
# 1. CACHING STRATEGIES:
#    ✓ Cache DataFrames that are reused multiple times
#    ✓ Use .cache() after expensive transformations (joins, aggregations)
#    ✓ Call an action (count, collect) to materialize the cache
#    ✓ Unpersist when done to free memory: df.unpersist()

# 2. QUERY OPTIMIZATION:
#    ✓ Cached data is stored in memory → faster subsequent reads
#    ✓ Spark only reads source data once when cached
#    ✓ Aggregations on cached data are much faster
#    ✓ Check Spark UI to see cache usage in Storage tab

# 3. RESOURCE ALLOCATION:
#    ✓ Monitor memory usage (cached data uses memory)
#    ✓ Balance between cache benefit vs memory cost
#    ✓ For datasets used 2+ times: caching pays off
#    ✓ For one-time operations: caching adds overhead

# 4. BOTTLENECK IDENTIFICATION (via Spark UI):
#    ✓ SQL tab: See query execution plans
#    ✓ Storage tab: View cached RDDs/DataFrames
#    ✓ Jobs tab: Identify slow stages
#    ✓ Compare execution times with/without cache
#    ✓ URL: http://localhost:4040

# 5. BEST PRACTICES:
#    ✓ Cache after expensive operations (not before)
#    ✓ Use persist() for custom storage levels
#    ✓ Cache selectively (not everything)
#    ✓ Monitor Spark UI for cache effectiveness
# """)

# # ==============================
# # Write Output
# # ==============================
# print("\n" + "="*60)
# print("PHASE 3: WRITING OUTPUT")
# print("="*60)

# # Use the cached DataFrame for output
# final_count = final_cached.count()

# print("Writing to JSON...")
# final_cached.coalesce(1).write \
#     .mode("overwrite") \
#     .option("multiline", True) \
#     .json(output_path)

# print("="*60)
# print("✓ PROCESSING COMPLETE!")
# print("="*60)
# print(f"Total records: {final_count}")
# print(f"Output folder: {output_path}")
# print(f"Spark UI: http://localhost:4040")
# print("="*60)

# # Preview results
# print("\nSample data (first 3 rows):")
# final_cached.select(
#     "Tên công ty", "Thành phố", "Vị trí tuyển dụng", 
#     "Cấp bậc", "Lương (VND)", "Hình thức làm việc"
# ).show(3, truncate=50)

# # Unpersist cache
# print("\nUnpersisting cache...")
# final_cached.unpersist()

# spark.stop()
# print("✓ Spark session stopped.")
# print("\nCheck Spark UI before it closes to see execution details!")