# ===================================
# JOB PREPROCESSING WITH BENEFITS FIX
# ===================================

import os
from pyspark.sql import SparkSession, functions as F, types as T
import re
from pyspark.sql.functions import sha2, concat_ws, col
import json
import time


os.environ["HADOOP_HOME"] = "C:\\hadoop-3.3.5"
os.environ["hadoop.home.dir"] = "C:\\hadoop-3.3.5"
os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python311\venv_spark311\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python311\venv_spark311\Scripts\python.exe"
os.environ["JAVA_HOME"] = r"C:\Program Files\RedHat\java-17-openjdk-17.0.10.0.7-2.jre"

print("Starting Spark session...")
spark = (
    SparkSession.builder
    .appName("BatchProcessing")
    .config("spark.hadoop.io.nativeio.disable", "true")
    .config("spark.hadoop.fs.checksum.file", "false")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

print(f"Spark UI: http://localhost:4040\n")

# ==============================
# Configuration
# ==============================
input_path = "vietnamworks_jobs_20251028_144306.json"

# ==============================
# Load JSON
# ==============================
print(f"Loading data from: {input_path}")
df_raw = spark.read.option("multiline", True) \
                   .option("inferSchema", True) \
                   .json(input_path)

print(f"Loaded {df_raw.count()} raw records\n")

# ==============================
# UDF Definitions
# ==============================
USD_TO_VND = 23500

def _salary_to_vnd(s):
    if s is None or not s:
        return None
    s2 = str(s).lower().replace(",", "").strip()
    if "thương lượng" in s2 or "negotiable" in s2:
        return "Thương lượng"
    is_usd = any(x in s2 for x in ["$", "usd"])
    is_year = any(x in s2 for x in ["năm", "/year", "per year", "yr", "/yr"])
    s2 = re.sub(r"/tháng|tháng|/month|per month|/yr|/năm|năm|year|/year", " ", s2)
    pattern = r"(\d+(?:[\.,]\d+)?)(?:\s*)(tr|m|k)?"
    nums = re.findall(pattern, s2)
    if len(nums) == 0:
        return None
    values = []
    for num_str, unit in nums:
        num_str = num_str.replace(",", ".")
        try:
            num = float(num_str)
        except:
            continue
        if unit == "tr" or unit == "m":
            num *= 1_000_000
        elif unit == "k":
            num *= 1_000
        if is_usd:
            num *= USD_TO_VND
        values.append(int(num))
    if not values:
        return None
    has_toi = any(x in s2 for x in ["tới", "to"])
    if has_toi:
        v = values[0]
        return f"Tới {v} VND/năm" if is_year else f"Tới {v} VND/tháng"
    if len(values) >= 2:
        return f"{min(values)} - {max(values)} VND"
    return f"{values[0]} VND"

salary_to_vnd_udf = F.udf(_salary_to_vnd, T.StringType())

def _extract_salary_numeric(salary_str):
    if salary_str is None or salary_str == "Thương lượng":
        return None
    nums = re.findall(r'(\d+)', salary_str)
    if nums:
        return int(nums[0])
    return None

extract_salary_numeric_udf = F.udf(_extract_salary_numeric, T.IntegerType())

def _infer_level(title):
    if title is None:
        return "Không xác định"
    title_lower = title.lower()
    if any(x in title_lower for x in ["intern", "thực tập"]):
        return "Thực tập sinh"
    elif any(x in title_lower for x in ["fresher", "junior", "entry"]):
        return "Fresher/Junior"
    elif any(x in title_lower for x in ["senior", "sr.", "chuyên viên cao cấp"]):
        return "Senior"
    elif any(x in title_lower for x in ["lead", "leader", "trưởng nhóm", "trưởng phòng"]):
        return "Lead/Manager"
    elif any(x in title_lower for x in ["director", "giám đốc", "cto", "ceo", "vp"]):
        return "Director/C-level"
    else:
        return "Mid-level"

infer_level_udf = F.udf(_infer_level, T.StringType())

def _extract_city(location):
    if location is None:
        return "Không xác định"
    loc_lower = location.lower()
    if "hà nội" in loc_lower or "hanoi" in loc_lower:
        return "Hà Nội"
    elif "hồ chí minh" in loc_lower or "tp.hcm" in loc_lower or "saigon" in loc_lower:
        return "TP. Hồ Chí Minh"
    elif "đà nẵng" in loc_lower or "da nang" in loc_lower:
        return "Đà Nẵng"
    elif "cần thơ" in loc_lower:
        return "Cần Thơ"
    elif "hải phòng" in loc_lower:
        return "Hải Phòng"
    else:
        return "Tỉnh thành khác"

extract_city_udf = F.udf(_extract_city, T.StringType())

def _infer_work_mode(desc, loc):
    desc = desc or ""
    loc = loc or ""
    txt = (desc + " " + loc).lower()
    if any(x in txt for x in ["remote", "làm từ xa", "work from home", "work-from-home"]):
        return "Làm từ xa"
    if any(x in txt for x in ["hybrid", "bán thời gian tại văn phòng", "kết hợp"]):
        return "Kết hợp"
    return "Trực tiếp"

infer_work_mode_udf = F.udf(_infer_work_mode, T.StringType())

def _normalize_date(s):
    if not s:
        return None
    s = str(s)
    m = re.search(r'(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})', s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3)) % 100
        return f"{d:02d}/{mo:02d}/{y:02d}"
    m2 = re.search(r'(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})', s)
    if m2:
        y, mo, d = int(m2.group(1)) % 100, int(m2.group(2)), int(m2.group(3))
        return f"{d:02d}/{mo:02d}/{y:02d}"
    return None

normalize_date_udf = F.udf(_normalize_date, T.StringType())

# FIX: Convert benefits array to comma-separated string (NO DEBUG PRINT)
def _benefits_to_string(x):
    """Convert benefits array to comma-separated string"""
    if x is None:
        return "Không có thông tin"
    
    # Handle if it's a Row object from Spark
    if hasattr(x, 'asDict'):
        x = x.asDict()
    
    if isinstance(x, list):
        titles = []
        for item in x:
            # Handle Row objects - Spark passes Row objects, not dicts!
            if hasattr(item, 'tieu_de'):
                title = item.tieu_de
            elif hasattr(item, 'title'):
                title = item.title
            elif isinstance(item, dict):
                title = item.get('tieu_de') or item.get('title') or ""
            elif isinstance(item, str):
                title = item
            else:
                continue
            
            if title and str(title).strip():
                titles.append(str(title).strip())
        
        if titles:
            return ", ".join(titles)
    
    return "Không có thông tin"

benefits_to_string_udf = F.udf(_benefits_to_string, T.StringType())

def _benefits_to_list(x):
    """Convert benefits array into Python list of strings"""
    if x is None:
        return []

    # Spark Row → dict
    if hasattr(x, 'asDict'):
        x = x.asDict()

    result = []

    if isinstance(x, list):
        for item in x:
            title = None

            if hasattr(item, 'tieu_de'):
                title = item.tieu_de
            elif hasattr(item, 'title'):
                title = item.title
            elif isinstance(item, dict):
                title = item.get('tieu_de') or item.get('title')
            elif isinstance(item, str):
                title = item

            if title and str(title).strip():
                result.append(str(title).strip())

    return result

benefits_to_list_udf = F.udf(_benefits_to_list, T.ArrayType(T.StringType()))

def _shorten_description(desc):
    if not desc:
        return None
    lines = desc.split("\n")

    summary = lines[0]

    bullets = [line for line in lines if line.strip().startswith("•")]
    bullets = bullets[:5]

    short = summary + "\n" + "\n".join(bullets)
    return short.strip()

shorten_desc_udf = F.udf(_shorten_description, T.StringType())








# ==============================
# Data Processing
# ==============================
print("="*60)
print("PHASE 1: DATA PREPROCESSING")
print("="*60)

df = df_raw.select(
    F.col('cong_ty').alias('company'),
    F.coalesce(F.col('dia_diem'), F.concat_ws(', ', F.col('dia_diem_chi_tiet'))).alias('location'),
    F.col('tieu_de').alias('title'),
    F.col('luong').alias('salary_raw'),
    F.col('mo_ta').alias('job_description'),
    F.col('phuc_loi').alias('benefits_raw'),
    F.coalesce(F.col('ngay_dang_chi_tiet'), F.col('ngay_dang')).alias('posted_date_raw'),
    F.col('url')
)

# DEBUG
print("\n=== Sample benefits_raw (first 3) ===")
print("(Skipping raw display to avoid encoding issues)")
print(f"Total records with benefits_raw: {df.filter(F.col('benefits_raw').isNotNull()).count()}")

# Deduplicate
df = df.withColumn(
    "row_key",
    F.when(F.col("url").isNotNull(), F.col("url"))
     .otherwise(sha2(concat_ws("||", col("company"), col("title"), col("location")), 256))
).dropDuplicates(["row_key"])




# Apply transformations
print("\nApplying transformations...")
df = df.withColumn("salary_vnd", salary_to_vnd_udf("salary_raw"))
df = df.withColumn("work_mode", infer_work_mode_udf("job_description", "location"))
df = df.withColumn("posted_date", normalize_date_udf("posted_date_raw"))
df = df.withColumn("benefits_list", benefits_to_list_udf("benefits_raw"))
df = df.withColumn("benefits", F.concat_ws(", ", "benefits_list"))
df = df.withColumn("job_description_short", shorten_desc_udf("job_description"))
df = df.withColumn("working_hours", F.lit("Giờ hành chính"))
df = df.withColumn("city", extract_city_udf("location"))
df = df.withColumn("job_level", infer_level_udf("title"))
df = df.withColumn("salary_numeric", extract_salary_numeric_udf("salary_vnd"))

print("✓ Transformations applied successfully")

# DEBUG after transformation
print("\n=== Checking benefits after UDF ===")
benefits_count = df.filter(F.col("benefits") != "Không có thông tin").count()
print(f"Records with benefits extracted: {benefits_count}")

final = df.select(
    F.col('company').alias('Tên công ty'),
    F.col('location').alias('Địa chỉ'),
    F.col('city').alias('Thành phố'),
    F.col('title').alias('Vị trí tuyển dụng'),
    F.col('job_level').alias('Cấp bậc'),
    F.col('salary_vnd').alias('Lương (VND)'),
    F.col('salary_numeric').alias('Lương (số)'),
    F.col('working_hours').alias('Thời gian làm việc'),
    F.col('work_mode').alias('Hình thức làm việc'),
    F.col('posted_date').alias('Ngày đăng tuyển (dd/MM/yy)'),
    F.col('job_description').alias('Mô tả công việc'),
    F.col('benefits_list').alias('Phúc lợi'),
    F.col('url').alias('Đường dẫn')
).fillna({
    'Tên công ty': 'Không rõ',
    'Vị trí tuyển dụng': 'Không rõ',
    'Hình thức làm việc': 'Trực tiếp',
    'Thời gian làm việc': 'Giờ hành chính',
    'Thành phố': 'Không xác định',
    'Cấp bậc': 'Không xác định'
    
})


print(f"\n✓ Processed {final.count()} records")

# Final verification
print("\n=== FINAL VERIFICATION (5 samples) ===")
final.select("Tên công ty", "Vị trí tuyển dụng", "Phúc lợi").show(5, truncate=50)

# ======================================
# SAVE OUTPUTS
# ======================================
print("\n" + "="*60)
print("SAVING OUTPUTS")
print("="*60)

# 1. JSON output
preview_output = "preprocessed_preview.json"
final.coalesce(1) \
     .write \
     .mode("overwrite") \
     .option("multiline", True) \
     .json(preview_output)
print(f"✓ JSON saved to: {preview_output}")

   
# 3. Manual JSON check (write a few records to console)
print("\n=== First record as JSON ===")
first_record = final.limit(1).toJSON().collect()[0]
print(json.dumps(json.loads(first_record), indent=2, ensure_ascii=False))

print("\n✅ Processing complete! Check the files above.")






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