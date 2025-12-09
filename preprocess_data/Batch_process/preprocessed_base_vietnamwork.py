# ===================================
# JOB PREPROCESSING WITH BENEFITS FIX
# ===================================

import os
from pyspark import SparkConf
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
# spark = (
#     SparkSession.builder
#     .appName("BatchProcessing")
#     .config("spark.hadoop.io.nativeio.disable", "true")
#     .config("spark.hadoop.fs.checksum.file", "false")
#     .config("spark.driver.memory", "4g")
#     .config("spark.executor.memory", "4g")
#     .getOrCreate()
# )
spark = SparkSession.builder \
    .appName("VietnamWorkPreprocessing") \
    .master("k8s://https://<K8S_API_SERVER>") \
    .config("spark.kubernetes.container.image", "<spark-image>") \
    .config("spark.executor.instances", 2) \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .getOrCreate()


spark = SparkSession.builder.config(conf=conf).getOrCreate()

print("Spark UI: http://localhost:4040")
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



# # 3. Save Parquet partitioned by dia_chi and ngay_dang_tuyen
# print("\nSaving Parquet (partitioned)...")

# # Normalize partition columns
# final_save = final \
#     .withColumn("dia_chi_partition", F.regexp_replace(F.col("Địa chỉ"), r"[^a-zA-Z0-9\s\.-]", "")) \
#     .withColumn(
#         "ngay_partition",
#         F.to_date(F.col("Ngày đăng tuyển (dd/MM/yy)"), "dd/MM/yy")
#     )

# # parquet_output_path = "hdfs://localhost:8020/processed_jobs"   # hoặc "s3a://bucket/processed_jobs" khi dùng MinIO

# print(spark.sparkContext._conf.get("spark.hadoop.fs.defaultFS"))

# output_path = "hdfs://localhost:9000/processed_jobs"


# final_save.write \
#     .mode("overwrite") \
#     .partitionBy("dia_chi_partition", "ngay_partition") \
#     .parquet(output_path)

# print("✓ Parquet saved to HDFS:", output_path)


# print(f"✓ Partitioned Parquet saved to: {parquet_output_path}")



# # 3. Save Parquet partitioned by dia_chi and ngay_dang_tuyen vào folder project
# print("\nSaving Parquet (partitioned)...")

# # Normalize partition columns
# final_save = final \
#     .withColumn("dia_chi_partition", F.regexp_replace(F.col("Địa chỉ"), r"[^a-zA-Z0-9\s\.-]", "")) \
#     .withColumn(
#         "ngay_partition",
#         F.to_date(F.col("Ngày đăng tuyển (dd/MM/yy)"), "dd/MM/yy")
#     )

# parquet_output_path = "hdfs://localhost:8020/processed_jobs"   # hoặc "s3a://bucket/processed_jobs" khi dùng MinIO

# final_save.write \
#     .mode("overwrite") \
#     .partitionBy("dia_chi_partition", "ngay_partition") 
# df.write.mode("overwrite").parquet(r"D:\20251\bigdata-nhom29\preprocess_data\output_parquet")


# print(f"✓ Partitioned Parquet saved to: {parquet_output_path}")

