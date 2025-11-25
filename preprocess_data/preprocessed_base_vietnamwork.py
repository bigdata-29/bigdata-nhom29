# ===================================
# JOB PREPROCESSING SCRIPT (VS CODE)
# ===================================

# STEP 1: Setup (run once per session)
print("Installing dependencies...")
# Nếu chạy trên Colab thì bật 2 dòng dưới:
# !apt-get install openjdk-11-jdk-headless -qq > /dev/null
# !pip install -q pyspark

import os
from pyspark.sql import SparkSession, functions as F, types as T
import re
from pyspark.sql.functions import sha2, concat_ws, col

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
print("Setup complete!\n")

# STEP 2: Create Spark Session
print("Starting Spark session...")
spark = SparkSession.builder \
    .appName("job_preprocess") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

print("Spark session started!\n")

# ==============================
# Configuration
# ==============================
input_path = "vietnamworks_jobs_20251028_144306.json"
output_path = "data"

# ==============================
# Load JSON
# ==============================
print(f"Loading data from: {input_path}")
df_raw = spark.read.option("multiline", True).json(input_path)
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


def _benefits_to_string(x):
    if x is None:
        return None
    if isinstance(x, list):
        vals = []
        for item in x:
            if isinstance(item, dict):
                title = item.get('tieu_de') or item.get('title') or ""
                desc = item.get('mo_ta') or item.get('description') or ""
                combined = f"{title}: {desc}".strip().strip(":")
                if combined:
                    vals.append(combined)
            else:
                vals.append(str(item))
        return " | ".join(filter(None, vals))
    return str(x)

benefits_to_string_udf = F.udf(_benefits_to_string, T.StringType())

# ==============================
# Data Processing
# ==============================
print("Processing data...")

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

# Deduplicate
df = df.withColumn(
    "row_key",
    F.when(F.col("url").isNotNull(), F.col("url"))
     .otherwise(sha2(concat_ws("||", col("company"), col("title"), col("location")), 256))
).dropDuplicates(["row_key"])

# Apply transformations
print("Applying UDFs...")
df = df.withColumn("Lương (VND)", salary_to_vnd_udf("salary_raw"))
df = df.withColumn("Hình thức làm việc", infer_work_mode_udf("job_description", "location"))
df = df.withColumn("Ngày đăng tuyển (dd/MM/yy)", normalize_date_udf("posted_date_raw"))
df = df.withColumn("Phúc lợi", benefits_to_string_udf("benefits_raw"))
df = df.withColumn("Thời gian làm việc", F.lit("Giờ hành chính"))

final = df.select(
    F.col('company').alias('Tên công ty'),
    F.col('location').alias('Địa chỉ'),
    F.col('title').alias('Vị trí tuyển dụng'),
    "Lương (VND)",
    "Thời gian làm việc",
    "Hình thức làm việc",
    F.col('Ngày đăng tuyển (dd/MM/yy)'),
    F.col('job_description').alias('Mô tả công việc'),
    "Phúc lợi",
    F.col('url').alias('Đường dẫn')
).fillna({
    'Tên công ty': 'Không rõ',
    'Vị trí tuyển dụng': 'Không rõ',
    'Hình thức làm việc': 'Trực tiếp',
    "Thời gian làm việc": "Giờ hành chính"
})

# ==============================
# Write Output (JSON)
# ==============================
print("Writing to JSON...")
final_count = final.count()

final.coalesce(1).write \
    .mode("overwrite") \
    .option("multiline", True) \
    .json(output_path)

print("="*50)
print("PROCESSING COMPLETE! (JSON Export)")
print("="*50)
print("Total records:", final_count)
print("Output folder:", output_path)
print("="*50)

# Preview results
print("\nSample data (first 5 rows):")
final.show(5, truncate=False)

spark.stop()
print("Spark session stopped.")
