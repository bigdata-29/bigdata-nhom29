# ===================================
# JOB PREPROCESSING WITH CACHING OPTIMIZATION
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
import time

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
print("Setup complete!\n")

# STEP 2: Create Spark Session with UI enabled
print("Starting Spark session...")
spark = SparkSession.builder \
    .appName("job_preprocess_with_cache") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

print("Spark session started!")
print(f"Spark UI available at: http://localhost:4040\n")

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


def _extract_salary_numeric(salary_str):
    """Extract numeric salary value for calculations (take min value from range)"""
    if salary_str is None or salary_str == "Thương lượng":
        return None
    
    nums = re.findall(r'(\d+)', salary_str)
    if nums:
        return int(nums[0])
    return None

extract_salary_numeric_udf = F.udf(_extract_salary_numeric, T.IntegerType())


def _infer_level(title):
    """Infer job level from title"""
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
    """Extract city from location string"""
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


def _benefits_to_array(x):
    """Extract only benefit titles as an array"""
    if x is None:
        return None
    if isinstance(x, list):
        titles = []
        for item in x:
            if isinstance(item, dict):
                title = item.get('tieu_de') or item.get('title') or ""
                if title and title.strip():
                    titles.append(title.strip())
            elif isinstance(item, str) and item.strip():
                titles.append(item.strip())
        return titles if titles else None
    return None

benefits_to_array_udf = F.udf(_benefits_to_array, T.ArrayType(T.StringType()))

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

# Deduplicate
df = df.withColumn(
    "row_key",
    F.when(F.col("url").isNotNull(), F.col("url"))
     .otherwise(sha2(concat_ws("||", col("company"), col("title"), col("location")), 256))
).dropDuplicates(["row_key"])

# Apply transformations
print("Applying transformations...")
df = df.withColumn("salary_vnd", salary_to_vnd_udf("salary_raw"))
df = df.withColumn("work_mode", infer_work_mode_udf("job_description", "location"))
df = df.withColumn("posted_date", normalize_date_udf("posted_date_raw"))
df = df.withColumn("benefits", benefits_to_array_udf("benefits_raw"))
df = df.withColumn("working_hours", F.lit("Giờ hành chính"))

# Add enrichment columns for analysis
df = df.withColumn("city", extract_city_udf("location"))
df = df.withColumn("job_level", infer_level_udf("title"))
df = df.withColumn("salary_numeric", extract_salary_numeric_udf("salary_vnd"))

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
    F.col('benefits').alias('Phúc lợi'),
    F.col('url').alias('Đường dẫn')
).fillna({
    'Tên công ty': 'Không rõ',
    'Vị trí tuyển dụng': 'Không rõ',
    'Hình thức làm việc': 'Trực tiếp',
    'Thời gian làm việc': 'Giờ hành chính',
    'Thành phố': 'Không xác định',
    'Cấp bậc': 'Không xác định'
})

print(f"✓ Processed {final.count()} records\n")

