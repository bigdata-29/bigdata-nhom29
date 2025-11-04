#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PySpark job preprocessing script
Inputs: a JSON file of job postings (one JSON object per job or an array)
Outputs: JSON result (1 file) in folder `data_json`

Run:
  spark-submit pyspark_job_preprocess.py <input_json_path>
"""

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import sha2, concat_ws
import sys
import re

# -----------------------------------------------------------------------------
# Init Spark
# -----------------------------------------------------------------------------
spark = SparkSession.builder.appName("job_preprocess").getOrCreate()

if len(sys.argv) < 2:
    print("Usage: spark-submit pyspark_job_preprocess.py <input_json_path>")
    sys.exit(1)

input_path = sys.argv[1]
df_raw = spark.read.option("multiline", True).json(input_path)

# -----------------------------------------------------------------------------
# Helper Dictionaries / UDFs
# -----------------------------------------------------------------------------
city_map = {
    r"(?i)^(hcm|tp\.hcm|ho chi minh|hochiminh|hồ chí minh|hcmc)$": "Ho Chi Minh City",
    r"(?i)^(hn|hanoi|ha noi|hà nội)$": "Hanoi",
    r"(?i)^(dn|da nang|đà nẵng)$": "Da Nang",
    r"(?i)^(hp|haiphong|hải phòng)$": "Hai Phong",
}

@F.udf(T.StringType())
def normalize_city(city):
    if not city: return None
    s = city.strip()
    for patt, val in city_map.items():
        if re.search(patt, s): return val
    return s.title()

@F.udf(T.StructType([
    T.StructField("min_vnd", T.LongType()),
    T.StructField("max_vnd", T.LongType()),
    T.StructField("currency", T.StringType())
]))
def parse_salary(s):
    if not s: return (None, None, None)
    s = s.replace('\u00a0', ' ').lower().strip()

    currency = None
    if "usd" in s: currency = "USD"
    elif any(c in s for c in ["vnd", "₫", "đ", "dong", "tr"]): currency = "VND"

    s = re.sub(r"/tháng|tháng|per month|/month", "", s)
    nums = []
    parts = re.split(r"[-–—to]+", s)

    for p in parts:
        p = re.sub(r"[₫$,]|vnd|usd|đ|dong|triệu|trieu|tr", "", p.replace(" ", ""))
        p2 = p.replace(",", "")
        try:
            nums.append(int(float(p2)))
        except:
            pass

    if not nums: return (None, None, currency)
    if len(nums) == 1: return (nums[0], nums[0], currency)
    return (min(nums), max(nums), currency)

@F.udf(T.ArrayType(T.StringType()))
def extract_skills(k1, k2, desc, req):
    skills, common = [], {"java","python","sql","linux","windows","aws","azure","gcp","docker","kubernetes"}
    for src in [k1, k2]:
        if src:
            if isinstance(src, list):
                skills += [s.strip() for s in src]
            else:
                skills += re.split(r"[,;|]+", src)

    text = f"{desc} {req}"
    tokens = re.findall(r"[A-Za-z0-9\+\#\-]{2,}", text)
    skills += [t for t in tokens if t.lower() in common]

    out, seen = [], set()
    for s in skills:
        if s and s.lower() not in seen:
            seen.add(s.lower())
            out.append(s)
    return out

@F.udf(T.StringType())
def infer_work_mode(desc, loc):
    t = f"{desc} {loc}".lower()
    return "remote" if any(x in t for x in ["remote", "làm từ xa", "work from home"]) else "on-site"

@F.udf(T.StringType())
def normalize_date(d):
    if not d: return None
    d = d.strip()
    if "hôm" in d.lower(): return None

    m = re.search(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})", d)
    if m:
        dd, mm, yy = int(m.group(1)), int(m.group(2)), int(m.group(3)) % 100
        return f"{dd:02d}/{mm:02d}/{yy:02d}"
    return None

def benefits_to_string(b):
    if not b: return None
    if isinstance(b, list):
        return " | ".join(f"{i.get('tieu_de', '')}: {i.get('mo_ta', '')}" for i in b)
    return str(b)

spark.udf.register("benefits_to_string", F.udf(benefits_to_string, T.StringType()))

# -----------------------------------------------------------------------------
# Transform
# -----------------------------------------------------------------------------
df = df_raw.select(
    F.coalesce("cong_ty", F.lit(None)).alias("company"),
    F.coalesce("dia_diem", F.concat_ws(", ", "dia_diem_chi_tiet")).alias("location"),
    "tieu_de", "luong", "mo_ta", "phuc_loi",
    F.coalesce("ngay_dang_chi_tiet", "ngay_dang").alias("date"),
    "ky_nang", "ky_nang_chi_tiet", "yeu_cau", "url", "cap_bac"
)

df = df.withColumn("row_key",
    F.when(F.col("url").isNotNull(), "url")
     .otherwise(sha2(concat_ws("||","company","tieu_de","location"), 256))
)

df = df.dropDuplicates(["row_key"])

parsed = (
    df.withColumn("city", normalize_city("location"))
      .withColumn("salary", F.expr("parse_salary(luong)"))
      .withColumn("skills", F.expr("extract_skills(ky_nang, ky_nang_chi_tiet, mo_ta, yeu_cau)"))
      .withColumn("work_mode", F.expr("infer_work_mode(mo_ta, location)"))
      .withColumn("posted_date", F.expr("normalize_date(date)"))
      .withColumn("benefits", F.expr("benefits_to_string(phuc_loi)"))
)

final = parsed.select(
    F.col("company").alias("Tên công ty"),
    F.col("location").alias("Địa chỉ"),
    F.col("city").alias("Thành phố"),
    F.col("tieu_de").alias("Vị trí tuyển dụng"),
    F.col("salary.min_vnd").alias("Lương tối thiểu (VND)"),
    F.col("salary.max_vnd").alias("Lương tối đa (VND)"),
    F.col("salary.currency").alias("Đơn vị tiền tệ"),
    "work_mode",
    "skills",
    F.col("posted_date").alias("Ngày đăng tuyển (dd/MM/yy)"),
    F.col("mo_ta").alias("Mô tả công việc"),
    "benefits",
    F.col("cap_bac").alias("Cấp bậc / Thời gian làm việc"),
    "url"
).fillna({
    "Tên công ty": "Không rõ",
    "Vị trí tuyển dụng": "Không rõ",
    "Thành phố": "Không rõ",
    "work_mode": "Trực tiếp"
})

# -----------------------------------------------------------------------------
# Write Output
# -----------------------------------------------------------------------------
out = "data_json"
(
    final.coalesce(1)
         .write.mode("overwrite")
         .option("encoding","utf-8")
         .option("multiline",True)
         .json(out)
)

print(f"Done. Output folder: {out} | Records: {final.count()}")
spark.stop()
