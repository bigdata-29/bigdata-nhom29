import os
import re
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

os.environ["PYSPARK_PYTHON"] = r"C:\miniconda3\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\miniconda3\python.exe"

spark = (
SparkSession.builder
.appName("CareerVietJobProcessing_SaveToJsonl")
.master("local[*]")
.config("spark.driver.memory", "4g")
.config("spark.driver.bindAddress", "127.0.0.1")
.config("spark.sql.execution.arrow.pyspark.enabled", "true")
.getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

input_path = "../careerviet_jobs.jsonl"
try:
    raw_df = spark.read.json(input_path)
except Exception as e:
    print(f"Lỗi khi đọc file JSON: {e}")
    spark.stop()
    exit()

deduplicated_df = raw_df.dropDuplicates(["url"]).coalesce(1)

vi_tri_cong_viec_expr = F.initcap(F.regexp_replace(
    F.regexp_replace(
        F.element_at(F.split(F.col("url"), "/"), -1),
        r".html$", ""
    ),
    r"(-[a-fA-F0-9]{8})?$", ""
))

ten_cong_ty_expr = F.coalesce(
    F.trim(F.regexp_extract(F.col("mo_ta_cong_viec"), r"(Công ty TNHH|Công ty Cổ phần|Tập đoàn)[\s\w&.,()-]+", 0)),
    F.trim(F.regexp_extract(F.col("mo_ta_cong_viec"), r"^([\w\s.&*()-]+?)\s+(tuyển dụng|tuyển|tìm kiếm)", 1)),
    F.trim(F.regexp_extract(F.col("mo_ta_cong_viec"), r"Tại ([\w\s.&*()-]+?),", 1)),
    F.lit("Chưa xác định")
)

skills_keywords = [
'Python', 'Java', 'C#', 'C++', 'SQL', 'JavaScript', 'TypeScript', 'Go', 'PHP', 'Ruby',
'React', 'Vue.js', 'Angular', 'Node.js', '.NET', 'Spring Boot', 'Django', 'Flask', 'Power BI',
'Tableau', 'Looker', 'SQL Server', 'PostgreSQL', 'MySQL', 'MongoDB', 'Oracle', 'Redis',
'AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes', 'Terraform', 'CI/CD', 'Jenkins', 'Git', 'SAP',
'ERP', 'Odoo', 'Machine Learning', 'AI', 'Deep Learning', 'ETL', 'Data Warehouse', 'Big Data',
'Spark', 'Hadoop', 'Kafka', 'Linux', 'Network', 'Security', 'Firewall', 'API', 'Microservices',
'Automation Test', 'Selenium', 'Agile', 'Scrum', 'Jira'
]

full_text_col = F.lower(F.concat_ws(" ", F.col("yeu_cau_cong_viec"), F.col("mo_ta_cong_viec")))
skill_checks = [F.when(full_text_col.rlike(r'\b' + re.escape(skill.lower()) + r'\b'), skill) for skill in skills_keywords]
skills_array_with_nulls = F.array(*skill_checks)
danh_sach_ky_nang_expr = F.expr("filter(skills_array, x -> x is not null)")

enriched_df = (
    deduplicated_df
    .withColumn("Vi tri cong viec", vi_tri_cong_viec_expr)
    .withColumn("Ten cong ty", ten_cong_ty_expr)
    .withColumn("skills_array", skills_array_with_nulls)
    .withColumn("Danh sach ky nang yeu cau", danh_sach_ky_nang_expr)
)

usd_to_vnd_rate = 25000

def parse_salary_expr(col):
    salary_parts = F.split(F.regexp_replace(col, r"[\n\r]", ""), r"\s*-\s*")
    min_salary_str = F.when(F.size(salary_parts) > 0, F.trim(salary_parts[0])).otherwise(None)
    max_salary_str = F.when(F.size(salary_parts) > 1, F.trim(salary_parts[1])).otherwise(None)

    def convert_to_vnd(salary_str, multiplier):
        cleaned_str = F.regexp_replace(salary_str, r"[^0-9.]", "")
        return F.when(
            salary_str.isNotNull() & (cleaned_str != ""),
            (cleaned_str.cast("float") * multiplier).cast("long")
        ).otherwise(None)

    return (
        F.when(col.isNull() | F.lower(col).contains("cạnh tranh"), None)
        .when(F.lower(col).contains("usd"),
              F.concat_ws(" - ",
                          convert_to_vnd(min_salary_str, usd_to_vnd_rate),
                          convert_to_vnd(max_salary_str, usd_to_vnd_rate)))
        .when(F.lower(col).contains("tr"),
              F.concat_ws(" - ",
                          convert_to_vnd(min_salary_str, 1_000_000),
                          convert_to_vnd(max_salary_str, 1_000_000)))
        .otherwise(None)
    )

processed_df = (
    enriched_df
    .withColumn("Dia chi", F.when(F.col("dia_diem_lam_viec.thanh_pho").isNull(), None)
    .when(F.col("dia_diem_lam_viec.thanh_pho").isin("Hồ Chí Minh", "TP.HCM", "HCM"), "Thành Phố Hồ Chí Minh")
    .otherwise(F.col("dia_diem_lam_viec.thanh_pho")))
    .withColumn("Luong (VND)", parse_salary_expr(F.col("thong_tin_khac.lương")))
    .withColumn("Cach thuc lam viec", F.lit("Trực tiếp"))
    .withColumn("Ngay dang tuyen", F.lit(None).cast(StringType()))
    .withColumn("Thoi gian lam viec",
        F.when(F.col("thong_tin_khac.thời_gian_làm_việc").isNotNull(),
        F.col("thong_tin_khac.thời_gian_làm_việc"))
        .otherwise("Giờ hành chính"))
)

final_df = processed_df.select(
    F.col("Ten cong ty").alias("Tên công ty"),
    F.col("Dia chi").alias("Địa chỉ"),
    F.col("Vi tri cong viec").alias("Vị trí công việc (tiếng Anh)"),
    F.col("Luong (VND)").alias("Lương (VND)"),
    F.col("Cach thuc lam viec").alias("Cách thức làm việc"),
    F.col("Danh sach ky nang yeu cau").alias("Danh sách kỹ năng yêu cầu"),
    F.col("Ngay dang tuyen").alias("Ngày đăng tuyển (dd/mm/yy)"),
    F.col("mo_ta_cong_viec").alias("Mô tả công việc"),
    F.col("phuc_loi").alias("Phúc lợi"),
    F.col("Thoi gian lam viec").alias("Thời gian làm việc")
)

print("Hoàn tất xử lý. Đang ghi dữ liệu vào preprocessed_data.jsonl ...")

output_file = "../preprocessed_data.jsonl"
try:
    with open(output_file, "w", encoding="utf-8") as f:
        for row in final_df.toLocalIterator():
            json.dump(row.asDict(recursive=True), f, ensure_ascii=False)
            f.write("\n")

    print(f"\nĐã lưu thành công vào file duy nhất: {output_file}")

except Exception as e:
    print(f"\nLỗi khi ghi file: {e}")

finally:
    spark.stop()
    print("Spark session đã được đóng.")