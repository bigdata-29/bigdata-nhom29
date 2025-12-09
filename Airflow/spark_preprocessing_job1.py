import os
import re
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# --- CẤU HÌNH ĐƯỜNG DẪN ---
# WebHDFS URL
HDFS_PREFIX = "webhdfs://192.168.49.1:9870"

# Input: File Raw JSONL do Crawler tạo ra
INPUT_PATH = f"{HDFS_PREFIX}/data/careerviet_jobs.jsonl"

# Output: File JSON sau khi preprocessing
OUTPUT_PATH = f"{HDFS_PREFIX}/data/preprocessed_data.json"


def setup_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("CareerVietJobProcessing_Final")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    # --- CẤU HÌNH HADOOP CHO WEBHDFS ---
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.webhdfs.impl", "org.apache.hadoop.hdfs.web.WebHdfsFileSystem")
    hadoop_conf.set("fs.defaultFS", HDFS_PREFIX)
    hadoop_conf.set("hadoop.job.ugi", "hoanvdtd")  # user HDFS nếu không dùng Kerberos

    spark.sparkContext.setLogLevel("ERROR")
    return spark

def parse_salary_expr(col):
    usd_to_vnd_rate = 25000
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

def main():
    spark = setup_spark_session()

    input_path = "../../careerviet_jobs.jsonl"
    try:
        # Đọc Raw JSON
        raw_df = spark.read.option("mergeSchema", "true").json(INPUT_PATH)

        # --- XỬ LÝ DỮ LIỆU ---
        deduplicated_df = raw_df.dropDuplicates(["url"]).coalesce(1)
        ten_cong_ty_expr = F.coalesce(
        F.when(F.col("ten_cong_ty").isNotNull() & (F.col("ten_cong_ty") != "N/A"), F.col("ten_cong_ty")),
        F.trim(F.regexp_extract(F.col("mo_ta_cong_viec"), r"(Công ty TNHH|Công ty Cổ phần|Tập đoàn)[\s\w&.,()-]+", 0)),
        F.trim(F.regexp_extract(F.col("mo_ta_cong_viec"), r"^([\w\s.&*()-]+?)\s+(tuyển dụng|tuyển|tìm kiếm)", 1)),
        F.trim(F.regexp_extract(F.col("mo_ta_cong_viec"), r"Tại ([\w\s.&*()-]+?),", 1)),
        F.lit("Chưa xác định")
    )
        skills_keywords = [
        'Python', 'Java', 'C#', 'C++', 'SQL', 'JavaScript', 'TypeScript', 'Go', 'PHP', 'Ruby', 'React', 'Vue.js',
        'Angular',
        'Node.js', '.NET', 'Spring Boot', 'Django', 'Flask', 'Power BI', 'Tableau', 'Looker', 'SQL Server',
        'PostgreSQL',
        'MySQL', 'MongoDB', 'Oracle', 'Redis', 'AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes', 'Terraform', 'CI/CD',
        'Jenkins', 'Git', 'SAP', 'ERP', 'Odoo', 'Machine Learning', 'AI', 'Deep Learning', 'ETL', 'Data Warehouse',
        'Big Data', 'Spark', 'Hadoop', 'Kafka', 'Linux', 'Network', 'Security', 'Firewall', 'API', 'Microservices',
        'Automation Test', 'Selenium', 'Agile', 'Scrum', 'Jira'
        ]
        full_text_col = F.lower(F.concat_ws(" ", F.col("yeu_cau_cong_viec"), F.col("mo_ta_cong_viec")))
        skill_checks = [F.when(full_text_col.rlike(r'\b' + re.escape(skill.lower()) + r'\b'), skill) for skill in skills_keywords]
        skills_array_with_nulls = F.array(*skill_checks)
        danh_sach_ky_nang_expr = F.expr("filter(skills_array, x -> x is not null)")
        processed_df = (
            deduplicated_df
            .withColumn("Ten cong ty", ten_cong_ty_expr)
            .withColumn("Vi tri cong viec", F.col("tieu_de"))
            .withColumn("skills_array", skills_array_with_nulls)
            .withColumn("Danh sach ky nang yeu cau", danh_sach_ky_nang_expr)
            .withColumn("Dia chi", F.when(F.col("dia_diem_lam_viec.thanh_pho").isNull(), None)
                        .when(F.col("dia_diem_lam_viec.thanh_pho").isin("Hồ Chí Minh", "TP.HCM", "HCM"),  
                              "Thành Phố Hồ Chí Minh")
                        .otherwise(F.col("dia_diem_lam_viec.thanh_pho")))
            .withColumn("Luong (VND)", parse_salary_expr(F.col("thong_tin_khac.lương")))
            .withColumn("Cach thuc lam viec", F.lit("Trực tiếp"))
            .withColumn("Ngay dang tuyen",
                        F.when(F.col("ngay_dang_tuyen").isNotNull() & (F.col("ngay_dang_tuyen") != "N/A"), F.col("ngay_dang_tuyen"))
                        .otherwise(None)
            )
            .withColumn("Thoi gian lam viec",
                        F.when(F.col("thong_tin_khac.thời_gian_làm_việc").isNotNull(),
                               F.col("thong_tin_khac.thời_gian_làm_việc"))
                               .otherwise("Giờ hành chính"))
        )
        
        final_df = processed_df.select(
            F.col("Ten cong ty").alias("Tên công ty"),
            F.col("Dia chi").alias("Địa chỉ"),
            F.col("Vi tri cong viec").alias("Vị trí công việc"),
            F.col("Luong (VND)").alias("Lương (VND)"),
            F.col("Cach thuc lam viec").alias("Cách thức làm việc"),
            F.col("Danh sach ky nang yeu cau").alias("Danh sách kỹ năng yêu cầu"),
            F.col("Ngay dang tuyen").alias("Ngày đăng tuyển"),
            F.col("mo_ta_cong_viec").alias("Mô tả công việc"),
            F.col("phuc_loi").alias("Phúc lợi"),
            F.col("Thoi gian lam viec").alias("Thời gian làm việc")
        )

        # Ghi JSON
        print(f"--- [JOB 1] Ghi JSON xuống: {OUTPUT_PATH}")
        final_df.write.mode("overwrite").json(OUTPUT_PATH)
        print("✓ Job Preprocessing hoàn tất!")
        

    except Exception as e:
        print(f"✗ Lỗi: {e}")
        spark.stop()
        raise e

    spark.stop()

if __name__ == "__main__":
    main()