import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import ArrayType, StringType

# Cấu hình môi trường
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = os.path.dirname(sys.executable)


def run_preprocess():
    spark = SparkSession.builder \
        .appName("Preprocess_Data") \
        .getOrCreate()

    # 1. Đọc dữ liệu thô (JSON Lines từ Crawler)
    input_path = "preprocess_data/cleaned_recruitment_data.json"  # Thư mục chứa các file json/jsonl
    if not os.path.exists(input_path):
        print(f"Lỗi: Không tìm thấy thư mục '{input_path}'")
        return

    print(">>> Đang đọc dữ liệu từ:", input_path)
    # allowBackslashEscapingAnyCharacter: Hỗ trợ đọc json có ký tự lạ
    raw_df = spark.read.option("recursiveFileLookup", "true").json(input_path)

    # 2. Lọc dữ liệu
    # - Loại bỏ bản ghi null
    # - Loại bỏ lương = 0 hoặc null (Trường hợp "Thỏa thuận")
    clean_df = raw_df \
        .filter(col("luong_min").isNotNull() & col("luong_max").isNotNull()) \
        .filter((col("luong_min") > 0) & (col("luong_max") > 0)) \
        .filter(col("Vị trí công việc").isNotNull())

    # 3. Tạo cột Label (Lương trung bình)
    processed_df = clean_df.withColumn(
        "avg_salary_label",
        (col("luong_min") + col("luong_max")) / 2
    )

    # 4. Xử lý giá trị Null cho các cột Feature (Tránh lỗi khi train)
    # Nếu "Phúc lợi" null -> mảng rỗng
    # Nếu "Địa chỉ" null -> "Unknown"
    processed_df = processed_df \
        .na.fill({"Địa chỉ": "Unknown", "cap_bac": "Unknown", "Vị trí công việc": ""})

    # Xử lý riêng cho cột Array (Phúc lợi) bị null
    # Spark không hỗ trợ fillna trực tiếp cho ArrayType, phải dùng when/otherwise
    processed_df = processed_df.withColumn(
        "Phúc lợi",
        when(col("Phúc lợi").isNull(), lit([]).cast(ArrayType(StringType())))
        .otherwise(col("Phúc lợi"))
    )

    # 5. Lưu ra file Parquet (Để bước Train đọc nhanh hơn)
    output_path = "model/processed_data"
    if os.path.exists(output_path):
        shutil.rmtree(output_path)  # Xóa cũ ghi mới

    print(f">>> Lưu dữ liệu sạch vào: {output_path}")
    processed_df.write.parquet(output_path)

    print(f"Số lượng mẫu sạch dùng để train: {processed_df.count()}")
    spark.stop()


if __name__ == "__main__":
    run_preprocess()
