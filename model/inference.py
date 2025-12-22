import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, lit, round

# Cấu hình môi trường
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = os.path.dirname(sys.executable)


def run_inference():
    spark = SparkSession.builder \
        .appName("Inference") \
        .getOrCreate()

    model_path = "models/salary_rf_model"
    data_path = "processed_data"  # Hoặc đường dẫn tới file dữ liệu mới cần dự đoán
    output_path = "results/predictions.json"

    if not os.path.exists(model_path):
        print("Chưa thấy Model. Hãy chạy step2_train.py trước.")
        return

    # 1. Load Model
    print(">>> Đang load model...")
    loaded_model = PipelineModel.load(model_path)

    # 2. Đọc dữ liệu cần dự đoán
    # Ở đây mình dùng lại processed_data để demo.
    # Thực tế bạn có thể đọc file 'new_jobs.json' chứa các job chưa có lương.
    df = spark.read.parquet(data_path)

    # 3. Thực hiện dự đoán (Transform)
    predictions = loaded_model.transform(df)

    # 4. Chọn các cột cần thiết để xuất báo cáo
    result_df = predictions.select(
        col("Tên công ty"),
        col("Vị trí công việc"),
        col("Địa chỉ"),
        col("avg_salary_label").alias("luong_thuc_te"),  # Để so sánh
        round(col("prediction"), -4).alias("luong_du_doan_AI")  # Làm tròn đến hàng chục nghìn
    )

    print("--- Kết quả dự đoán mẫu ---")
    result_df.show(5, truncate=False)

    # 5. Lưu kết quả ra file JSON (1 file duy nhất cho dễ đọc)
    print(f">>> Đang lưu kết quả vào: {output_path}")

    # Coalesce(1) để gom hết dữ liệu vào 1 file part duy nhất (chỉ dùng khi dữ liệu < 1GB)
    # Nếu dữ liệu lớn, bỏ coalesce(1)
    result_df.coalesce(1).write \
        .mode("overwrite") \
        .json("results_temp")

    # Đổi tên file part-... thành predictions.json cho đẹp (Optional)
    # Spark lưu folder, ta cần thủ thuật nhỏ để lấy file json ra
    for filename in os.listdir("results_temp"):
        if filename.endswith(".json"):
            shutil.move(
                os.path.join("results_temp", filename),
                output_path
            )
    shutil.rmtree("results_temp")  # Xóa folder tạm

    print(">>> Hoàn tất dự đoán.")
    spark.stop()


if __name__ == "__main__":
    run_inference()