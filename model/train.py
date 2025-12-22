import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder,
    RegexTokenizer, CountVectorizer,
    VectorAssembler
)
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Cấu hình môi trường
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = os.path.dirname(sys.executable)


def run_training():
    spark = SparkSession.builder \
        .appName("Train_Model") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc dữ liệu sạch
    input_path = "model/processed_data"
    if not os.path.exists(input_path):
        print("Chưa có dữ liệu sạch. Hãy chạy step1_preprocess.py trước.")
        return

    df = spark.read.parquet(input_path)

    # 2. Xây dựng Pipeline (Feature Engineering)
    stages = []

    # a. Xử lý Địa chỉ & Cấp bậc (Category)
    # setHandleInvalid("keep"): Nếu gặp giá trị mới lạ chưa từng thấy, không báo lỗi
    stages += [StringIndexer(inputCol="Địa chỉ", outputCol="loc_idx", handleInvalid="keep")]
    stages += [OneHotEncoder(inputCols=["loc_idx"], outputCols=["loc_vec"])]

    stages += [StringIndexer(inputCol="cap_bac", outputCol="lvl_idx", handleInvalid="keep")]
    stages += [OneHotEncoder(inputCols=["lvl_idx"], outputCols=["lvl_vec"])]

    # b. Xử lý Tiêu đề (Text)
    stages += [RegexTokenizer(inputCol="Vị trí công việc", outputCol="title_words", pattern="\\W")]
    stages += [CountVectorizer(inputCol="title_words", outputCol="title_vec", vocabSize=1000, minDF=2.0)]

    # c. Xử lý Phúc lợi (Array)
    stages += [CountVectorizer(inputCol="Phúc lợi", outputCol="benefit_vec", vocabSize=500, minDF=2.0)]

    # d. Gom Vector
    stages += [VectorAssembler(
        inputCols=["loc_vec", "lvl_vec", "title_vec", "benefit_vec"],
        outputCol="features"
    )]

    # 3. Model Regression
    rf = RandomForestRegressor(featuresCol="features", labelCol="avg_salary_label", numTrees=50, seed=42)
    stages += [rf]

    pipeline = Pipeline(stages=stages)

    # 4. Chia tập Train/Test
    (train_data, test_data) = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Train size: {train_data.count()} | Test size: {test_data.count()}")

    # 5. Train
    print(">>> Đang training...")
    model = pipeline.fit(train_data)

    # 6. Evaluate
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="avg_salary_label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"=" * 30)
    print(f"RMSE (Sai số trung bình): {rmse:,.0f} VND")
    print(f"=" * 30)

    # 7. Lưu Model
    model_path = "models/salary_rf_model"
    if os.path.exists(model_path):
        shutil.rmtree(model_path)

    model.write().overwrite().save(model_path)
    print(f">>> Đã lưu model tại: {model_path}")
    spark.stop()


if __name__ == "__main__":
    run_training()