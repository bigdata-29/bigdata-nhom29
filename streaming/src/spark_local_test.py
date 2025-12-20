# ==============================================================================
# SPARK STREAMING PROCESSOR FOR IT JOBS PIPELINE
# ==============================================================================
# Mô tả: Ứng dụng Spark Structured Streaming này là trái tim của hệ thống.
# Nó thực hiện các công việc sau:
#   1. Đọc dữ liệu việc làm IT thời gian thực từ một topic Kafka.
#   2. Làm sạch, chuẩn hóa và làm giàu dữ liệu (Data Cleaning & Enrichment).
#   3. Áp dụng các phép biến đổi nâng cao như xử lý trùng lặp và tính toán theo cửa sổ thời gian.
#   4. Ghi kết quả ra hai hệ thống lưu trữ khác nhau cho các mục đích riêng biệt:
#      - MinIO (S3-compatible): Lưu trữ lâu dài dưới định dạng Parquet, phục vụ cho Data Lake.
#      - Elasticsearch: Phục vụ cho các truy vấn tìm kiếm và dashboard thời gian thực.
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, regexp_extract, to_timestamp, window, udf
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DoubleType

# --- Phần 1: Cấu hình hệ thống ---
# Các giá trị này được đọc từ các biến môi trường hoặc file cấu hình trong môi trường production.
# Đối với mục đích phát triển, chúng ta định nghĩa chúng ở đây.

# Cấu hình Kafka Source
# Sử dụng tên service và namespace trong Kubernetes để kết nối nội bộ.
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "it-jobs"

# Cấu hình MinIO (S3) Sink cho Data Lake
# Spark sẽ kết nối tới MinIO thông qua S3A protocol.
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET_NAME = "it-jobs-processed"
DATALAKE_PATH = f"s3a://{MINIO_BUCKET_NAME}/processed_jobs"
CHECKPOINT_PATH_DATALAKE = f"s3a://{MINIO_BUCKET_NAME}/checkpoints/datalake"

# Cấu hình Elasticsearch Sink cho Serving Layer
ES_HOST = "localhost"
ES_PORT = "9200"
ES_INDEX = "it_jobs"
#CHECKPOINT_PATH_ES = f"s3a://{MINIO_BUCKET_NAME}/checkpoints/elasticsearch"
LOCAL_CHECKPOINT_PATH = "checkpoints/local_test"

# --- Phần 2: Hàm UDF (User-Defined Function) tùy chỉnh ---
# UDF cho phép chúng ta thực hiện các logic phức tạp không có sẵn trong Spark SQL.

@udf(returnType=StringType())
def standardize_city(city_str: str) -> str:
    """
    Chuẩn hóa tên thành phố về một định dạng thống nhất.
    Bài học: Đảm bảo chất lượng dữ liệu (Data Quality).
    """
    if city_str is None:
        return "Unknown"
    city_str = city_str.lower().strip()
    if "hồ chí minh" in city_str or "hcm" in city_str:
        return "Ho Chi Minh City"
    if "hà nội" in city_str:
        return "Ha Noi"
    if "đà nẵng" in city_str:
        return "Da Nang"
    return city_str.title()

# --- Phần 3: Khởi tạo và Cấu hình Spark Session ---

def create_spark_session():
    """
    Tạo và cấu hình một SparkSession để kết nối với S3 (MinIO).
    Bài học: Tích hợp hệ thống (System Integration), Tối ưu Spark (Spark Optimization).
    """
    return SparkSession.builder \
        .appName("ITJobStreamingProcessor") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.apache.spark:spark-hadoop-cloud_2.12:3.5.0") \
        .getOrCreate()

# --- Phần 4: Luồng xử lý chính ---

def process_stream(spark: SparkSession):
    """
    Định nghĩa và khởi chạy toàn bộ luồng xử lý streaming.
    """
    print("Bắt đầu định nghĩa luồng streaming...")

    # Bước 1: Đọc dữ liệu từ Kafka (Ingestion)
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Bước 2: Parse và Làm sạch dữ liệu (Transformation & Data Quality)
    job_schema = StructType([
        StructField("job_title", StringType()),
        StructField("company_name", StringType()),
        StructField("salary", StringType()),
        StructField("company_location", StringType()),
        StructField("working_model", StringType()),
        StructField("post_date", StringType()),
        StructField("skills_list", ArrayType(StringType())),
        StructField("crawled_at", StringType()),
        StructField("source", StringType()),
        StructField("url", StringType())
        # Thêm các trường tùy chọn khác nếu có
    ])

    parsed_df = raw_df.select(from_json(col("value").cast("string"), job_schema).alias("data")).select("data.*")

    # Áp dụng các phép biến đổi để làm sạch và cấu trúc lại dữ liệu
    cleaned_df = parsed_df \
        .withColumn("crawled_timestamp", to_timestamp(col("crawled_at"))) \
        .withColumn("post_timestamp", to_timestamp(col("post_date"), "dd/MM/yy")) \
        .withColumn("city", standardize_city(col("company_location"))) \
        .withColumn("min_salary_vnd", (regexp_extract(col("salary"), r"(\d+)", 1).cast(LongType()) * 1000000)) \
        .select("job_title", "company_name", "city", "working_model", "skills_list", 
                "min_salary_vnd", "crawled_timestamp", "post_timestamp", "source", "url")

    # Bước 3: Xử lý trùng lặp và dữ liệu đến muộn (Advanced Stream Processing)
    # Bài học: Xử lý luồng (Stream Processing), Chất lượng dữ liệu (Data Quality).
    # Watermark cho phép Spark xử lý các sự kiện đến muộn trong vòng 1 giờ.
    # dropDuplicates loại bỏ các bản ghi có cùng URL được cào trong vòng 1 giờ đó.
    deduplicated_df = cleaned_df \
        .withWatermark("crawled_timestamp", "1 hour") \
        .dropDuplicates(["url", "crawled_timestamp"])

    # Bước 4: Ghi dữ liệu ra các Sinks (Data Storage)

    # Sink 1: Ghi vào Data Lake (MinIO)
    # Định dạng Parquet, phân vùng theo thành phố và ngày đăng để tối ưu truy vấn sau này.
    # Bài học: Lưu trữ dữ liệu (Data Storage), Tối ưu hiệu năng (Performance Optimization).
    print(f"Bắt đầu ghi vào Data Lake tại: {DATALAKE_PATH}")
    # query_to_datalake = deduplicated_df.writeStream \
    #     .format("parquet") \
    #     .outputMode("append") \
    #     .option("path", DATALAKE_PATH) \
    #     .option("checkpointLocation", CHECKPOINT_PATH_DATALAKE) \
    #     .partitionBy("city") \
    #     .trigger(processingTime="5 minutes") \
    #     .start()

    # Sink 2: Ghi vào Elasticsearch
    # Để phục vụ cho tìm kiếm và dashboard. Sử dụng URL làm ID để đảm bảo tính duy nhất.
    # Bài học: Tích hợp hệ thống (System Integration), Đảm bảo Exactly-Once (Stream Processing).
    print(f"Bắt đầu ghi vào Elasticsearch index: {ES_INDEX}")
    query_to_es = deduplicated_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", ES_INDEX) \
        .option("es.nodes", ES_HOST) \
        .option("es.port", ES_PORT) \
        .option("es.mapping.id", "url") \
        .option("es.nodes.wan.only", "true") \
        .option("checkpointLocation", LOCAL_CHECKPOINT_PATH) \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Bật Sink console để xem kết quả trực tiếp trên terminal
    query_to_console = deduplicated_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
    
    

    print("Các luồng streaming đã được khởi động.")
    spark.streams.awaitAnyTermination()


# --- Phần 5: Điểm bắt đầu của ứng dụng ---

if __name__ == "__main__":
    spark_session = None
    try:
        spark_session = create_spark_session()
        process_stream(spark_session)
    except Exception as e:
        print(f"Gặp lỗi trong quá trình xử lý: {e}")
    finally:
        if spark_session:
            print("Đang dừng Spark Session.")
            spark_session.stop()