from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp, window, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

import config_streaming as config
from udfs import extract_skills_udf, parse_min_salary_udf, parse_max_salary_udf, classify_level_udf, standardize_location_udf

def create_spark_session():
    return SparkSession.builder \
        .appName("ITJobStreamingAnalytics") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Schema dữ liệu đầu vào (phải khớp với Scraper gửi lên Kafka)
    schema = StructType([
        StructField("url", StringType()),
        StructField("job_title", StringType()),
        StructField("company_name", StringType()),
        StructField("salary", StringType()),
        StructField("company_location", StringType()),
        StructField("job_description", StringType()),
        StructField("job_requirements", StringType()),
        StructField("post_time", StringType()),
        StructField("source", StringType()),
        StructField("crawled_at", StringType())
    ])

    # 2. Đọc từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    job_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # 3. ÁP DỤNG LOGIC LÀM SẠCH VÀ BIẾN ĐỔI (Sử dụng UDFs từ file udfs.py)
    processed_df = job_df \
        .withColumn("crawled_timestamp", to_timestamp(col("crawled_at"))) \
        .withColumn("standardized_location", standardize_location_udf(col("company_location"))) \
        .withColumn("min_salary", parse_min_salary_udf(col("salary"))) \
        .withColumn("max_salary", parse_max_salary_udf(col("salary"))) \
        .withColumn("level", classify_level_udf(col("job_title"))) \
        .withColumn("skills", extract_skills_udf(col("job_description"), col("job_requirements"))) \
        .withColumn("processing_time", current_timestamp())

    # Xử lý watermark để windowing (Yêu cầu thầy giáo)
    # Loại bỏ dữ liệu bị trễ quá 1 giờ và trùng lặp
    clean_stream_df = processed_df \
        .withWatermark("crawled_timestamp", "1 hour") \
        .dropDuplicates(["url", "crawled_timestamp"])

    # 4. GHI DỮ LIỆU (SINKS)

    # Sink 1: MinIO (Data Lake) - Lưu trữ dài hạn
    ds_query = clean_stream_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", config.DATALAKE_PATH) \
        .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/datalake") \
        .partitionBy("standardized_location") \
        .trigger(processingTime="1 minute") \
        .start()

    # Sink 2: Elasticsearch - Phục vụ tìm kiếm và Dashboard
    es_query = clean_stream_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", config.ES_INDEX) \
        .option("es.nodes", config.ES_HOST) \
        .option("es.port", config.ES_PORT) \
        .option("es.mapping.id", "url") \
        .option("es.nodes.wan.only", "true") \
        .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/elasticsearch") \
        .start()

    print("Streaming Job đã khởi động...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()