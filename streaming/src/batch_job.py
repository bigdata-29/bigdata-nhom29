from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, trim, split, regexp_replace, element_at, size, coalesce, lit, current_timestamp
from pyspark.sql.types import LongType
import config 
from udfs import classify_level_udf 

def create_batch_session():
    return SparkSession.builder \
        .appName("ITJobBatchProcessing") \
        .config("spark.hadoop.fs.s3a.endpoint", config.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", config.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", config.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = create_batch_session()
    
    # 1. ĐỌC DỮ LIỆU TỪ DATA LAKE (MINIO)
    print(f"Đọc dữ liệu từ: {config.DATALAKE_PATH}")
    try:
        # Đọc toàn bộ thư mục Parquet
        df = spark.read.parquet(config.DATALAKE_PATH)
    except Exception as e:
        print(f"Chưa có dữ liệu trong Data Lake hoặc lỗi đọc: {e}")
        return

    # 2. XỬ LÝ BATCH CHUYÊN SÂU 
    
    # Deduplicate toàn bộ lịch sử 
    df_unique = df.dropDuplicates(["url"])

    df_batch = df_unique.withColumn("batch_processed_at", current_timestamp())

    # 3. GHI VÀO SERVING LAYER (Elasticsearch - Batch Index)
    ES_INDEX_BATCH = "it_jobs_batch"
    
    print(f"Ghi dữ liệu vào Elasticsearch Index: {ES_INDEX_BATCH}")
    df_batch.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.resource", ES_INDEX_BATCH) \
        .option("es.nodes", config.ES_HOST) \
        .option("es.port", config.ES_PORT) \
        .option("es.mapping.id", "url") \
        .option("es.nodes.wan.only", "true") \
        .save()
        
    print("Batch Job hoàn tất!")
    spark.stop()

if __name__ == "__main__":
    main()