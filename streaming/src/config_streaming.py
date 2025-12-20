# src/processing/config.py

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = "my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
KAFKA_TOPIC = "it-jobs"

# MinIO (Data Lake) Config
MINIO_ENDPOINT = "http://minio.data-lake.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "it-jobs-processed"
DATALAKE_PATH = f"s3a://{MINIO_BUCKET}/processed_data"
CHECKPOINT_PATH = f"s3a://{MINIO_BUCKET}/checkpoints"

# Elasticsearch Config
ES_HOST = "elasticsearch-es-http.datastore.svc.cluster.local"
ES_PORT = "9200"
ES_INDEX = "it_jobs"