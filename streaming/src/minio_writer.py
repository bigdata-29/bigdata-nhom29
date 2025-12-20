import os
import json
import io
import uuid
from datetime import datetime
from minio import Minio
from minio.error import S3Error

class MinioWriter:
    """
    Quản lý việc kết nối và ghi dữ liệu thô (raw JSON) vào MinIO Data Lake.
    Đây là lớp Bronze của kiến trúc Lakehouse.
    """
    def __init__(self):
        self.endpoint = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
        self.access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
        self.bucket_name = os.getenv('MINIO_BRONZE_BUCKET', 'it-jobs-raw') # Bucket cho dữ liệu thô
        self.client = self._create_client()
        self._ensure_bucket_exists()

    def _create_client(self):
        try:
            client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=False # Đặt là False vì đang chạy local, không có SSL/TLS
            )
            print("Kết nối tới MinIO thành công!")
            return client
        except Exception as e:
            print(f"Không thể kết nối tới MinIO: {e}")
            return None

    def _ensure_bucket_exists(self):
        """Kiểm tra xem bucket đã tồn tại chưa, nếu chưa thì tạo mới."""
        if self.client:
            try:
                found = self.client.bucket_exists(self.bucket_name)
                if not found:
                    self.client.make_bucket(self.bucket_name)
                    print(f"Bucket '{self.bucket_name}' đã được tạo.")
                else:
                    print(f"Bucket '{self.bucket_name}' đã tồn tại.")
            except S3Error as e:
                print(f"Lỗi khi kiểm tra hoặc tạo bucket: {e}")

    def save_job_to_bronze(self, job_data: dict):
        """
        Lưu một job (dạng dict) vào MinIO dưới dạng file JSON.
        Cấu trúc file: source/YYYY/MM/DD/timestamp_uuid.json
        """
        if not self.client:
            return False
        
        try:
            # Chuyển dict thành bytes JSON
            json_bytes = json.dumps(job_data, ensure_ascii=False, indent=2).encode('utf-8')
            json_stream = io.BytesIO(json_bytes)

            # Tạo đường dẫn file (object name)
            source = job_data.get('source', 'unknown')
            now = datetime.now()
            object_name = (
                f"{source}/"
                f"{now.strftime('%Y/%m/%d')}/"
                f"{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4()}.json"
            )

            # Tải file lên MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=json_stream,
                length=len(json_bytes),
                content_type='application/json'
            )
            return True
        except S3Error as e:
            print(f"Lỗi khi lưu file vào MinIO: {e}")
            return False