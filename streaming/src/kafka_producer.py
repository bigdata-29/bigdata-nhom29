import json
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import logging

#logging.basicConfig(level=logging.DEBUG)
class JobProducer:
    """
    Một class quản lý việc kết nối và gửi dữ liệu việc làm vào Kafka.
    Đọc cấu hình từ biến môi trường để linh hoạt hơn.
    """
    def __init__(self):
        # Lấy thông tin cấu hình từ biến môi trường
        #self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        #self.topic = os.getenv('KAFKA_TOPIC', 'it-jobs')
        self.bootstrap_servers= 'localhost:9094'
        self.topic= 'it-jobs'
        self.producer = self._create_producer()

    def _create_producer(self):
        """Tạo một instance KafkaProducer, xử lý việc thử lại nếu broker chưa sẵn sàng."""
        retries = 5
        for i in range(retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(','),
                    # Chuyển đổi dict Python thành bytes JSON trước khi gửi
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    # Các cài đặt để tăng hiệu suất
                    acks='all',  # Đảm bảo message đã được ghi an toàn
                    retries=3,
                    batch_size=16384, # 16 KB
                    linger_ms=5, # Chờ 5ms để gom message lại thành batch
                    request_timeout_ms=10000, # Giảm timeout xuống 10s cho nhanh
                )
                
                print(" Đang thử kết nối tới Kafka...")
                # Lệnh này bắt buộc Python phải nói chuyện với Kafka ngay lập tức
                topics = producer.partitions_for(self.topic) 
                if topics:
                    print(f" Kết nối THẬT SỰ thành công! Tìm thấy topic '{self.topic}' với các partition: {topics}")
                    return producer
                else:
                    print(f" Kết nối được nhưng không tìm thấy topic '{self.topic}'.")
                    return producer
                
            except NoBrokersAvailable:
                print(f"Không thể kết nối tới Kafka. Thử lại sau 5 giây... (lần {i+1}/{retries})")
                time.sleep(5)
        print("Không thể tạo kết nối tới Kafka sau nhiều lần thử. Vui lòng kiểm tra lại địa chỉ bootstrap server.")
        return None

    def send_job(self, job_data):
        """Gửi một tin tuyển dụng (dưới dạng dict) vào Kafka topic."""
        if self.producer:
            try:
                self.producer.send(self.topic, value=job_data)
            except Exception as e:
                print(f"Lỗi khi gửi tin nhắn: {e}")

    def close(self):
        """Đóng kết nối producer một cách an toàn."""
        if self.producer:
            print("Đang đóng kết nối Kafka producer...")
            self.producer.flush() # Đảm bảo mọi tin nhắn chờ đều được gửi đi
            self.producer.close()
            print("Đã đóng kết nối.")