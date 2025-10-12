# 🌟 TỔNG QUAN VỀ APACHE SPARK

## 1. Giới thiệu




**Apache Spark** là một framework xử lý dữ liệu lớn (Big Data) mã nguồn mở, được phát triển tại UC Berkeley AMP Lab và hiện do Apache Software Foundation duy trì.  
Spark cho phép **xử lý dữ liệu phân tán** với **tốc độ cao**, **dễ lập trình**, và **hỗ trợ nhiều mô hình tính toán** như batch, streaming, machine learning và graph.

---



## 2. Đặc điểm nổi bật

- ⚡ **Nhanh**: Xử lý dữ liệu nhanh hơn Hadoop MapReduce gấp 10–100 lần nhờ cơ chế in-memory computing (lưu dữ liệu trung gian trong RAM thay vì ghi ra đĩa).
- 💡 **Dễ lập trình**: Hỗ trợ nhiều ngôn ngữ — **Python (PySpark)**, **Scala**, **Java**, **R**, **SQL**.
- 🔁 **Đa dạng mô hình tính toán**:
  - Spark SQL → Truy vấn dữ liệu dạng bảng.
  - Spark Streaming → Xử lý luồng dữ liệu thời gian thực.
  - MLlib → Thư viện Machine Learning.
  - GraphX → Xử lý đồ thị.
- ☁️ **Tương thích cao**: Tích hợp dễ dàng với Hadoop, Hive, Cassandra, HDFS, S3, v.v.

![Spark vs Hadoop](blob:https://web.facebook.com/d4ff9dec-88e2-4cfa-ad06-ef8d33d5a6ce)
---

## 3. Kiến trúc tổng quan
### 3.1 Thành phần chính

![Sơ đò kiến trúc Spark](blob:https://web.facebook.com/aa08267b-7c2b-4133-a99f-bf01cdfcaf6d)

1. **Driver Program**
   - Là chương trình điều khiển (main app).
   - Tạo `SparkContext` – cổng giao tiếp với cluster.
2. **Cluster Manager**
   - Quản lý tài nguyên cho toàn cluster.
   - Các loại: **Standalone**, **YARN**, **Mesos**, **Kubernetes**.
3. **Executors**
   - Tiến trình chạy trên các node của cluster, chịu trách nhiệm thực thi tác vụ (task).
4. **Tasks**
   - Các đơn vị công việc nhỏ nhất được gửi từ driver tới executors để thực thi.

### 3.2 Luồng xử lý

![Luồng xử lý](blob:https://web.facebook.com/aa08267b-7c2b-4133-a99f-bf01cdfcaf6d)
```
User Code → Driver → SparkContext → Cluster Manager → Executors → Tasks → Kết quả
```
Client (người dùng) gửi một yêu cầu (Job) đến Cluster Manager.
 → Ví dụ: bạn gọi df.show() hoặc spark.sql(...).
Cluster Manager (ví dụ YARN) kiểm tra yêu cầu hợp lệ, sau đó phân bổ tài nguyên và chỉ định Driver Node để xử lý.
Driver Node:
Tạo SparkContext / SparkSession.
Phân tích yêu cầu thành nhiều Stage và Task nhỏ.
Liên hệ với NameNode (trong HDFS) để biết dữ liệu nằm ở đâu.
Gửi các Task cho các Worker Node tương ứng.
Worker Node:
Nhận Task từ Driver.
Node Manager (của YARN) tạo các Container để chạy Executors.
Mỗi Executor xử lý nhiều Task, truy cập DataNode để đọc dữ liệu.
Các Task được chạy song song trên nhiều Worker.
Driver Node:
Theo dõi tiến trình và thu kết quả từ các Executor.
Khi tất cả Task hoàn tất → tổng hợp và gửi kết quả lại cho người dùng (client).
Cluster Manager:
Giải phóng tài nguyên (CPU, RAM).
Ghi log toàn bộ quá trình để debug hoặc giám sát.
---

## 4. RDD – Resilient Distributed Dataset
Là **trái tim của Spark**, RDD đại diện cho tập dữ liệu phân tán được xử lý song song.

### Tính chất:
- **Resilient**: Chống lỗi (tự phục hồi khi node hỏng).
- **Distributed**: Phân tán qua nhiều node.
- **Lazy Evaluation**: Chỉ thực thi khi có action (`collect`, `count`, `saveAsTextFile`, ...).

### Loại thao tác:
- **Transformation** (biến đổi): tạo RDD mới, như `map()`, `filter()`, `flatMap()`.
- **Action** (hành động): trả kết quả về driver, như `count()`, `collect()`.

---

## 5. DataFrame & Dataset API
- **DataFrame**: Cấu trúc dữ liệu dạng bảng (giống Pandas hoặc SQL).
- **Dataset**: Mở rộng DataFrame (hỗ trợ type safety trong Scala/Java).
- Cả hai đều chạy trên **Spark SQL Engine (Catalyst Optimizer)**.

---

## 6. Các Module Chính của Spark
| Module | Mô tả |
|--------|--------|
| **Spark Core** | Cung cấp API RDD, quản lý bộ nhớ, lập lịch task |
| **Spark SQL** | Truy vấn dữ liệu bằng SQL hoặc DataFrame |
| **Spark Streaming** | Xử lý dữ liệu thời gian thực |
| **MLlib** | Thư viện Machine Learning |
| **GraphX** | Xử lý dữ liệu đồ thị |

---

## 7. Cơ chế xử lý In-Memory
Spark lưu dữ liệu trung gian trong **RAM**, giúp:
- Giảm số lần đọc/ghi đĩa.
- Tăng tốc độ xử lý lặp (đặc biệt trong ML hoặc iterative jobs).

---

## 8. So sánh Spark vs Hadoop MapReduce
| Tiêu chí | Spark | Hadoop MapReduce |
|-----------|--------|------------------|
| **Tốc độ** | Rất nhanh (in-memory) | Chậm (ghi ra đĩa sau mỗi bước) |
| **Dễ lập trình** | API đơn giản, hỗ trợ nhiều ngôn ngữ | Cần nhiều code (Java) |
| **Xử lý real-time** | Có (Spark Streaming) | Không |
| **ML hỗ trợ sẵn** | MLlib | Cần tích hợp thư viện ngoài |

---

## 9. Ứng dụng thực tế
- Phân tích log hệ thống, clickstream.
- Hệ thống gợi ý (recommendation systems).
- Phân tích dữ liệu tài chính.
- Phát hiện gian lận.
- Machine Learning và AI pipeline.

---

## 10. Kết luận
Apache Spark là **nền tảng xử lý dữ liệu mạnh mẽ và linh hoạt** cho Big Data.  
Với khả năng xử lý nhanh, hỗ trợ đa dạng mô hình và tích hợp mạnh mẽ, Spark đã trở thành **chuẩn công nghiệp** trong phân tích dữ liệu hiện đại.

---

📘 **Tài liệu tham khảo**
- [https://spark.apache.org/](https://spark.apache.org/)
- [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)
