# PySpark

 - PySpark DataFrame and SQL
   - DF trong spark cơ bản giống với pandas, tuy nhiên thao tác trên DF Spark cần phải sử dụng các hàm/API của Spark, còn nếu tự build thì phức tạp hơn
 - PySpark Streaming
 - PySpark MLlib: có các thuật toán ML cơ bản để khai phá thêm hoặc xử lý thêm dữ liệu
 - PySpark GraphFrames
 - PySpark Resource: thành phần vật lý và logic được cấp phát để chạy hiệu quả bigdata
   - Đây chính là phần cần cấu hình: Driver Program, executor, task, partition đều phải được tương đương với một thành phần phần cứng hoặc tài nguyên nào đó

* Lưu ý *
 - Spark có 2 tác dụng chính: xử lý dữ liệu cơ bản, cấu hình để xử lý trên cluster. Điều quan trọng là đặt các tham số trong config 1 SparkSession (cpu, ram, parallel terminal, partition, core/partition)
 - Muốn config được đúng thì cần phải giám sát Spark Web UI để điều chỉnh các tham số

# Airflow/Prefect
Có tác dụng lập lịch tự động các task, đảm bảo ràng buộc, báo lỗi và retry, kích hoạt pyspark

# K8s
Đồng bộ hóa môi trường chạy giống Docker, phân bổ nơi thực thi, cpu, ram, Scale quy mô cluster

# Airflow + K8s
Quản lý cả về thời gian và không gian thực hiện pipeline