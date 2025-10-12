#  Data Warehouse Design — Job Data Analytics System

## Giới thiệu

Trong hệ thống Big Data về **việc làm**, Data Warehouse (DWH) đóng vai trò là **lớp lưu trữ dữ liệu có cấu trúc**, phục vụ cho các mục tiêu:
- Phân tích, báo cáo, trực quan hóa dữ liệu (BI)
- Cung cấp dữ liệu đầu vào cho mô hình Machine Learning
- Quản lý và chuẩn hóa dữ liệu thu thập được từ nhiều nguồn khác nhau (các trang tuyển dụng, mạng xã hội nghề nghiệp, API, ...)

---

## Mục tiêu của Data Warehouse

- **Tổng hợp dữ liệu việc làm** từ nhiều nguồn (ETL)
- **Lưu trữ chuẩn hóa** theo mô hình dữ liệu phân tích (Fact & Dimension)
- **Hỗ trợ truy vấn hiệu quả** cho các câu hỏi như:
  - “Mức lương trung bình theo kỹ năng là bao nhiêu?”
  - “Top 5 công ty có số lượng tin tuyển dụng cao nhất?”
  - “Xu hướng nhu cầu tuyển dụng Data Engineer trong 6 tháng qua?”

---

## Cách tổ chức dữ liệu trong Data Warehouse

###  Mô hình dữ liệu phân tích

Dữ liệu trong Data Warehouse được tổ chức theo **mô hình đa chiều (Dimensional Model)**, gồm hai loại bảng:

| Loại bảng | Mô tả | Vai trò |
|------------|--------|---------|
| **Fact Table** | Lưu trữ các **sự kiện, chỉ số đo lường** | Dữ liệu trung tâm cho các phân tích |
| **Dimension Table** | Lưu trữ các **thuộc tính mô tả** liên quan đến fact | Cung cấp ngữ cảnh, thông tin chi tiết |

---

###  Fact Table — `fact_job_postings`

Bảng này lưu các **sự kiện đăng tin tuyển dụng** và **chỉ số định lượng** (measure).

Ví dụ:

| Cột | Kiểu dữ liệu | Mô tả |
|------|---------------|--------|
| `job_posting_id` | INT | Khóa chính (duy nhất mỗi tin tuyển dụng) |
| `job_id` | INT | Liên kết đến bảng `dim_job` |
| `company_id` | INT | Liên kết đến bảng `dim_company` |
| `location_id` | INT | Liên kết đến bảng `dim_location` |
| `date_id` | INT | Liên kết đến bảng `dim_date` |
| `min_salary` | FLOAT | Mức lương tối thiểu (triệu VND) |
| `max_salary` | FLOAT | Mức lương tối đa (triệu VND) |
| `num_applicants` | INT | Số lượng ứng viên (nếu có) |

---

### Dimension Tables

#### `dim_job`
Ví dụ:
| Cột | Kiểu | Mô tả |
|------|------|--------|
| `job_id` | INT | Khóa chính |
| `job_title` | STRING | Tên vị trí |
| `job_category` | STRING | Nhóm ngành nghề |
| `required_skills` | STRING | Kỹ năng yêu cầu (Python, SQL, Spark, …) |

#### `dim_company`
Ví dụ
| Cột | Kiểu | Mô tả |
|------|------|--------|
| `company_id` | INT | Khóa chính |
| `company_name` | STRING | Tên công ty |
| `company_size` | STRING | Quy mô nhân sự |
| `industry` | STRING | Ngành nghề hoạt động |

#### `dim_location`
Ví dụ:
| Cột | Kiểu | Mô tả |
|------|------|--------|
| `location_id` | INT | Khóa chính |
| `city` | STRING | Thành phố |
| `region` | STRING | Miền (Bắc, Trung, Nam) |

#### `dim_date`
Ví dụ:
| Cột | Kiểu | Mô tả |
|------|------|--------|
| `date_id` | INT | Khóa chính (định dạng YYYYMMDD) |
| `date` | DATE | Ngày thực tế |
| `day` | INT | Ngày trong tháng |
| `month` | INT | Tháng |
| `year` | INT | Năm |
| `quarter` | STRING | Quý (Q1–Q4) |

---

## Mô hình Star Schema (Ngôi sao)

Mô hình **Star Schema** được sử dụng vì:
- Dễ hiểu và trực quan
- Hiệu suất truy vấn cao
- Tối ưu cho công cụ phân tích (BI tools, OLAP, Spark SQL)

<img width="1280" height="720" alt="image" src="https://github.com/user-attachments/assets/52804e96-349e-49c6-aba9-2d7344981f25" />

## Mở rộng: Snowflake Schema

Trong trường hợp muốn chuẩn hóa dữ liệu hơn nữa, có thể tách nhỏ các dimension (ví dụ industry thành bảng riêng):

dim_company → dim_industry

Tuy nhiên, mô hình Snowflake phức tạp hơn nên thường chỉ dùng cho hệ thống dữ liệu rất lớn hoặc yêu cầu tính toàn vẹn cao.

## Kết nối với quy trình ETL/ELT
Dữ liệu thô (raw data) thu thập được từ web crawl, API, hoặc streaming sẽ được xử lý qua các bước:
**Extract:** Thu thập tin tuyển dụng, thông tin công ty, kỹ năng, lương, ...
**Transform:** Làm sạch, chuẩn hóa format, mapping dữ liệu sang ID chuẩn.
**Load:** Đưa dữ liệu đã xử lý vào các bảng fact_* và dim_* trong Data Warehouse.
