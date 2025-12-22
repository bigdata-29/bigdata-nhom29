import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_extract, regexp_replace,
    trim, lower, split, coalesce, element_at, size, udf
)
from pyspark.sql.types import LongType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, lower, trim, regexp_replace

def setup_spark_session():
    """
    Khởi tạo và cấu hình một Spark Session.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Fix cho lỗi Hadoop trên Windows
    os.environ["HADOOP_HOME"] = os.path.dirname(sys.executable)

    spark = SparkSession.builder \
        .appName("job_preprocess_optimized") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
        .config("spark.hadoop.io.nativeio.enabled", "false") \
        .config("spark.sql.parquet.writeLegacyFormat", "true") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
        .config("spark.sql.sources.commitProtocolClass",
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# Khởi tạo Spark Session
spark = setup_spark_session()

# Đọc dữ liệu từ file JSONL
df = spark.read.json("preprocessed_data.jsonl") #sửa đường dẫn hdfs

print("=== Dữ liệu gốc ===")
df.show(5, truncate=False)
print(f"Số bản ghi ban đầu: {df.count()}")

# ============================================
# BƯỚC 1: LỌC DỮ LIỆU THIẾU CÁC TRƯỜNG QUAN TRỌNG
# ============================================
print("\n=== Thống kê dữ liệu null trước khi lọc ===")
df.select([
    col("Địa chỉ").isNull().alias("dia_chi_null"),
    col("Lương (VND)").isNull().alias("luong_null"),
    col("Vị trí công việc").isNull().alias("vi_tri_null"),
    col("Ngày đăng tuyển").isNull().alias("ngay_dang_null")
]).describe().show()

# Loại bỏ các record thiếu trường quan trọng (KHÔNG bao gồm lương)
df = df.filter(
    col("Địa chỉ").isNotNull() &
    col("Vị trí công việc").isNotNull() &
    col("Ngày đăng tuyển").isNotNull()
)

print("\n=== XỬ LÝ TIN TRÙNG (Batch Deduplication – Improved) ===")
before_dedup = df.count()

# Chuẩn hóa các cột dùng để dedup (KHÔNG ảnh hưởng dữ liệu gốc)
df = (
    df
    .withColumn(
        "company_norm",
        lower(trim(regexp_replace(col("Tên công ty"), r"\s+", " ")))
    )
    .withColumn(
        "title_norm",
        lower(trim(regexp_replace(col("Vị trí công việc"), r"\s+", " ")))
    )
    .withColumn(
        "address_norm",
        lower(trim(regexp_replace(col("Địa chỉ"), r"\s+", " ")))
    )
)

# Window dedup: cùng công ty + vị trí + địa chỉ → giữ tin mới nhất
window_spec = Window.partitionBy(
    "company_norm",
    "title_norm",
    "address_norm"
).orderBy(col("Ngày đăng tuyển").desc())

df = (
    df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num", "company_norm", "title_norm", "address_norm")
)

after_dedup = df.count()

print(f"Trước dedup: {before_dedup}")
print(f"Sau dedup:   {after_dedup}")
print(f"Đã loại bỏ:  {before_dedup - after_dedup} tin trùng")

# ============================================
# BƯỚC 2: CHUẨN HÓA CỘT ĐỊA CHỈ
# ============================================
df = df.withColumn(
    "Địa chỉ",
    # --- Các thành phố lớn & quy tắc riêng của bạn ---
    when(lower(col("Địa chỉ")).contains("hồ chí minh"), "TP.HCM")
    .when(lower(col("Địa chỉ")).contains("hà nội"), "Hà Nội")
    .when(lower(col("Địa chỉ")).contains("đà nẵng"), "Đà Nẵng")
    .when(lower(col("Địa chỉ")).contains("cần thơ"), "Cần Thơ")
    .when(lower(col("Địa chỉ")).contains("hải phòng"), "Hải Phòng")
    .when(lower(col("Địa chỉ")).contains("biên hòa"), "Đồng Nai")
    .when(lower(col("Địa chỉ")).contains("nha trang"), "Khánh Hòa")
    .when(lower(col("Địa chỉ")).contains("huế"), "Thừa Thiên Huế")
    .when(lower(col("Địa chỉ")).contains("vinh"), "Nghệ An")
    .when(lower(col("Địa chỉ")).contains("buôn ma thuột"), "Đắk Lắk")

    # --- Miền Bắc ---
    .when(lower(col("Địa chỉ")).contains("bắc giang"), "Bắc Giang")
    .when(lower(col("Địa chỉ")).contains("bắc kạn"), "Bắc Kạn")
    .when(lower(col("Địa chỉ")).contains("bắc ninh"), "Bắc Ninh")
    .when(lower(col("Địa chỉ")).contains("cao bằng"), "Cao Bằng")
    .when(lower(col("Địa chỉ")).contains("điện biên"), "Điện Biên")
    .when(lower(col("Địa chỉ")).contains("hà giang"), "Hà Giang")
    .when(lower(col("Địa chỉ")).contains("hà nam"), "Hà Nam")
    .when(lower(col("Địa chỉ")).contains("hải dương"), "Hải Dương")
    .when(lower(col("Địa chỉ")).contains("hòa bình"), "Hòa Bình")
    .when(lower(col("Địa chỉ")).contains("hưng yên"), "Hưng Yên")
    .when(lower(col("Địa chỉ")).contains("lai châu"), "Lai Châu")
    .when(lower(col("Địa chỉ")).contains("lạng sơn"), "Lạng Sơn")
    .when(lower(col("Địa chỉ")).contains("lào cai"), "Lào Cai")
    .when(lower(col("Địa chỉ")).contains("nam định"), "Nam Định")
    .when(lower(col("Địa chỉ")).contains("ninh bình"), "Ninh Bình")
    .when(lower(col("Địa chỉ")).contains("phú thọ"), "Phú Thọ")
    .when(lower(col("Địa chỉ")).contains("quảng ninh"), "Quảng Ninh")
    .when(lower(col("Địa chỉ")).contains("sơn la"), "Sơn La")
    .when(lower(col("Địa chỉ")).contains("thái bình"), "Thái Bình")
    .when(lower(col("Địa chỉ")).contains("thái nguyên"), "Thái Nguyên")
    .when(lower(col("Địa chỉ")).contains("tuyên quang"), "Tuyên Quang")
    .when(lower(col("Địa chỉ")).contains("vĩnh phúc"), "Vĩnh Phúc")
    .when(lower(col("Địa chỉ")).contains("yên bái"), "Yên Bái")

    # --- Miền Trung ---
    .when(lower(col("Địa chỉ")).contains("bình định"), "Bình Định")
    .when(lower(col("Địa chỉ")).contains("bình thuận"), "Bình Thuận")
    .when(lower(col("Địa chỉ")).contains("hà tĩnh"), "Hà Tĩnh")
    .when(lower(col("Địa chỉ")).contains("khánh hòa"),
          "Khánh Hòa")
    .when(lower(col("Địa chỉ")).contains("ninh thuận"), "Ninh Thuận")
    .when(lower(col("Địa chỉ")).contains("phú yên"), "Phú Yên")
    .when(lower(col("Địa chỉ")).contains("quảng bình"), "Quảng Bình")
    .when(lower(col("Địa chỉ")).contains("quảng nam"), "Quảng Nam")
    .when(lower(col("Địa chỉ")).contains("quảng ngãi"), "Quảng Ngãi")
    .when(lower(col("Địa chỉ")).contains("quảng trị"), "Quảng Trị")
    .when(lower(col("Địa chỉ")).contains("thanh hóa"), "Thanh Hóa")
    .when(lower(col("Địa chỉ")).contains("huế"), "Thừa Thiên Huế")

    # --- Tây Nguyên ---
    .when(lower(col("Địa chỉ")).contains("đắk lắk"), "Đắk Lắk")
    .when(lower(col("Địa chỉ")).contains("đắk nông"), "Đắk Nông")
    .when(lower(col("Địa chỉ")).contains("gia lai"), "Gia Lai")
    .when(lower(col("Địa chỉ")).contains("kon tum"), "Kon Tum")
    .when(lower(col("Địa chỉ")).contains("lâm đồng"), "Lâm Đồng")

    # --- Miền Nam ---
    .when(lower(col("Địa chỉ")).contains("an giang"), "An Giang")
    .when(lower(col("Địa chỉ")).contains("bà rịa"), "Bà Rịa - Vũng Tàu")
    .when(lower(col("Địa chỉ")).contains("vũng tàu"), "Bà Rịa - Vũng Tàu")
    .when(lower(col("Địa chỉ")).contains("bạc liêu"), "Bạc Liêu")
    .when(lower(col("Địa chỉ")).contains("bến tre"), "Bến Tre")
    .when(lower(col("Địa chỉ")).contains("bình dương"), "Bình Dương")
    .when(lower(col("Địa chỉ")).contains("bình phước"), "Bình Phước")
    .when(lower(col("Địa chỉ")).contains("cà mau"), "Cà Mau")
    .when(lower(col("Địa chỉ")).contains("đồng nai"), "Đồng Nai")
    .when(lower(col("Địa chỉ")).contains("đồng tháp"), "Đồng Tháp")
    .when(lower(col("Địa chỉ")).contains("hậu giang"), "Hậu Giang")
    .when(lower(col("Địa chỉ")).contains("kiên giang"), "Kiên Giang")
    .when(lower(col("Địa chỉ")).contains("long an"), "Long An")
    .when(lower(col("Địa chỉ")).contains("sóc trăng"), "Sóc Trăng")
    .when(lower(col("Địa chỉ")).contains("tây ninh"), "Tây Ninh")
    .when(lower(col("Địa chỉ")).contains("tiền giang"), "Tiền Giang")
    .when(lower(col("Địa chỉ")).contains("trà vinh"), "Trà Vinh")
    .when(lower(col("Địa chỉ")).contains("vĩnh long"), "Vĩnh Long")

    .otherwise("Khác")
)

# ============================================
# BƯỚC 3: XỬ LÝ CỘT LƯƠNG (GIỮ LẠI LƯƠNG THỎA THUẬN)
# ============================================

# Chuẩn hóa chuỗi lương (giữ null cho "Thỏa thuận")
df_luong = df.withColumn(
    "luong_chuan_hoa",
    when(
        col("Lương (VND)").isNull() |
        lower(trim(col("Lương (VND)"))).rlike("thoả thuận|thỏa thuận|thoa thuan"),
        None
    ).otherwise(lower(trim(col("Lương (VND)"))))
)

# Tạo cột hệ số nhân (triệu, nghìn)
df_luong = df_luong.withColumn(
    "he_so_nhan",
    when(col("luong_chuan_hoa").rlike("triệu|tr"), 1000000)
    .when(col("luong_chuan_hoa").rlike("nghìn|k"), 1000)
    .otherwise(1)
)

# Dọn dẹp và tách chuỗi số
df_luong = df_luong.withColumn(
    "luong_so",
    split(regexp_replace(col("luong_chuan_hoa"), r"[^0-9.-]+", ""), "-")
)

# Tạo cột luong_min và luong_max (sử dụng element_at để tránh lỗi index out of bounds)
df_luong = df_luong.withColumn(
    "luong_min",
    when(col("luong_chuan_hoa").isNotNull(),
         element_at(col("luong_so"), 1).cast(LongType()) * col("he_so_nhan"))
    .otherwise(None)
)

df_luong = df_luong.withColumn(
    "luong_max_temp",
    when((col("luong_chuan_hoa").isNotNull()) & (size(col("luong_so")) > 1),
         element_at(col("luong_so"), 2).cast(LongType()) * col("he_so_nhan"))
    .otherwise(None)
)

df_luong = df_luong.withColumn(
    "luong_max",
    coalesce(col("luong_max_temp"), col("luong_min"))
)

df = df_luong.drop("luong_chuan_hoa", "he_so_nhan", "luong_so", "luong_max_temp")


# ============================================
# BƯỚC 4A: PHÂN LOẠI CẤP BẬC BẰNG UDF
# ============================================

def classify_level_udf_func(job_title):
    """
    UDF để phân loại cấp bậc dựa vào tiêu đề công việc.
    Hỗ trợ cả tiếng Việt có dấu và không dấu.
    """
    if job_title is None or job_title.strip() == "":
        return "Mid-level"

    job_title_lower = job_title.lower()

    # Leader/Manager patterns
    leader_keywords = [
        'trưởng', 'truong', 'manager', 'lead', 'director',
        'giám đốc', 'giam doc', 'phó', 'pho', 'head of',
        'chief', 'cto', 'ceo', 'coo', 'cfo', 'vp', 'vice president'
    ]

    # Senior patterns
    senior_keywords = [
        'senior', 'chuyên viên chính', 'chuyen vien chinh',
        'principal', 'chủ nhiệm', 'chu nhiem', 'expert', 'specialist'
    ]

    # Junior patterns
    junior_keywords = [
        'junior', 'fresher', 'intern', 'thực tập', 'thuc tap',
        'mới tốt nghiệp', 'moi tot nghiep', 'entry level',
        'entry-level', 'graduate', 'trainee'
    ]

    # Kiểm tra theo thứ tự ưu tiên (Leader > Senior > Junior)
    for keyword in leader_keywords:
        if keyword in job_title_lower:
            return "Leader"

    for keyword in senior_keywords:
        if keyword in job_title_lower:
            return "Senior"

    for keyword in junior_keywords:
        if keyword in job_title_lower:
            return "Junior"

    # Mặc định là Mid-level nếu không khớp với bất kỳ pattern nào
    return "Mid-level"


# Đăng ký UDF với returnType rõ ràng
classify_level_udf = udf(classify_level_udf_func, StringType())

print("\n=== Áp dụng UDF để phân loại cấp bậc ===")
df = df.withColumn("cap_bac_udf", classify_level_udf(col("Vị trí công việc")))

# ============================================
# BƯỚC 4B: PHÂN LOẠI CẤP BẬC BẰNG BUILT-IN (SO SÁNH)
# ============================================

# Tạo một cột chứa vị trí công việc đã được chuyển thành chữ thường
df = df.withColumn("vi_tri_lower", lower(col("Vị trí công việc")))

# Định nghĩa các pattern cho từng cấp bậc
leader_pattern = 'trưởng|truong|manager|lead|director|giám đốc|giam doc|phó|pho|head of|chief|cto|ceo|coo|cfo|vp|vice president'
senior_pattern = 'senior|chuyên viên chính|chuyen vien chinh|principal|chủ nhiệm|chu nhiem|expert|specialist'
junior_pattern = 'junior|fresher|intern|thực tập|thuc tap|mới tốt nghiệp|moi tot nghiep|entry level|entry-level|graduate|trainee'

df = df.withColumn(
    "cap_bac_builtin",
    when(col("vi_tri_lower").rlike(leader_pattern), "Leader")
    .when(col("vi_tri_lower").rlike(senior_pattern), "Senior")
    .when(col("vi_tri_lower").rlike(junior_pattern), "Junior")
    .otherwise("Mid-level")
)

df = df.drop("vi_tri_lower")

# Sử dụng kết quả từ UDF làm cột chính
df = df.withColumn("cap_bac", col("cap_bac_udf"))

# ============================================
# KẾT QUẢ
# ============================================

# DataFrame đã được làm sạch (GIỮ LẠI CẢ LƯƠNG THỎA THUẬN)
df_cleaned = df

print("\n=== Dữ liệu sau khi làm sạch ===")
df_cleaned.select(
    "Tên công ty",
    "Địa chỉ",
    "Vị trí công việc",
    "cap_bac",
    "Lương (VND)",
    "luong_min",
    "luong_max",
    "Ngày đăng tuyển"
).show(10, truncate=False)

# So sánh kết quả UDF vs Built-in
print("\n=== So sánh UDF vs Built-in ===")
df_cleaned.filter(col("cap_bac_udf") != col("cap_bac_builtin")).select(
    "Vị trí công việc",
    "cap_bac_udf",
    "cap_bac_builtin"
).show(5, truncate=False)

comparison_count = df_cleaned.filter(col("cap_bac_udf") != col("cap_bac_builtin")).count()
print(f"Số lượng khác biệt giữa UDF và Built-in: {comparison_count}")

print(f"\nSố bản ghi sau khi làm sạch: {df_cleaned.count()}")

# Thống kê số lượng bản ghi có lương null (thỏa thuận)
luong_null_count = df_cleaned.filter(col("luong_min").isNull() & col("luong_max").isNull()).count()
luong_co_gia_tri = df_cleaned.filter(col("luong_min").isNotNull()).count()
print(f"Số bản ghi có lương thỏa thuận (null): {luong_null_count}")
print(f"Số bản ghi có lương cụ thể: {luong_co_gia_tri}")

# Thống kê phân bố cấp bậc (UDF)
print("\n=== Phân bố cấp bậc (UDF) ===")
df_cleaned.groupBy("cap_bac_udf").count().orderBy("count", ascending=False).show()

# Thống kê phân bố cấp bậc (Built-in)
print("\n=== Phân bố cấp bậc (Built-in) ===")
df_cleaned.groupBy("cap_bac_builtin").count().orderBy("count", ascending=False).show()

# Thống kê phân bố địa chỉ
print("\n=== Phân bố địa chỉ ===")
df_cleaned.groupBy("Địa chỉ").count().orderBy("count", ascending=False).show(20)

# Lưu kết quả (ưu tiên JSON cho Windows để tránh lỗi Hadoop)
print("\n=== Đang lưu dữ liệu... ===")

# Xóa các cột so sánh trước khi lưu
df_cleaned = df_cleaned.drop("cap_bac_udf", "cap_bac_builtin")

# Lưu dạng JSON (luôn work trên Windows)
try:
    output_path = "../cleaned_recruitment_data.json"
    df_cleaned.write.mode("overwrite").json(output_path)
    print(f"✓ Đã lưu dữ liệu JSON vào: {output_path}")
except Exception as e:
    print(f"✗ Lỗi khi lưu JSON: {e}")

# Thử lưu Parquet (có thể lỗi trên Windows)
try:
    output_path_parquet = "../cleaned_recruitment_data.parquet"
    df_cleaned.coalesce(1).write.mode("overwrite") \
        .option("compression", "none") \
        .parquet(output_path_parquet)
    print(f"✓ Đã lưu dữ liệu Parquet vào: {output_path_parquet}")
except Exception as e:
    print(f"⚠ Không thể lưu Parquet (lỗi Hadoop trên Windows): {str(e)[:150]}...")
    print("  → Dùng file JSON ở trên thay thế")

# Lưu CSV làm backup (dễ đọc)
try:
    output_path_csv = "cleaned_recruitment_data.csv"
    df_cleaned.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .csv(output_path_csv)
    print(f"✓ Đã lưu dữ liệu CSV vào: {output_path_csv}")
except Exception as e:
    print(f"⚠ Không thể lưu CSV: {str(e)[:100]}")

# Dừng Spark Session
spark.stop()