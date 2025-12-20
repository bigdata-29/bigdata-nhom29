import os
import sys
import re
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def setup_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("ITviecProcessing_Final")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# chuẩn hóa địa chỉ công ty
def standardize_city_pyspark(spark, df, address_col="company_location"):
    """
    Chuẩn hóa tên thành phố trong DataFrame PySpark về 'Hà Nội' hoặc 'Thành Phố Hồ Chí Minh'.
    Các chuỗi không chứa từ khóa liên quan sẽ được giữ nguyên.
    """
    
    # Chuẩn hóa input về chữ thường không dấu để khớp với regex
    def normalize_text(text):
        if text is None:
            return ""
        # Rất khó để chuẩn hóa tiếng Việt không dấu hoàn toàn trong regex,
        # nên ta tập trung vào các biến thể tiếng Anh và ký tự đặc biệt.
        text = text.lower().replace('.', ' ').replace(',', ' ').strip()
        return text

    # Các từ khóa liên quan đến Hà Nội (HN, H.N, Hanoi, Ha Noi, HaNoi)
    hn_patterns = (
        r'.*(\bhn\b|\bhni\b|hà\s?nội|\bha\s?n\s?oi|hanoi|h\s?n)\b.*'
    )
    # Các từ khóa liên quan đến TP. Hồ Chí Minh (HCM, TPHCM, Ho Chi Minh, Sai Gon)
    hcm_patterns = (
        r'.*(\bhcm\b|\btphcm\b|hồ\s?chí\s?minh|\bho\s?chi\s?minh|\bsaigon|\bsài\s?gòn)\b.*'
    )

    # 1. Định nghĩa UDF (User Defined Function) để chuẩn hóa text
    normalize_udf = F.udf(normalize_text, F.StringType())
    
    # 2. Áp dụng chuẩn hóa và logic regex
    df = df.withColumn(
        "company_location",
        F.when(
            # Kiểm tra TP. Hồ Chí Minh (Ưu tiên kiểm tra thành phố dài hơn trước)
            F.regexp_extract(normalize_udf(F.col(address_col)), hcm_patterns, 0) != "",
            F.lit("Thành Phố Hồ Chí Minh")
        ).when(
            # Kiểm tra Hà Nội
            F.regexp_extract(normalize_udf(F.col(address_col)), hn_patterns, 0) != "",
            F.lit("Hà Nội")
        )
        .otherwise(F.col(address_col)) # Giữ nguyên nếu không khớp
    )
    
    return df


# chuẩn hóa lương
def standardize_salary_pyspark(spark, df, salary_col="salary"):
    """
    Chuẩn hóa cột lương: chuyển 'love/negot/competitive' sang unknown,
    chuyển khoảng lương sang 0-max (hoặc min-max), chuẩn hóa đơn vị.
    Đơn vị: USD (tỷ giá 26000), tr (triệu), mặc định là VND.
    """
    
    # Tỷ giá USD
    USD_RATE = 26000 
    
    # Chuyển đổi lương về dạng số (tính bằng VND)
    def normalize_salary(salary_str):
        if salary_str is None:
            return "unknown"
        
        salary_str = salary_str.lower().strip()
        
        # 1. Trường hợp 'unknown' (love, negot, competitive)
        if re.search(r'\b(love|negot|competitive)\b', salary_str):
            return "unknown"
            
        # 2. Trường hợp không có số
        if not re.search(r'\d', salary_str):
            return "unknown"

        # 3. Chuẩn hóa USD, tr, và dấu phẩy ngăn cách
        
        # Xóa các dấu phân cách hàng nghìn (',' hoặc '.')
        salary_str = re.sub(r'(\d)[.,](\d{3})', r'\1\2', salary_str)
        
        # Trích xuất các số (có thể có đơn vị tr/usd đi kèm)
        numbers = re.findall(r'(\d+)\s*(tr|usd)?', salary_str)
        
        if not numbers:
            return "unknown"

        def calculate_amount(num_str, unit):
            try:
                amount = int(num_str)
                if unit == 'tr':
                    return amount * 1_000_000
                elif unit == 'usd':
                    return amount * USD_RATE
                else:
                    # Mặc định là đơn vị VND nhỏ (nghìn/triệu)
                    if amount < 1000: # Ví dụ: 70 -> 70 triệu
                        return amount * 1_000_000 
                    return amount # Nếu là số lớn (ví dụ: 70000000)
            except ValueError:
                return 0

        # Lấy giá trị Min và Max
        amounts = [calculate_amount(num, unit) for num, unit in numbers if num.isdigit()]
        amounts = [a for a in amounts if a > 0] # Lọc các số 0

        if not amounts:
            return "unknown"
            
        min_salary = min(amounts)
        max_salary = max(amounts)
        
        # 4. Chuẩn hóa Up To (Không áp dụng cho min/max salary range)
        if re.search(r'(up\s+to|lên\s+đến)', salary_str):
            # Nếu là "Up to X", chuẩn hóa thành "0 - X"
            return f"0 - {max_salary}"
        
        # 5. Trường hợp lương cố định hoặc min-max
        if min_salary == max_salary:
            return str(min_salary) # Lương cố định
        
        return f"{min_salary} - {max_salary}" # Khoảng lương

    # 4. Định nghĩa UDF và áp dụng
    normalize_salary_udf = F.udf(normalize_salary, F.StringType())
    
    df = df.withColumn(
        "salary",
        normalize_salary_udf(F.col(salary_col))
    )
    
    return df


if __name__ == "__main__":
    spark = setup_spark_session()
    df = spark.read.json("../itviec_jobs.jsonl")
    df = standardize_city_pyspark(spark, df, "company_location")
    df = standardize_salary_pyspark(spark, df, "salary")