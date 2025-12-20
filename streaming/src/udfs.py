# src/processing/udfs.py
import re
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, LongType, ArrayType

# XỬ LÝ KỸ NĂNG (SKILLS) 
SKILLS_KEYWORDS = [
    'Python', 'Java', 'C#', 'C++', 'SQL', 'JavaScript', 'TypeScript', 'Go', 'PHP', 'Ruby', 'React', 'Vue.js',
    'Angular', 'Node.js', '.NET', 'Spring Boot', 'Django', 'Flask', 'Power BI', 'Tableau', 'Looker', 'SQL Server',
    'PostgreSQL', 'MySQL', 'MongoDB', 'Oracle', 'Redis', 'AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes', 'Terraform',
    'CI/CD', 'Jenkins', 'Git', 'Spark', 'Hadoop', 'Kafka', 'Linux', 'Machine Learning', 'AI'
]

@udf(returnType=ArrayType(StringType()))
def extract_skills_udf(description, requirements):
    """Trích xuất kỹ năng từ mô tả và yêu cầu công việc."""
    full_text = (str(description) + " " + str(requirements)).lower()
    found_skills = []
    for skill in SKILLS_KEYWORDS:
        # Tìm từ khóa chính xác (whole word match)
        if re.search(r'\b' + re.escape(skill.lower()) + r'\b', full_text):
            found_skills.append(skill)
    return found_skills

# XỬ LÝ LƯƠNG (SALARY) 
@udf(returnType=LongType())
def parse_min_salary_udf(salary_str):
    """Trích xuất lương tối thiểu (VND)."""
    if not salary_str: return None
    salary_str = salary_str.lower().replace(',', '').replace('.', '')
    
    # Tỷ giá USD giả định
    USD_RATE = 25000
    multiplier = 1
    
    if "usd" in salary_str: multiplier = USD_RATE
    elif "tr" in salary_str or "triệu" in salary_str: multiplier = 1000000
    elif "nghìn" in salary_str or "k" in salary_str: multiplier = 1000
    
    # Tìm các con số
    numbers = re.findall(r'\d+', salary_str)
    if not numbers: return None
    
    return int(float(numbers[0]) * multiplier)

@udf(returnType=LongType())
def parse_max_salary_udf(salary_str):
    """Trích xuất lương tối đa (VND)."""
    if not salary_str: return None
    salary_str = salary_str.lower().replace(',', '').replace('.', '')
    
    USD_RATE = 25000
    multiplier = 1
    
    if "usd" in salary_str: multiplier = USD_RATE
    elif "tr" in salary_str or "triệu" in salary_str: multiplier = 1000000
    
    numbers = re.findall(r'\d+', salary_str)
    if len(numbers) >= 2:
        return int(float(numbers[1]) * multiplier)
    elif len(numbers) == 1 and ("up to" in salary_str or "tới" in salary_str):
        return int(float(numbers[0]) * multiplier)
    return None

# PHÂN LOẠI CẤP BẬC
@udf(returnType=StringType())
def classify_level_udf(job_title):
    if not job_title: return "Mid-level"
    title = job_title.lower()
    
    if any(k in title for k in ['trưởng', 'manager', 'lead', 'director', 'head', 'chief', 'cto', 'ceo']):
        return "Leader/Manager"
    if any(k in title for k in ['senior', 'chuyên viên chính', 'principal', 'expert']):
        return "Senior"
    if any(k in title for k in ['junior', 'fresher', 'intern', 'thực tập', 'trainee', 'mới tốt nghiệp']):
        return "Junior/Fresher"
        
    return "Mid-level"

# CHUẨN HÓA ĐỊA ĐIỂM 
@udf(returnType=StringType())
def standardize_location_udf(location):
    if not location: return "Khác"
    loc = location.lower()
    if "hồ chí minh" in loc or "hcm" in loc: return "TP.HCM"
    if "hà nội" in loc: return "Hà Nội"
    if "đà nẵng" in loc: return "Đà Nẵng"
    if "remote" in loc: return "Remote"
    return location.title()