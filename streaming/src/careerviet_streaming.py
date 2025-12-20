import threading
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import time
import datetime
import re
import os
from webdriver_manager.chrome import ChromeDriverManager


try:
    from kafka_producer import JobProducer
except ImportError:
    print(" Lỗi: Không tìm thấy file kafka_producer.py.")
    exit(1)

# --- CẤU HÌNH ---
MAX_WORKERS = 3  # Số luồng (số trình duyệt chạy cùng lúc). 
PAGES_TO_CRAWL = 5 # Số lượng trang danh sách muốn cào 
BASE_URL_TEMPLATE = "https://careerviet.vn/viec-lam/cntt-phan-cung-mang-cntt-phan-mem-c63,1-trang-{page_num}-vi.html"

def setup_driver():
    """Khởi tạo driver riêng biệt cho mỗi luồng."""
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Chạy ẩn để đỡ tốn tài nguyên
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Tắt log rác
    chrome_options.add_argument("--log-level=3")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def extract_job_details(html_content, job_url):
    """Trích xuất chi tiết công việc từ trang job."""
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Schema chuẩn cho hệ thống
    job_details = {
        "url": job_url,
        "job_title": "N/A",
        "company_name": "N/A",
        "salary": "N/A",
        "company_location": "",
        "job_description": "",
        "job_requirements": "",
        "post_time": datetime.datetime.now().strftime("%d/%m/%Y"), # Default
        "skills_list": [],
        "working_model": "Trực tiếp", # CareerViet ít ghi cái này, để mặc định
        "source": "careerviet.vn",
        "crawled_at": datetime.datetime.now().isoformat()
    }

    
    # Header
    job_header = soup.find("div", class_="job-desc")
    if job_header:
        title_tag = job_header.find("h1", class_="title")
        if title_tag: job_details["job_title"] = title_tag.get_text(strip=True)

        company_tag = job_header.find("a", class_="job-company-name")
        if company_tag: job_details["company_name"] = company_tag.get_text(strip=True)

    # Blue Box Info
    blue_box = soup.find("div", class_="bg-blue")
    if blue_box:
        location_tag = blue_box.select_one("strong:-soup-contains('Địa điểm') + p a")
        if location_tag:
            job_details["company_location"] = location_tag.get_text(strip=True)

        update_date_tag = blue_box.select_one("strong:-soup-contains('Ngày cập nhật') + p")
        if update_date_tag:
            job_details["post_time"] = update_date_tag.get_text(strip=True)

        salary_tag = blue_box.select_one("strong:-soup-contains('Lương') + p")
        if salary_tag:
            job_details["salary"] = salary_tag.get_text(strip=True)

    # Description & Requirements
    description_title = soup.find('h2', class_='detail-title', string=re.compile(r"Mô tả Công việc", re.IGNORECASE))
    if description_title:
        content_tags = description_title.find_next_siblings('p')
        job_details["job_description"] = '\n'.join(p.get_text(separator='\n', strip=True) for p in content_tags)

    requirement_title = soup.find('h2', class_='detail-title', string=re.compile(r"Yêu Cầu Công Việc", re.IGNORECASE))
    if requirement_title:
        content_tags = requirement_title.find_next_siblings('p')
        job_details["job_requirements"] = '\n'.join(p.get_text(separator='\n', strip=True) for p in content_tags)

    return job_details

def process_single_page(page_num, producer):
    """
    Hàm worker: Chạy trên một luồng riêng.
    Nhiệm vụ: Mở trình duyệt -> Vào trang danh sách X -> Lấy link -> Vào từng link -> Gửi Kafka.
    """
    thread_name = threading.current_thread().name
    print(f"[{thread_name}]  Bắt đầu xử lý trang {page_num}...")
    
    driver = None
    try:
        driver = setup_driver()
        list_url = BASE_URL_TEMPLATE.format(page_num=page_num)
        
        # 1. Lấy danh sách link trên trang này
        driver.get(list_url)
        try:
            # Chờ link job xuất hiện
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "job_link")))
        except TimeoutException:
            print(f"[{thread_name}]  Trang {page_num} không có bài đăng hoặc load lỗi.")
            return

        # Cuộn trang để load hết (lazy load)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        
        soup_page = BeautifulSoup(driver.page_source, "html.parser")
        job_links_tags = soup_page.find_all("a", class_="job_link")
        
        # Lọc link hợp lệ
        urls_on_page = list({tag.get("href") for tag in job_links_tags if tag.get("href") and tag.get("href").startswith("http")})
        print(f"[{thread_name}] -> Tìm thấy {len(urls_on_page)} jobs trên trang {page_num}.")

        # 2. Duyệt từng link và gửi Kafka ngay lập tức
        for i, job_url in enumerate(urls_on_page):
            try:
                driver.get(job_url)
                
                # Chờ tiêu đề load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1.title, div.job-desc"))
                )
                
                # Parse
                detail = extract_job_details(driver.page_source, job_url)
                
                # Gửi Kafka
                producer.send_job(detail)
                print(f"[{thread_name}]  Đã gửi: {detail['job_title']}")
                
                # Nghỉ ngắn để server không chặn
                time.sleep(2) 
                
            except Exception as e:
                print(f"[{thread_name}]  Lỗi link {job_url}: {e}")
                continue

    except Exception as e:
        print(f"[{thread_name}]  Lỗi nghiêm trọng trang {page_num}: {e}")
    finally:
        if driver:
            driver.quit()
        print(f"[{thread_name}]  Hoàn thành trang {page_num}.")

def main():
    print(f"--- BẮT ĐẦU CÀO SONG SONG ({MAX_WORKERS} LUỒNG) ---")
    
    # Khởi tạo Producer (Dùng chung cho các thread vì KafkaProducer thread-safe)
    producer = JobProducer()
    
    # Sử dụng ThreadPool để quản lý luồng
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Tạo danh sách các task (mỗi trang là 1 task)
        # cào từ trang 1 đến PAGES_TO_CRAWL
        futures = [
            executor.submit(process_single_page, page_num, producer) 
            for page_num in range(1, PAGES_TO_CRAWL + 1)
        ]
        
        # Chờ tất cả hoàn thành
        for future in futures:
            future.result()

    producer.close()
    print("--- HOÀN TẤT TOÀN BỘ ---")

if __name__ == "__main__":
    main()