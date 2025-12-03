import json
import os
import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from bs4 import BeautifulSoup
import time
import json
import os
from webdriver_manager.chrome import ChromeDriverManager

# --- Cấu hình ---
SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080
BATCH_SIZE = 25


def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver
# (Giữ nguyên các import setup driver của bạn ở trên)
# ... Import code setup_driver, extract_job_details cũ ...

# --- CẤU HÌNH STREAM ---
DATA_FOLDER = "stream_data"  # Thư mục chứa dữ liệu đầu vào cho Spark
CHECK_INTERVAL = 300  # Quét lại sau mỗi 5 phút (300s)
SEEN_URLS = set()  # Bộ nhớ tạm để tránh crawl lại tin cũ

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)


def extract_job_details(html_content, job_url):
    soup = BeautifulSoup(html_content, "html.parser")
    job_details = {
        "url": job_url,
        "ten_cong_ty": "N/A",
        "tieu_de": "N/A",
        "ngay_dang_tuyen": "N/A",
        "luong": "N/A",
        "kinh_nghiem": "N/A",  # Trang này không có trường kinh nghiệm riêng
        "cap_bac": "N/A",
        "nganh_nghe": [],
        "phuc_loi": [],
        "mo_ta_cong_viec": "",
        "yeu_cau_cong_viec": "",
        "dia_diem_lam_viec": {},
        "thong_tin_khac": {}
    }

    # 1. Trích xuất Tiêu đề và Tên công ty (trong header)
    # Phần này đã tốt, nhưng có thể trang không có header.
    # Ta sẽ tìm lại ở phần khác nếu không thấy.
    job_header = soup.find("div", class_="job-desc")
    if job_header:
        title_tag = job_header.find("h1", class_="title")
        if title_tag: job_details["tieu_de"] = title_tag.get_text(strip=True)

        company_tag = job_header.find("a", class_="job-company-name")
        if company_tag: job_details["ten_cong_ty"] = company_tag.get_text(strip=True)

    # 2. Trích xuất các thông tin chính từ box màu xanh (chính xác và hiệu quả hơn)
    blue_box = soup.find("div", class_="bg-blue")
    if blue_box:
        location_tag = blue_box.select_one("strong:-soup-contains('Địa điểm') + p a")
        if location_tag:
            job_details["dia_diem_lam_viec"]["thanh_pho"] = location_tag.get_text(strip=True)

        update_date_tag = blue_box.select_one("strong:-soup-contains('Ngày cập nhật') + p")
        if update_date_tag:
            job_details["ngay_dang_tuyen"] = update_date_tag.get_text(strip=True)

        industry_tags = blue_box.select("strong:-soup-contains('Ngành nghề') + p a")
        if industry_tags:
            job_details["nganh_nghe"] = [tag.get_text(strip=True) for tag in industry_tags]

        salary_tag = blue_box.select_one("strong:-soup-contains('Lương') + p")
        if salary_tag:
            job_details["luong"] = salary_tag.get_text(strip=True)

        level_tag = blue_box.select_one("strong:-soup-contains('Cấp bậc') + p")
        if level_tag:
            job_details["cap_bac"] = level_tag.get_text(strip=True)

    # =============================================================================
    # PHẦN SỬA LỖI CHÍNH: CRAWL MÔ TẢ VÀ YÊU CẦU CÔNG VIỆC
    # =============================================================================

    # Tìm thẻ h2 chứa "Mô tả Công việc"
    # re.compile giúp tìm kiếm linh hoạt, không phân biệt hoa thường
    description_title = soup.find('h2', class_='detail-title', string=re.compile(r"Mô tả Công việc", re.IGNORECASE))
    if description_title:
        # Từ tiêu đề, tìm thẻ cha bao bọc nó
        parent_div = description_title.find_parent('div', class_='detail-row')
        if parent_div:
            # Lấy tất cả các thẻ <p> là "em" của tiêu đề
            content_tags = description_title.find_next_siblings('p')
            job_details["mo_ta_cong_viec"] = '\n'.join(p.get_text(separator='\n', strip=True) for p in content_tags)

    # Tương tự cho "Yêu Cầu Công Việc"
    requirement_title = soup.find('h2', class_='detail-title', string=re.compile(r"Yêu Cầu Công Việc", re.IGNORECASE))
    if requirement_title:
        parent_div = requirement_title.find_parent('div', class_='detail-row')
        if parent_div:
            content_tags = requirement_title.find_next_siblings('p')
            job_details["yeu_cau_cong_viec"] = '\n'.join(p.get_text(separator='\n', strip=True) for p in content_tags)

    # =============================================================================

    # 4. Trích xuất Phúc lợi (đã tốt)
    welfare_section = soup.find("h2", class_="detail-title", string=re.compile(r"Phúc lợi", re.IGNORECASE))
    if welfare_section:
        welfare_list_ul = welfare_section.find_next_sibling("ul", class_="welfare-list")
        if welfare_list_ul:
            welfare_items = welfare_list_ul.find_all("li")
            job_details["phuc_loi"] = [li.get_text(strip=True) for li in welfare_items]

    # 5. Trích xuất Địa chỉ chi tiết và Thông tin khác (chính xác hơn)
    address_detail_section = soup.find("div", class_="info-place-detail")
    if address_detail_section:
        address_span = address_detail_section.find("span")
        if address_span:
            job_details["dia_diem_lam_viec"]["dia_chi_day_du"] = address_span.get_text(strip=True).strip()

    other_info_section = soup.find("h3", class_="detail-title", string=re.compile(r"Thông tin khác", re.IGNORECASE))
    if other_info_section:
        content_div = other_info_section.find_next_sibling("div", class_="content_fck")
        if content_div:
            list_items = content_div.find_all("li")
            for li_item in list_items:
                parts = [part.strip() for part in li_item.get_text(strip=True).split(':', 1)]
                if len(parts) == 2:
                    key = parts[0].lower().replace(" ", "_")
                    value = parts[1].strip()
                    job_details["thong_tin_khac"][key] = value

    return job_details

# --- HÀM BỔ SUNG ĐỂ TRÍCH XUẤT CÁCH THỨC LÀM VIỆC ---
def detect_work_method(job_details):
    # Logic đơn giản để detect Remote/Hybrid dựa trên text
    full_text = (job_details.get("mo_ta_cong_viec", "") + " " +
                 job_details.get("tieu_de", "")).lower()

    if "remote" in full_text or "làm việc từ xa" in full_text:
        return "Remote"
    elif "hybrid" in full_text or "linh hoạt" in full_text:
        return "Hybrid"
    else:
        return "Office"  # Mặc định


def run_streaming_producer():
    print("--- BẮT ĐẦU CHẾ ĐỘ STREAMING 24/7 (JSON LINES) ---")
    driver = setup_driver()
    base_url = "https://careerviet.vn/viec-lam/cntt-phan-cung-mang-cntt-phan-mem-c63,1-vi.html"

    try:
        while True:
            print(f"\n[Time: {datetime.now().strftime('%H:%M:%S')}] Đang quét tin mới...")
            try:
                driver.get(base_url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "job_link")))

                soup = BeautifulSoup(driver.page_source, "html.parser")
                job_links_tags = soup.find_all("a", class_="job_link")
                current_urls = {tag.get("href") for tag in job_links_tags if tag.get("href")}

                new_urls = [url for url in current_urls if url not in SEEN_URLS]

                if not new_urls:
                    print(" -> Không có tin mới. Ngủ chờ...")
                else:
                    print(f" -> Tìm thấy {len(new_urls)} tin mới. Bắt đầu crawl...")
                    batch_data = []

                    for url in new_urls:
                        try:
                            driver.get(url)
                            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "job-desc")))

                            details = extract_job_details(driver.page_source, url)
                            details['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            details['cach_thuc_lam_viec'] = detect_work_method(details)

                            batch_data.append(details)
                            SEEN_URLS.add(url)
                            print(f"    + Crawled: {details.get('tieu_de')}")

                        except Exception as e:
                            print(f"    ! Lỗi link {url}: {e}")
                            continue

                    # --- GHI FILE JSONL (Cập nhật đuôi file) ---
                    if batch_data:
                        # Thay đổi đuôi file thành .jsonl
                        file_name = f"{DATA_FOLDER}/batch_{int(time.time())}.jsonl"

                        with open(file_name, 'w', encoding='utf-8') as f:
                            for job in batch_data:
                                # Ghi từng object trên một dòng (JSON Lines standard)
                                f.write(json.dumps(job, ensure_ascii=False) + '\n')

                        print(f" -> Đã lưu file: {file_name} ({len(batch_data)} jobs)")

            except Exception as e:
                print(f"Lỗi vòng lặp chính: {e}")
                if driver:
                    driver.quit()
                driver = setup_driver()

            print(f"Đợi {CHECK_INTERVAL} giây trước lần quét tiếp theo...")
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("Dừng Streaming.")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    run_streaming_producer()