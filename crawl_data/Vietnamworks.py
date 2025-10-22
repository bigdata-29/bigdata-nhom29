# ===================================
# VIETNAMWORKS JOB SCRAPER - JSON ONLY
# ===================================

# BƯỚC 1: Cài đặt thư viện cần thiết (chỉ cần chạy lần đầu)
# pip install beautifulsoup4 selenium webdriver-manager -q
# apt-get update -qq
# apt-get install -qq chromium-chromedriver

# ===================================
# IMPORT
# ===================================
import sys
import re
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===================================
# SETUP SELENIUM
# ===================================
def setup_driver():
    """Khởi tạo Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# ===================================
# HÀM SCRAPING CHI TIẾT
# ===================================
def extract_job_detail(html_content):
    """Trích xuất thông tin chi tiết từ trang công việc"""
    soup = BeautifulSoup(html_content, "html.parser")
    detail = {
        "mo_ta": "",
        "yeu_cau": "",
        "phuc_loi": [],
        "dia_diem_chi_tiet": [],
        "ngay_dang_chi_tiet": "",
        "cap_bac": "",
        "nganh_nghe": "",
        "ky_nang_chi_tiet": [],
        "linh_vuc": "",
        "ngon_ngu": "",
        "kinh_nghiem": "",
        "quoc_tich": ""
    }
    try:
        # Mô tả
        mo_ta_section = soup.find("div", class_=re.compile(r'sc-1671001a-1'))
        if mo_ta_section:
            mo_ta_div = mo_ta_section.find("div", class_=re.compile(r'sc-1671001a-6'))
            if mo_ta_div:
                detail["mo_ta"] = mo_ta_div.get_text(strip=True, separator="\n")
        # Yêu cầu
        all_sections = soup.find_all("div", class_=re.compile(r'sc-1671001a-4'))
        if len(all_sections) >= 2:
            yeu_cau_div = all_sections[1].find("div", class_=re.compile(r'sc-1671001a-6'))
            if yeu_cau_div:
                detail["yeu_cau"] = yeu_cau_div.get_text(strip=True, separator="\n")
        # Phúc lợi
        phuc_loi_section = soup.find("div", class_=re.compile(r'sc-c683181c-0'))
        if phuc_loi_section:
            benefit_items = phuc_loi_section.find_all("div", class_=re.compile(r'sc-c683181c-1'))
            for item in benefit_items:
                benefit_title = item.find("p", class_=re.compile(r'sc-ab270149-0.*jlpjAq'))
                benefit_desc = item.find("div", class_=re.compile(r'sc-c683181c-2'))
                if benefit_title and benefit_desc:
                    detail["phuc_loi"].append({
                        "tieu_de": benefit_title.get_text(strip=True),
                        "mo_ta": benefit_desc.get_text(strip=True)
                    })
        # Thông tin chi tiết khác
        info_section = soup.find("div", class_=re.compile(r'sc-7bf5461f-0'))
        if info_section:
            info_items = info_section.find_all("div", class_=re.compile(r'sc-7bf5461f-1'))
            for item in info_items:
                label_elem = item.find("label", class_=re.compile(r'sc-ab270149-0.*dfyRSX'))
                value_elem = item.find("p", class_=re.compile(r'sc-ab270149-0.*cLLblL'))
                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True)
                    value = value_elem.get_text(strip=True)
                    if "NGÀY ĐĂNG" in label: detail["ngay_dang_chi_tiet"] = value
                    elif "CẤP BẬC" in label: detail["cap_bac"] = value
                    elif "NGÀNH NGHỀ" in label: detail["nganh_nghe"] = value
                    elif "KỸ NĂNG" in label: detail["ky_nang_chi_tiet"] = [s.strip() for s in value.split(",")]
                    elif "LĨNH VỰC" in label: detail["linh_vuc"] = value
                    elif "NGÔN NGỮ" in label: detail["ngon_ngu"] = value
                    elif "KINH NGHIỆM" in label or "NĂM KINH NGHIỆM" in label: detail["kinh_nghiem"] = value
                    elif "QUỐC TỊCH" in label: detail["quoc_tich"] = value
        # Địa điểm
        dia_diem_section = soup.find("div", class_=re.compile(r'sc-a137b890-0'))
        if dia_diem_section:
            location_items = dia_diem_section.find_all("div", class_=re.compile(r'sc-a137b890-1'))
            for item in location_items:
                location_text = item.find("p", class_=re.compile(r'sc-ab270149-0.*cLLblL'))
                if location_text:
                    detail["dia_diem_chi_tiet"].append(location_text.get_text(strip=True))
    except Exception as e:
        print(f"Lỗi khi parse chi tiết: {e}")
    return detail

# ===================================
# HÀM SCRAPING DANH SÁCH
# ===================================
def extract_job_list_vnw(html_content, driver=None, base_url="https://www.vietnamworks.com", crawl_detail=True):
    soup = BeautifulSoup(html_content, "html.parser")
    jobs = []
    job_cards = soup.find_all("div", class_=re.compile(r'job-item|search_list|view_job_item|job-card'))
    if not job_cards:
        job_cards = soup.find_all("div", {"data-job-id": True})

    for idx, card in enumerate(job_cards):
        job_data = {
            "url": "N/A",
            "tieu_de": "N/A",
            "luong": "N/A",
            "cong_ty": "N/A",
            "dia_diem": "N/A",
            "ngay_dang": "N/A",
            "ky_nang": [],
            "trang_thai": "",
            # Chi tiết
            "mo_ta": "",
            "yeu_cau": "",
            "phuc_loi": [],
            "dia_diem_chi_tiet": [],
            "ngay_dang_chi_tiet": "",
            "cap_bac": "",
            "nganh_nghe": "",
            "ky_nang_chi_tiet": [],
            "linh_vuc": "",
            "ngon_ngu": "",
            "kinh_nghiem": "",
            "quoc_tich": ""
        }
        # Tiêu đề + URL
        title_tag = card.find("h2") or card.find("h3") or card.find("a", class_=re.compile(r'job-title|title'))
        if title_tag:
            link = title_tag.find("a") if title_tag.name != "a" else title_tag
            if link:
                job_data["tieu_de"] = re.sub(r'^(Mới|New)\s+', '', link.get_text(strip=True))
                href = link.get("href", "")
                if href: job_data["url"] = base_url + href if href.startswith("/") else href
        # Công ty
        company_tag = card.find("div", class_=re.compile(r'company|sc-cdaca')) or card.find("a", href=re.compile(r'/nha-tuyen-dung/'))
        if company_tag:
            company_link = company_tag.find("a") if company_tag.name != "a" else company_tag
            job_data["cong_ty"] = company_link.get_text(strip=True) if company_link else company_tag.get_text(strip=True)
        # Lương
        salary_tag = card.find("span", class_=re.compile(r'salary|sc-fgSWkL|gKHoAZ')) or card.find("div", class_=re.compile(r'salary'))
        if salary_tag: job_data["luong"] = salary_tag.get_text(strip=True)
        # Địa điểm
        location_tag = card.find("span", class_=re.compile(r'location|sc-kzkBiZ|hAkUGp')) or card.find("div", class_=re.compile(r'location'))
        if location_tag: job_data["dia_diem"] = location_tag.get_text(strip=True)
        # Ngày đăng
        date_tag = card.find("div", class_=re.compile(r'date|time|sc-fnLEGM|fVEuMk')) or card.find("span", class_=re.compile(r'date|time'))
        if date_tag: job_data["ngay_dang"] = date_tag.get_text(strip=True)
        # Kỹ năng
        skill_tags = card.find_all("label", class_=re.compile(r'skill|tag|sc-cgjDci|brZjJh')) or card.find_all("span", class_=re.compile(r'skill|tag'))
        if skill_tags: job_data["ky_nang"] = [tag.get_text(strip=True) for tag in skill_tags if tag.get_text(strip=True)]
        # Crawl chi tiết
        if crawl_detail and job_data["url"] != "N/A" and driver:
            try:
                detail_html = fetch_with_selenium(job_data["url"], driver, wait_time=3, silent=True)
                if detail_html:
                    detail_info = extract_job_detail(detail_html)
                    job_data.update(detail_info)
            except Exception as e:
                print(f"Lỗi crawl chi tiết: {e}")
        if job_data["tieu_de"] != "N/A":
            jobs.append(job_data)
    return jobs

# ===================================
# HÀM FETCH
# ===================================
def fetch_with_selenium(url, driver, wait_time=5, silent=False):
    try:
        if not silent: print(f" Đang tải: {url}")
        driver.get(url)
        time.sleep(wait_time)
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='job'], div[class*='search_list'], div[class*='sc-1671001a']"))
            )
        except:
            if not silent: print("Timeout khi đợi nội dung")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        return driver.page_source
    except Exception as e:
        if not silent: print(f"Lỗi fetch: {e}")
        return None

# ===================================
# MAIN - TỰ ĐỘNG CRAWL NHIỀU TRANG
# ===================================
if __name__ == "__main__":
    SEARCH_URL = "https://www.vietnamworks.com/viec-lam?g=5"
    CRAWL_DETAIL = True      # crawl chi tiết từng job (nếu muốn nhanh thì False)
    DELAY = 3                # giây nghỉ giữa các trang
    MAX_PAGES = 30           # phòng trường hợp có hơn 15 trang
    EXPECTED_JOBS_PER_PAGE = 50

    driver = setup_driver()
    all_jobs = []
    start_time = time.time()

    try:
        for page in range(1, MAX_PAGES + 1):
            url = f"{SEARCH_URL}&page={page}" if page > 1 else SEARCH_URL
            print(f"\n Crawling trang {page}: {url}")

            html = fetch_with_selenium(url, driver, wait_time=5)
            if not html:
                print(" Không tải được trang, dừng lại.")
                break

            jobs = extract_job_list_vnw(html, driver=driver, crawl_detail=CRAWL_DETAIL)
            if not jobs:
                print(f" Không tìm thấy công việc nào trên trang {page}, dừng lại.")
                break

            print(f" Thu được {len(jobs)} công việc trên trang {page}")
            all_jobs.extend(jobs)

            # Nếu trang hiện tại có ít hơn 50 job => có thể là trang cuối
            if len(jobs) < EXPECTED_JOBS_PER_PAGE:
                print(" Có vẻ đã đến trang cuối, dừng lại.")
                break

            time.sleep(DELAY)

    finally:
        driver.quit()

    elapsed_time = time.time() - start_time
    print(f"\n Tổng số công việc: {len(all_jobs)} | Thời gian: {elapsed_time:.2f}s")

    # Lưu ra JSON
    if all_jobs:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_filename = f"vietnamworks_jobs_{timestamp}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_jobs, f, ensure_ascii=False, indent=2)
        print(f" Đã lưu JSON: {json_filename}")
    else:
        print(" Không tìm thấy công việc nào.")
