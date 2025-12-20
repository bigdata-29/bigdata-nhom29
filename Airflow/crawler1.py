import re
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
from hdfs import InsecureClient
from webdriver_manager.chrome import ChromeDriverManager

SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080
BATCH_SIZE = 25

def setup_driver():
    """Cấu hình và khởi tạo ChromeDriver với các tùy chọn nâng cao."""
    chrome_options = Options()
    chrome_options.add_argument("--app")
    chrome_options.add_argument(f"--window-size={SCREEN_WIDTH},{SCREEN_HEIGHT}")
    chrome_options.add_argument("--window-position=0,0")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


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

def get_all_job_urls(base_url_template):
    print("--- GIAI ĐOẠN 1: Bắt đầu thu thập tất cả URL công việc ---")
    driver = setup_driver()
    all_unique_urls = set()
    page_num = 1
    try:
        while page_num <= 50:  # Giới hạn 50 trang để tránh chạy vô tận
            current_page_url = base_url_template.format(page_num=page_num)
            print(f"Đang quét trang {page_num}...")
            driver.get(current_page_url)
            time.sleep(2)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "job_link")))
            except TimeoutException:
                print(f"Trang {page_num} không có công việc hoặc hết thời gian chờ. Dừng thu thập URL.")
                break

            last_height = driver.execute_script("return document.body.scrollHeight")
            for _ in range(3):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2.5)  # Tăng thời gian chờ
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height: break
                last_height = new_height

            soup_page = BeautifulSoup(driver.page_source, "html.parser")
            job_links_tags = soup_page.find_all("a", class_="job_link")
            urls_on_page = {tag.get("href") for tag in job_links_tags if
                            tag.get("href") and tag.get("href").startswith("http")}

            if not urls_on_page and page_num > 1:
                print(f"Trang {page_num} không có link nào sau khi cuộn. Dừng.")
                break

            all_unique_urls.update(urls_on_page)
            print(f"  -> Đã tìm thấy {len(urls_on_page)} URL. Tổng số URL duy nhất: {len(all_unique_urls)}")
            page_num += 1
            time.sleep(1)
    except Exception as e:
        print(f"Lỗi trong quá trình thu thập URL: {e}")
    finally:
        if driver: driver.quit()
    print(f"--- KẾT THÚC GIAI ĐOẠN 1: Thu thập được {len(all_unique_urls)} URL duy nhất ---")
    return list(all_unique_urls)


def main():
    base_url_template = "https://careerviet.vn/viec-lam/cntt-phan-cung-mang-cntt-phan-mem-c63,1-trang-{page_num}-vi.html"
    local_output = "/tmp/careerviet_jobs.jsonl"  # lưu tạm trong WSL2
    hdfs_output_path = "/data/careerviet_jobs.jsonl"  # đường dẫn đích trên HDFS

    if os.path.exists(local_output):
        os.remove(local_output)
        print(f"Đã xóa file cũ: {local_output}")

    all_job_urls = get_all_job_urls(base_url_template)

    if not all_job_urls:
        print("Không có URL nào để xử lý. Kết thúc script.")
        return

    print(f"\n--- GIAI ĐOẠN 2: Bắt đầu crawl chi tiết {len(all_job_urls)} công việc ---")
    driver = None
    crawled_count = 0

    for i, job_url in enumerate(all_job_urls):
        if i % BATCH_SIZE == 0:
            if driver:
                print(f"\n--- Đạt đến kích thước lô ({BATCH_SIZE}), khởi động lại trình duyệt ---")
                driver.quit()

            print(f"Khởi động trình duyệt cho lô bắt đầu từ công việc số {i + 1}...")
            driver = setup_driver()
            try:
                driver.get("https://careerviet.vn/")
                time.sleep(2)
            except Exception as e:
                print(f"    -> Lỗi trong quá trình warm-up: {e}")

        print(f"  [{i + 1}/{len(all_job_urls)}] Đang xử lý: {job_url}")

        try:
            driver.get(job_url)

            WebDriverWait(driver, 20).until(
                EC.any_of(
                    EC.presence_of_element_located((By.XPATH,
                                                    "//*[self::h2 or self::h3][@class='detail-title' and contains(text(), 'Mô tả Công việc')]")),
                    EC.presence_of_element_located((By.XPATH, "//strong[contains(., 'Ngày cập nhật')]"))
                )
            )
            time.sleep(1.5)  # Tăng thời gian chờ cho ổn định

            job_detail_html = driver.page_source
            job_data = extract_job_details(job_detail_html, job_url)

            if job_data:
                with open(local_output, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(job_data, ensure_ascii=False) + '\n')
                crawled_count += 1
                print(
                    f"    -> Đã lưu thành công: {job_data.get('ten_cong_ty', 'N/A')} - {job_data.get('tieu_de', 'N/A')}")

            time.sleep(2)
        except Exception as e:
            print(f"    -> Lỗi khi xử lý {job_url}: {type(e).__name__} - {e}. Bỏ qua.")
            continue

    if driver:
        driver.quit()

    print(f"\nHoàn tất. Đã lưu tổng cộng {crawled_count} công việc vào file local '{local_output}'.")

    #  Upload file lên HDFS (ngoài Kubernetes)
    try:
        #  THAY đổi tại đây: dùng localhost:9870 (vì HDFS chạy ngoài Kubenetes)
        client = InsecureClient("http://172.21.87.65:9870", user="hoanvdtd")
        client.upload(hdfs_output_path, local_output, overwrite=True)
        print(f" Đã upload file lên HDFS: {hdfs_output_path}")
    except Exception as e:
        print(f" Lỗi khi upload lên HDFS: {e}")

if __name__ == "__main__":
    main()
