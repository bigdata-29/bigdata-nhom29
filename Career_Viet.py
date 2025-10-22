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
    """Trích xuất chi tiết công việc từ nội dung HTML bằng BeautifulSoup."""
    soup = BeautifulSoup(html_content, "html.parser")
    job_details = {"url": job_url, "tieu_de": "N/A", "luong": "N/A", "kinh_nghiem": "N/A", "cap_bac": "N/A",
                   "nganh_nghe": [], "phuc_loi": [], "mo_ta_cong_viec": "", "yeu_cau_cong_viec": "",
                   "dia_diem_lam_viec": {}, "thong_tin_khac": {}}
    main_content = soup.find("section", class_="job-detail-content")
    if not main_content: return job_details
    blue_box = main_content.find("div", class_="bg-blue")
    if blue_box:
        salary_tag = blue_box.find("strong", string=lambda text: text and "Lương" in text)
        if salary_tag and salary_tag.find_next_sibling("p"): job_details["luong"] = salary_tag.find_next_sibling(
            "p").get_text(strip=True)
        exp_tag = blue_box.find("strong", string=lambda text: text and "Kinh nghiệm" in text)
        if exp_tag and exp_tag.find_next_sibling("p"): job_details["kinh_nghiem"] = exp_tag.find_next_sibling(
            "p").get_text(strip=True)
        level_tag = blue_box.find("strong", string=lambda text: text and "Cấp bậc" in text)
        if level_tag and level_tag.find_next_sibling("p"): job_details["cap_bac"] = level_tag.find_next_sibling(
            "p").get_text(strip=True)
        industry_tag = blue_box.find("strong", string=lambda text: text and "Ngành nghề" in text)
        if industry_tag and industry_tag.find_next_sibling("p"):
            industries = industry_tag.find_next_sibling("p").find_all("a")
            job_details["nganh_nghe"] = [industry.get_text(strip=True) for industry in industries]
    welfare_section = main_content.find("h2", class_="detail-title", string=lambda text: text and "Phúc lợi" in text)
    if welfare_section and welfare_section.find_next_sibling("ul", class_="welfare-list"):
        welfare_list = welfare_section.find_next_sibling("ul", class_="welfare-list").find_all("li")
        job_details["phuc_loi"] = [li.get_text(strip=True) for li in welfare_list]
    detail_rows = main_content.find_all("div", class_="detail-row")
    for row in detail_rows:
        title_tag = row.find(["h2", "h3"], class_="detail-title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            content_div = title_tag.find_next_sibling()
            if content_div:
                content_text = content_div.get_text(separator="\n", strip=True)
                if "Mô tả Công việc" in title_text:
                    job_details["mo_ta_cong_viec"] = content_text
                elif "Yêu Cầu Công Việc" in title_text:
                    job_details["yeu_cau_cong_viec"] = content_text
                elif "Địa điểm làm việc" in title_text:
                    city_tag = content_div.find("div", class_="place")
                    address_tag = content_div.find("span")
                    if city_tag: job_details["dia_diem_lam_viec"]["thanh_pho"] = city_tag.get_text(strip=True)
                    if address_tag: job_details["dia_diem_lam_viec"]["dia_chi_day_du"] = address_tag.get_text(
                        strip=True)
                elif "Thông tin khác" in title_text:
                    ul_tag = content_div.find("ul")
                    if ul_tag:
                        list_items = ul_tag.find_all("li")
                        for item in list_items:
                            parts = item.get_text(separator=":", strip=True).split(":", 1)
                            if len(parts) == 2:
                                key, value = parts[0].strip().replace(" ", "_").lower(), parts[1].strip()
                                job_details["thong_tin_khac"][key] = value
    job_title_tag = soup.find("h1", class_="detail-title")
    job_details["tieu_de"] = job_title_tag.get_text(strip=True) if job_title_tag else "N/A"
    return job_details


def get_all_job_urls(base_url_template):
    """Giai đoạn 1: Dùng một driver để thu thập tất cả URL công việc."""
    print("--- GIAI ĐOẠN 1: Bắt đầu thu thập tất cả URL công việc ---")
    driver = setup_driver()
    all_unique_urls = set()
    page_num = 1
    try:
        while True:
            current_page_url = base_url_template.format(page_num=page_num)
            print(f"Đang quét trang {page_num}...")
            driver.get(current_page_url)
            time.sleep(2)
            try:
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "job_link")))
            except TimeoutException:
                print(f"Trang {page_num} không có công việc. Dừng thu thập URL.")
                break
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height: break
                last_height = new_height
            soup_page = BeautifulSoup(driver.page_source, "html.parser")
            job_links_tags = soup_page.find_all("a", class_="job_link")
            urls_on_page = {tag.get("href") for tag in job_links_tags if
                            tag.get("href") and tag.get("href").startswith("http")}
            if not urls_on_page:
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
    output_filename = "careerviet_jobs.jsonl"

    if os.path.exists(output_filename):
        os.remove(output_filename)
        print(f"Đã xóa file cũ: {output_filename}")

    # Giai đoạn 1: Lấy tất cả URL (sử dụng phiên bản test 1 trang nếu cần)
    all_job_urls = get_all_job_urls(base_url_template)  # Dùng để chạy thật

    if not all_job_urls:
        print("Không có URL nào để xử lý. Kết thúc script.")
        return

    # Giai đoạn 2: Xử lý các URL theo lô với MỘT TAB DUY NHẤT
    print(f"\n--- GIAI ĐOẠN 2: Bắt đầu crawl chi tiết {len(all_job_urls)} công việc ---")
    driver = None
    crawled_count = 0

    for i, job_url in enumerate(all_job_urls):
        # Logic khởi động lại driver theo lô
        if i % BATCH_SIZE == 0:
            if driver:
                print(f"\n--- Đạt đến kích thước lô ({BATCH_SIZE}), khởi động lại trình duyệt ---")
                driver.quit()

            print(f"Khởi động trình duyệt cho lô bắt đầu từ công việc số {i + 1}...")
            driver = setup_driver()
            # Bước "warm-up" vẫn hữu ích để xử lý cookie/popup
            try:
                driver.get("https://careerviet.vn/")
                time.sleep(2)
            except Exception as e:
                print(f"    -> Lỗi trong quá trình warm-up: {e}")

        print(f"  [{i + 1}/{len(all_job_urls)}] Đang xử lý: {job_url}")

        try:
            # Điều hướng trên cùng một tab
            driver.get(job_url)

            # THAY ĐỔI LỚN: Sửa lại điều kiện WebDriverWait
            # Chờ sự xuất hiện của tiêu đề "Mô tả Công việc"
            # XPath này tìm thẻ h2 hoặc h3 có class 'detail-title' và chứa text 'Mô tả Công việc'
            wait_condition = (By.XPATH,
                              "//*[self::h2 or self::h3][@class='detail-title' and contains(text(), 'Mô tả Công việc')]")

            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located(wait_condition)
            )

            time.sleep(1)  # Chờ thêm cho ổn định

            # Lấy dữ liệu và lưu
            job_detail_html = driver.page_source
            job_data = extract_job_details(job_detail_html, job_url)

            if job_data:
                with open(output_filename, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(job_data, ensure_ascii=False) + '\n')
                crawled_count += 1
                print(f"    -> Đã lưu thành công: {job_data.get('tieu_de', 'N/A')}")

            time.sleep(2)

        except Exception as e:
            print(f"    -> Lỗi khi xử lý {job_url}: {type(e).__name__}. Bỏ qua.")
            continue

    # Đóng driver cuối cùng sau khi vòng lặp kết thúc
    if driver:
        driver.quit()

    print(f"\nHoàn tất. Đã lưu tổng cộng {crawled_count} công việc vào file '{output_filename}'.")


if __name__ == "__main__":
    main()