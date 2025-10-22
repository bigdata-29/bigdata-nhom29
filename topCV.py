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

# from webdriver_manager.chrome import ChromeDriverManager
# --- Cấu hình ---
SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080
BATCH_SIZE = 25  # Xử lý 25 công việc rồi khởi động lại trình duyệt


# Xóa import này nếu có, chúng ta không cần nó nữa
# from webdriver_manager.chrome import ChromeDriverManager

# --- GIỮ NGUYÊN HÀM NÀY, NÓ ĐÃ ĐÚNG ---

# Đảm bảo bạn CÓ import dòng này ở đầu file
from webdriver_manager.chrome import ChromeDriverManager

SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080
BATCH_SIZE = 25  # Xử lý 25 công việc rồi khởi động lại trình duyệt


def setup_driver():
    """Cấu hình và khởi tạo ChromeDriver với các tùy chọn nâng cao."""
    # ... (Hàm này không thay đổi) ...
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
    # ... (Hàm này không thay đổi) ...
    soup = BeautifulSoup(html_content, "html.parser")
    job_details = {"url": job_url, "tieu_de": "N/A", "luong": "N/A", "kinh_nghiem": "N/A", "cap_bac": "N/A",
                   "nganh_nghe": [], "phuc_loi": [], "mo_ta_cong_viec": "", "yeu_cau_cong_viec": "",
                   "dia_diem_lam_viec": {}, "thong_tin_khac": {}}
    main_content = soup.find("div", class_="job-detail__body")
    if not main_content: return job_details
    blue_box = main_content.find("div", class_="job-detail__body-left")
    if blue_box:
        # Lương
        salary_tag = main_content.find("div",
                                       class_="job-detail__info--section-content-title",
                                       string=lambda text: text and "Mức lương" in text.strip())
        if salary_tag:
            value_tag = salary_tag.find_next_sibling("div", class_="job-detail__info--section-content-value")
            if value_tag:
                job_details["luong"] = value_tag.get_text(strip=True)

        # Kinh nghiệm
        exp_tag = main_content.find("div",
                                    class_="job-detail__info--section-content-title",
                                    string=lambda text: text and "Kinh nghiệm" in text.strip())
        if exp_tag:
            value_tag = exp_tag.find_next_sibling("div", class_="job-detail__info--section-content-value")
            if value_tag:
                job_details["kinh_nghiem"] = value_tag.get_text(strip=True)

        # Phúc lợi...
        description_container = main_content.find("div", class_="job-description")
        if description_container:
            detail_items = description_container.find_all("div", class_="job-description__item")

            for item in detail_items:
                title_tag = item.find("h3")
                content_div = item.find("div", class_="job-description__item--content")

                if title_tag and content_div:
                    title_text = title_tag.get_text(strip=True)

                    if "Mô tả công việc" in title_text:
                        job_details["mo_ta_cong_viec"] = content_div.get_text(separator="\n", strip=True)

                    elif "Yêu cầu ứng viên" in title_text:
                        job_details["yeu_cau_cong_viec"] = content_div.get_text(separator="\n", strip=True)

                    elif "Quyền lợi" in title_text:
                        benefits = content_div.find_all("p")
                        job_details["phuc_loi"] = [b.get_text(strip=True) for b in benefits if b.get_text(strip=True)]

                    elif "Địa điểm làm việc" in title_text:
                        address_div = content_div.find("div")
                        if address_div:
                            address_text = address_div.get_text(strip=True)
                            parts = address_text.split(":", 1)

                            if len(parts) == 2:
                                city = parts[0].replace("-", "").strip()
                                address = parts[1].strip()
                                job_details["dia_diem_lam_viec"]["thanh_pho"] = city
                                job_details["dia_diem_lam_viec"]["dia_chi_day_du"] = address
                            else:
                                job_details["dia_diem_lam_viec"]["dia_chi_day_du"] = address_text
                    elif "Thời gian làm việc" in title_text:
                        content_text = content_div.get_text(strip=True)
                        job_details["thong_tin_khac"]["thoi_gian_lam_viec"] = content_text

    other_detail = main_content.find("div", class_="job-detail__body-right")
    if other_detail:
        # Cấp bậc:
        level_tag = other_detail.find("div",
                                      class_="box-general-group-info-title",
                                      string=lambda text: text and "Cấp bậc" in text.strip())
        if level_tag:
            value_tag = level_tag.find_next_sibling("div", class_="box-general-group-info-value")
            if value_tag:
                job_details["cap_bac"] = value_tag.get_text(strip=True)
    job_title_tag = soup.find("h1", class_="job-detail__title")
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
                # --- SỬA 1: DÙNG CSS SELECTOR "CONTAINS" ---
                # h3[class*="title"] a
                # Nghĩa là: "Tìm thẻ <a> BÊN TRONG thẻ <h3>
                # mà thuộc tính 'class' của nó CÓ CHỨA chữ 'title'"
                wait_selector = 'h3[class*="title"] a'
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_selector))
                )
            except TimeoutException:
                print(f"Trang {page_num} không có công việc (selector '{wait_selector}' not found).")
                print("!!! Đang lưu ảnh chụp màn hình và HTML để kiểm tra...")
                driver.save_screenshot("debug_screenshot.png")
                with open("debug_page.html", "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                print("!!! ĐÃ LƯU: 'debug_screenshot.png' và 'debug_page.html'.")
                break

            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height: break
                last_height = new_height

            soup_page = BeautifulSoup(driver.page_source, "html.parser")

            # --- SỬA 2: DÙNG CÙNG CSS SELECTOR "CONTAINS" ---
            job_links_tags = soup_page.select('h3[class*="title"] a')

            # --- SỬA 3: XỬ LÝ LINK (GIỮ NGUYÊN) ---
            urls_on_page = {tag.get("href") for tag in job_links_tags if
                            tag.get("href") and tag.get("href").startswith("http")}

            if not urls_on_page:
                relative_urls = {tag.get("href") for tag in job_links_tags if
                                 tag.get("href") and tag.get("href").startswith("/")}

                if relative_urls:
                    print("  -> Đã tìm thấy link tương đối. Đang thêm tên miền...")
                    urls_on_page = {"https://www.topcv.vn" + href for href in relative_urls}
                else:
                    print(f"Trang {page_num} không có link nào sau khi cuộn. Dừng.")
                    break

            all_unique_urls.update(urls_on_page)
            print(f"  -> Đã tìm thấy {len(urls_on_page)} URL. Tổng số URL duy nhất: {len(all_unique_urls)}")
            page_num += 1
            time.sleep(1)
            break
    except Exception as e:
        print(f"Lỗi trong quá trình thu thập URL: {e}")
    finally:
        if driver: driver.quit()

    print(f"--- KẾT THÚC GIAI ĐOẠN 1: Thu thập được {len(all_unique_urls)} URL duy nhất ---")
    return list(all_unique_urls)


def main():
    base_url_template = "https://www.topcv.vn/viec-lam-it?page={page_num}"
    output_filename = "careerviet_jobs.jsonl"

    if os.path.exists(output_filename):
        os.remove(output_filename)
        print(f"Đã xóa file cũ: {output_filename}")

    # Giai đoạn 1: Lấy tất cả URL (sử dụng phiên bản test 1 trang nếu cần)
    # all_job_urls = get_all_job_urls(base_url_template, max_pages=1) # Dùng để test
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
                driver.get("https://www.topcv.vn/")
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
                              "//h2[@class='job-detail__information-detail--title' and contains(normalize-space(.), 'Chi tiết tin tuyển dụng')]")

            WebDriverWait(driver, 10).until(
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
    # Nhớ định nghĩa các hàm khác (setup_driver, extract_job_details, get_all_job_urls) ở trên
    main()