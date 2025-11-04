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


def extract_job_details(html_content, job_url):
    soup = BeautifulSoup(html_content, "html.parser")
    job_details = {
        "url": job_url, "ten_cong_ty": "N/A", "tieu_de": "N/A", "ngay_dang_tuyen": "N/A",
        "luong": "N/A", "kinh_nghiem": "N/A", "cap_bac": "N/A", "nganh_nghe": [],
        "phuc_loi": [], "mo_ta_cong_viec": "", "yeu_cau_cong_viec": "",
        "dia_diem_lam_viec": {}, "thong_tin_khac": {}
    }

    # 1. Trích xuất Tiêu đề và Tên công ty
    job_header = soup.find("div", class_="job-desc")
    if job_header:
        title_tag = job_header.find("h1", class_="title")
        if title_tag: job_details["tieu_de"] = title_tag.get_text(strip=True)

        company_tag = job_header.find("a", class_="job-company-name")
        if company_tag: job_details["ten_cong_ty"] = company_tag.get_text(strip=True)

    # 2. Trích xuất các thông tin chung (Lương, Kinh nghiệm, Ngày cập nhật, v.v.)
    all_list_items = soup.find_all("li")
    for item in all_list_items:
        label_tag = item.find("strong")
        value_tag = item.find("p")

        if label_tag and value_tag:
            label = label_tag.get_text(strip=True)
            value = value_tag.get_text(strip=True)

            if "Ngày cập nhật" in label:
                job_details["ngay_dang_tuyen"] = value
            elif "Lương" in label:
                # Gán lương vào thong_tin_khac để PySpark xử lý tập trung
                job_details["thong_tin_khac"]["lương"] = item.find("p").get_text(separator=" ", strip=True)
            elif "Kinh nghiệm" in label:
                job_details["kinh_nghiem"] = value.replace("\n", " ").strip()
            elif "Cấp bậc" in label:
                job_details["cap_bac"] = value
            elif "Ngành nghề" in label:
                industry_tags = value_tag.find_all("a")
                if industry_tags:
                    job_details["nganh_nghe"] = [tag.get_text(strip=True) for tag in industry_tags]

    # Ghi đè lương từ box màu xanh nếu có (ưu tiên hơn)
    blue_box_salary_tag = soup.select_one("div.bg-blue strong:-soup-contains('Lương') + p")
    if blue_box_salary_tag:
        job_details["luong"] = blue_box_salary_tag.get_text(strip=True)

    # 3. Trích xuất Mô tả, Yêu cầu, Địa điểm
    detail_rows = soup.find_all("div", class_="detail-row")
    for row in detail_rows:
        title_tag = row.find(["h2", "h3"], class_="detail-title")
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            # Tìm thẻ div nội dung ngay sau thẻ tiêu đề
            content_div = title_tag.find_next_sibling("div")

            if content_div:
                if "Mô tả Công việc" in title_text:
                    job_details["mo_ta_cong_viec"] = content_div.get_text(separator="\n", strip=True)
                elif "Yêu Cầu Công Việc" in title_text:
                    job_details["yeu_cau_cong_viec"] = content_div.get_text(separator="\n", strip=True)
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
                        for li_item in list_items:
                            parts = li_item.get_text(separator=":", strip=True).split(":", 1)
                            if len(parts) == 2:
                                key = parts[0].strip().replace(" ", "_").lower()
                                value = parts[1].strip()
                                job_details["thong_tin_khac"][key] = value

    # 4. Trích xuất Phúc lợi
    welfare_section = soup.find("h2", class_="detail-title", string=lambda text: text and "Phúc lợi" in text)
    if welfare_section:
        welfare_list_ul = welfare_section.find_next_sibling("ul", class_="welfare-list")
        if welfare_list_ul:
            welfare_items = welfare_list_ul.find_all("li")
            job_details["phuc_loi"] = [li.get_text(strip=True) for li in welfare_items]

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
    output_filename = "../careerviet_jobs.jsonl"

    if os.path.exists(output_filename):
        os.remove(output_filename)
        print(f"Đã xóa file cũ: {output_filename}")

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
                with open(output_filename, 'a', encoding='utf-8') as f:
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

    print(f"\nHoàn tất. Đã lưu tổng cộng {crawled_count} công việc vào file '{output_filename}'.")

if __name__ == "__main__":
    main()