from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from webdriver_manager.chrome import ChromeDriverManager 
from minio_writer import MinioWriter

import datetime
import time
import os
import random
from bs4 import BeautifulSoup
import re
SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080
BATCH_SIZE = 25

def setup_driver():
    chrome_options = Options()
    # Thêm đường dẫn tới Chrome browser
    # chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"  # Đường dẫn mặc định của Chrome
    
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
    #chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def sign_in(driver, email, password):
    driver.get("https://itviec.com/sign_in")

    time.sleep(2)

    email_input = driver.find_element(By.NAME, "user[email]")
    password_input = driver.find_element(By.NAME, "user[password]")

    email_input.send_keys(email)
    password_input.send_keys(password)

    submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
    submit_button.click()

    time.sleep(2)
    print("Đăng nhập thành công!")
    
def crawl_links(driver):
    driver.get("https://itviec.com/it-jobs")
    time.sleep(random.uniform(2, 3))
    links = []
    while True:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        job_cards = soup.find_all('div', class_='job-card')
        for job_card in job_cards:
            # Tìm kiếm tất cả các link công việc trong chính thẻ đó
            job_url_attribute = 'data-search--job-selection-job-url-value'
            
            if job_url_attribute in job_card.attrs:
                relative_link = job_card.attrs[job_url_attribute]
                relative_link = relative_link.split('/content')[0] # loại bỏ phần dư thừa để truy cập vào đúng link 
                full_link = "https://itviec.com" + relative_link
                links.append(full_link)
            else:
                print("ERROR: Không tìm thấy thuộc tính link trong thẻ job-card.")
                
        print("------------------------------------")
        print('đang ở trang số:', len(links)//BATCH_SIZE)
        print("Đã thu thập được", len(links), "liên kết công việc.")
        try:
            next_page = soup.find('a', rel='next')
            if next_page and next_page['href']:
                driver.get("https://itviec.com" + next_page['href'])
                time.sleep(random.uniform(2, 3))
            else:
                print("Không tìm thấy trang tiếp theo. Kết thúc thu thập liên kết.")
                break
            
        except Exception as e:
            print("Đã hết trang hoặc có lỗi xảy ra:", e)
            break
        
    return links



    
def parse_detail_job(driver, link):
    driver.get(link)
    time.sleep(random.uniform(3, 5))
    content = driver.page_source
    
    soup = BeautifulSoup(content, 'html.parser')
    job_title = soup.find('div', class_='job-header-info').find('h1').get_text(strip=True)
    company_name = soup.find('div', class_='employer-name').get_text(strip=True)
    salary = soup.find('div', class_='salary').get_text(strip=True)
    
    
    
    job_show_info = soup.find('div', class_='job-show-info').find_all('span')
    job_show_info_list = [info.get_text().replace('\n',' ') for info in job_show_info]
    company_location  = job_show_info_list[0]
    woring_model = job_show_info_list[1]
    post_time = job_show_info_list[2]
    #skill and job_expertise
    skills, job_expertise = soup.find('div', class_='job-show-info').find_all('div', class_='imb-4 imb-xl-3 d-flex flex-column flex-xl-row igap-3 align-items-xl-baseline')
    skills_list = [skill.get_text(strip=True) for skill in skills.find_all('a')]
    job_expertise_list = [exp.get_text(strip=True) for exp in job_expertise.find_all('a')]
    
    #job_domain
    job_domain = soup.find_all('div', class_='itag bg-light-grey itag-sm cursor-default')
    job_domain = [domain.get_text(strip=True) for domain in job_domain]
    
    #job description, your_skill_and_experience, benefit
    job_description, your_skill_and_experience, benefit = soup.find_all('div', class_='imy-5 paragraph')
    job_description = job_description.get_text(strip=True, separator='\n')
    your_skill_and_experience = your_skill_and_experience.get_text(strip=True, separator='\n')
    benefit = benefit.get_text(strip=True, separator='\n')
    
    # company detail với pattern để chỉ chọn đúng class
    company_detail = {}
    pattern_1 = re.compile(r'\b' + re.escape('job-show-employer-info') + r'\b')
    job_show_employer_info = soup.find('section', class_=pattern_1)
    
    if job_show_employer_info:
        company_detail['company_name'] = job_show_employer_info.find('h3').get_text(strip=True)
        company_detail['declaration'] = job_show_employer_info.find('div', class_='imt-5 imt-xl-4').get_text(strip=True)
        
        pattern_2 = re.compile(r'\b' + re.escape('row ipy-2') + r'\b')
        info_items = job_show_employer_info.find_all('div', class_=pattern_2)
        for item in info_items:
            item = item.get_text(strip=True, separator='\n')
            key = item.split('\n')[0]
            value = item.split('\n')[1]
            company_detail[key] = value
    
    return {
        'job_title': job_title,
        'company_name': company_name,
        'salary': salary,
        'company_location': company_location,
        'woring_model': woring_model,
        'post_time': post_time,
        'skills_list': skills_list,
        'job_expertise': job_expertise_list,
        'job_domain': job_domain,
        'job_description': job_description,
        'your_skill_and_experience': your_skill_and_experience,
        'benefit': benefit,
        'company_detail': company_detail,
        
    }
    


# def crawl_first_page_links(driver):
#     """Một phiên bản sửa đổi của crawl_links, chỉ lấy link ở trang đầu tiên."""
#     driver.get("https://itviec.com/it-jobs")
#     #time.sleep(random.uniform(2, 3))
#     time.sleep(random.uniform(8, 10))
#     links = []
#     soup = BeautifulSoup(driver.page_source, 'html.parser')
#     #job_cards = soup.find_all('div', class_='job-card')
#     job_cards = soup.select('div.job-card')

#     if not job_cards:
#         print("CẢNH BÁO: Không tìm thấy job card nào bằng CSS selector 'div.job-card'. Có thể cấu trúc trang đã thay đổi hoàn toàn.")
    
#     for job_card in job_cards:
#         job_url_attribute = 'data-search--job-selection-job-url-value'
#         if job_url_attribute in job_card.attrs:
#             relative_link = job_card.attrs[job_url_attribute].split('/content')[0]
#             full_link = "https://itviec.com" + relative_link
#             links.append(full_link)
#     print(f"Đã thu thập được {len(links)} links từ trang đầu tiên.")
#     return links

def crawl_first_page_links(driver):
    """
    Một phiên bản nâng cấp, sử dụng WebDriverWait để đảm bảo các job card đã được tải xong.
    """
    print("Đang truy cập trang việc làm và chờ dữ liệu động...")
    driver.get("https://itviec.com/it-jobs")

    try:
        # ===== THAY ĐỔI QUAN TRỌNG NHẤT =====
        # Chờ tối đa 15 giây cho đến khi ÍT NHẤT MỘT thẻ div có class 'job-card' xuất hiện.
        # Ngay khi nó xuất hiện, code sẽ tiếp tục chạy mà không cần chờ hết 15 giây.
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.job-card"))
        )
        print("-> Các job card đã được tải thành công!")

    except Exception as e:
        # Nếu sau 15 giây mà vẫn không thấy, in ra lỗi và trả về danh sách rỗng.
        print(f"!!! Lỗi: Không tìm thấy job card nào sau 15 giây. Có thể trang đã thay đổi hoặc có lỗi tải trang. Lỗi: {e}")
        # Lưu lại HTML để debug xem tại sao
        with open("debug_itviec_timeout.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("!!! Đã lưu HTML lúc bị lỗi vào file 'debug_itviec_timeout.html'")
        return []

    # Bây giờ, khi chúng ta chắc chắn các job card đã ở đó, chúng ta mới lấy page_source
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    job_cards = soup.select('div.job-card')
    links = []
    for job_card in job_cards:
        job_url_attribute = 'data-search--job-selection-job-url-value'
        if job_url_attribute in job_card.attrs:
            relative_link = job_card.attrs[job_url_attribute].split('/content')[0]
            full_link = "https://itviec.com" + relative_link
            links.append(full_link)
    
    print(f"Đã thu thập được {len(links)} links từ trang đầu tiên.")
    return links

#def run_continuous_crawl(interval_seconds=15, email='dihidromonooxit01012000@gmail.com', password='1!Aaaaaaaaaa'): # 1800 giây = 30 phút
    """
    Chạy crawler ở chế độ streaming liên tục, định kỳ cào trang đầu tiên.
    """
    producer = JobProducer()
    if not producer.producer: return

    driver = None
    total_jobs_sent = 0

    try:
        driver = setup_driver()
        sign_in(driver, email, password)
        
        while True: # Vòng lặp vô hạn để chạy liên tục
            print(f"\n{'='*50}")
            print(f"Bắt đầu chu kỳ cào dữ liệu mới lúc: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            links = crawl_first_page_links(driver)
            session_job_count = 0
            for i, link in enumerate(links):
                try:
                    # TODO: Thêm logic để kiểm tra xem link này đã được cào trước đó chưa (dùng DB/cache)
                    # Nếu đã cào rồi thì `continue`
                    
                    detail = parse_detail_job(driver, link)
                    detail['source'] = 'itviec.com'
                    # Thêm timestamp thời điểm cào dữ liệu
                    detail['crawled_at'] = datetime.datetime.now().isoformat()
                    producer.send_job(detail)
                    session_job_count += 1
                except Exception as e:
                    print(f"Lỗi khi xử lý link {link}: {e}")
            
            total_jobs_sent += session_job_count
            print(f"Chu kỳ hoàn tất. Đã gửi {session_job_count} tin mới. Tổng cộng đã gửi: {total_jobs_sent}")
            print(f"Sẽ nghỉ trong {interval_seconds / 60:.1f} phút...")
            print(f"{'='*50}\n")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n\n Đã nhận tín hiệu ngắt (Ctrl+C). Đang dừng streaming...")
    finally:
        print("\nĐang dọn dẹp và thoát...")
        if driver: driver.quit()
        if producer: producer.close()

def run_continuous_crawl(interval_seconds=60, email='dihidromonooxit01012000@gmail.com', password='1!Aaaaaaaaaa'):
    """
    Chạy crawler, cào dữ liệu và LƯU VÀO MINIO ở lớp Bronze.
    """
    driver = None
    total_jobs_saved = 0

    try:
        driver = setup_driver()
        # Đăng nhập chỉ một lần ở đầu
        # sign_in(driver, "email", "password")

        while True:
            print(f"\n{'='*50}")
            print(f"Bắt đầu chu kỳ cào dữ liệu lúc: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            # Tạo writer mới ở mỗi chu kỳ để đảm bảo kết nối tốt
            writer = MinioWriter()
            if not writer.client:
                print("Không thể kết nối MinIO, sẽ thử lại ở chu kỳ sau.")
                time.sleep(interval_seconds)
                continue

            links = crawl_first_page_links(driver)
            session_job_count = 0
            for i, link in enumerate(links):
                try:
                    detail = parse_detail_job(driver, link)
                    detail['source'] = 'itviec.com'
                    detail['crawled_at'] = datetime.datetime.now().isoformat()
                    detail['url'] = link

                    # Lưu job vào MinIO
                    if writer.save_job_to_bronze(detail):
                        session_job_count += 1
                        print(f"[{i+1}/{len(links)}] Đã lưu job '{detail['job_title']}' vào MinIO.")

                except Exception as e:
                    print(f"Lỗi khi xử lý link {link}: {e}")

            total_jobs_saved += session_job_count
            print(f"Chu kỳ hoàn tất. Đã lưu {session_job_count} tin mới. Tổng cộng đã lưu: {total_jobs_saved}")
            print(f"Sẽ nghỉ trong {interval_seconds / 60:.1f} phút...")
            print(f"{'='*50}\n")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\n\n Đã nhận tín hiệu ngắt (Ctrl+C). Đang dừng streaming...")
    finally:
        print("\nĐang dọn dẹp và thoát...")
        if driver:
            driver.quit()
if __name__ == '__main__':
    #run_crawl()
    run_continuous_crawl()