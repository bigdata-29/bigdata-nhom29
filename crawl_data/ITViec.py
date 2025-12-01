from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pickle
from webdriver_manager.chrome import ChromeDriverManager 

import time
import os
import random
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from typing import Optional



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
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


# load và save cookie
def save_cookies(driver, path):
    """Lưu cookies của session hiện tại vào file"""
    if not os.path.exists('cookies'):
        os.makedirs('cookies')
    with open(path, 'wb') as f:
        pickle.dump(driver.get_cookies(), f)
    print(f"Đã lưu cookies vào {path}")

def load_cookies(driver, path):
    """Load cookies từ file vào driver"""
    if not os.path.exists(path):
        print(f"Không tìm thấy file cookies: {path}")
        return False
    
    with open(path, 'rb') as f:
        cookies = pickle.load(f)
        for cookie in cookies:
            # Giả sử 'cookie' là một dictionary chứa thông tin cookie
            # if 'domain' in cookie:
            #     del cookie['domain']
            # if 'expiry' in cookie: # Khóa 'expiry' cũng thường gây lỗi
            #     del cookie['expiry']
            driver.add_cookie(cookie)
    print(f"Đã load cookies từ {path}")
    return True

def is_logged_in(driver):
    """Kiểm tra xem đã đăng nhập chưa"""
    try:
        # Đợi tối đa 3 giây để tìm element chỉ xuất hiện khi đã đăng nhập
        # Ví dụ: link "Sign Out" hoặc tên user
        driver.get("https://itviec.com")
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located((By.CLASS_NAME, "sign-in-user-avatar"))
        )
        return True
    except Exception:
        return False

def sign_in(driver, email, password):
    """
    Đăng nhập vào ITviec và lưu cookies để duy trì session
    Returns:
        bool: True nếu đăng nhập thành công, False nếu thất bại
    """
    cookies_path = os.path.join('cookies', 'itviec_cookies.pkl')
    
    # Thử load cookies trước
    if os.path.exists(cookies_path):
        driver.get("https://itviec.com")
        load_cookies(driver, cookies_path)
        driver.refresh()  # Refresh để áp dụng cookies
        
        if is_logged_in(driver):
            print("Đã đăng nhập thành công bằng cookies!")
            return True
        else:
            print("Cookie không có tác dụng gì")
    
    # Nếu không có cookies hoặc cookies hết hạn, đăng nhập bình thường
    print("Tiến hành đăng nhập...")
    driver.get("https://itviec.com/sign_in")
    time.sleep(2)

    try:
        email_input = driver.find_element(By.NAME, "user[email]")
        password_input = driver.find_element(By.NAME, "user[password]")

        email_input.send_keys(email)
        password_input.send_keys(password)

        submit_button = driver.find_element(By.XPATH, "//button[@class='ibtn ibtn-md ibtn-primary w-100']")
        submit_button.click()

        # Đợi cho đến khi đăng nhập thành công (tối đa 10 giây)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "sign-in-user-avatar"))
        )
        
        # Lưu cookies sau khi đăng nhập thành công
        save_cookies(driver, cookies_path)
        print("Đăng nhập thành công!")
        return True

    except Exception as e:
        print(f"Lỗi khi đăng nhập: {str(e)}")
        return False
    
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






def process_date_and_time_ago(html_content: str, time_ago_str: str) -> Optional[str]:
    """
    Trích xuất ngày 'Last updated' và tính toán ngày chuẩn dựa trên chuỗi 'X time ago'.
    Chỉ cần trả về ngày chuẩn (YYYY-MM-DD).
    """
    
    # 1. Trích xuất ngày 'Last updated' từ comment HTML
    # Regex tìm kiếm comment chứa ngày tháng
    date_comment_pattern = re.compile(r'Last updated:\s*(\W[\d]{4}-[\d]{2}-[\d]{2} [\d]{2}:[\d]{2}:[\d]{2})')
    
    # Tìm kiếm comment trong toàn bộ nội dung HTML
    match = date_comment_pattern.search(html_content)
    
    if not match:
        return None # Không tìm thấy ngày base
        
    base_date_str = match.group(1)
    base_date_str = base_date_str[1:] # loại bỏ dấu " ở đầu
    
    try:
        # Chuyển đổi ngày base thành đối tượng datetime
        base_datetime = datetime.strptime(base_date_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None # Lỗi định dạng ngày base

    # 2. Xử lý chuỗi 'X time ago'
    time_ago_str = time_ago_str.lower().strip()
    
    # Regex để trích xuất số và đơn vị (minute/hour/day)
    # Ví dụ: "post 1 day ago" -> (1, 'day')
    time_pattern = re.compile(r'(\d+)\s+(minute|hour|day)s?\s+ago')
    
    time_match = time_pattern.search(time_ago_str)
    
    if not time_match:
        # Nếu không khớp, giả định bài đăng quá cũ hoặc định dạng khác, trả về ngày base
        return base_datetime.date().strftime('%d-%m-%Y')
        
    amount = int(time_match.group(1))
    unit = time_match.group(2)
    
    # 3. Tính toán ngày chuẩn
    
    # Sử dụng timedelta để trừ thời gian
    if unit == 'minute':
        final_datetime = base_datetime - timedelta(minutes=amount)
    elif unit == 'hour':
        final_datetime = base_datetime - timedelta(hours=amount)
    elif unit == 'day':
        final_datetime = base_datetime - timedelta(days=amount)
    else:
        # Trường hợp không xử lý được unit, trả về ngày base
        return base_datetime.date().strftime('%d-%m-%Y')
        
    # Trả về ngày chuẩn dưới định dạng YYYY-MM-DD
    return final_datetime.date().strftime('%d-%m-%Y')

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
    woring_model = job_show_info_list[-2]
    post_time = job_show_info_list[-1]
    
    post_time=process_date_and_time_ago(content, post_time) 
    
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
    
def run_crawl(save_to_file=True, output_dir='.', email='dihidromonooxit01012000@gmail.com', password='1!Aaaaaaaaaa'):
    """Chạy crawler chính, trả về danh sách job và (tuỳ chọn) lưu ra file JSON có timestamp."""
    driver = setup_driver()
    # Đăng nhập (nếu cần) và thu thập links
    try:
        sign_in(driver, email, password)
    except Exception as e:
        # Nếu trang không yêu cầu đăng nhập hoặc đăng nhập thất bại, tiếp tục nhưng in cảnh báo
        print(f'Warning: không thể đăng nhập hoặc bỏ qua đăng nhập: {e}')
    links = crawl_links(driver)
    print(len(links), 'links đã được thu thập.')
    job_details = []
    for i,link in enumerate(links):
        try:
            detail = parse_detail_job(driver, link)
            job_details.append(detail)
            print("đang parse link thứ {i}:", link)
        except Exception as e:
            print(f'Lỗi khi parse {link} thứ {i}: {e}. Bỏ qua.')
            continue
    # Đóng driver khi xong
    try:
        driver.quit()
    except Exception:
        pass
    # Lưu file JSON nếu yêu cầu
    if save_to_file:
        import json, os, datetime
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(output_dir, f'itviec_jobs_{timestamp}.json')
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(job_details, f, ensure_ascii=False, indent=2)
            print(f'Đã lưu {len(job_details)} job vào {filename}')
        except Exception as e:
            print(f'Không thể lưu file JSON: {e}')
    return job_details
# Runner ví dụ: gọi run_crawl và lưu kết quả (file được ghi trong thư mục hiện tại).
if __name__ == '__main__':
    # Bạn có thể đổi output_dir thành đường dẫn lưu mong muốn
    jobs = run_crawl(save_to_file=True, output_dir='.')
    print('Hoàn tất. Số job thu thập được:', len(jobs))