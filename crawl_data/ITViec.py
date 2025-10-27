from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from webdriver_manager.chrome import ChromeDriverManager 

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
    chrome_options.add_argument("--headless")
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