from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from webdriver_manager.chrome import ChromeDriverManager 

import time
import os

from bs4 import BeautifulSoup

SCREEN_WIDTH = 1920
SCREEN_HEIGHT = 1080
BATCH_SIZE = 25

def setup_driver():
    chrome_options = Options()
    # Thêm đường dẫn tới Chrome browser
    chrome_options.binary_location = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"  # Đường dẫn mặc định của Chrome
    
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

    time.sleep(5)
    print("Đăng nhập thành công!")
    
    
def test_crawl(driver):
    driver.get("https://itviec.com/it-jobs")
    time.sleep(5)
    test = driver.page_source
    print("Truy cập trang chủ ITviec thành công!")
    print(test[:500])  # In ra 500 ký tự đầu tiên của trang để kiểm tra
    
driver =setup_driver()
sign_in(driver, "dihidromonooxit01012000@gmail.com", "1!Aaaaaaaaaa")
test_crawl(driver)