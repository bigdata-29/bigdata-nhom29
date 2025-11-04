# Sử dụng image Airflow chính thức
FROM apache/airflow:3.0.2-python3.10

# Cài Chrome và ChromeDriver
USER root

RUN apt-get update && \
    apt-get install -y wget unzip curl gnupg && \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb && \
    # Lấy version Chrome đang cài
    CHROME_VERSION=$(google-chrome --version | grep -oE '[0-9]+(\.[0-9]+)+') && \
    # Lấy version chromedriver tương thích
    DRIVER_VERSION=$(curl -sS "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE") && \
    wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${DRIVER_VERSION}/linux64/chromedriver-linux64>
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip /usr/local/bin/chromedriver-linux64 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Cài thư viện Python cần thiết
USER airflow

RUN pip install --no-cache-dir \
    selenium \
    webdriver-manager \
    beautifulsoup4 \
    requests \
    pyspark \
    hdfs \
    apache-airflow-providers-cncf-kubernetes[common.compat]

# Copy DAG và mã crawler vào container
COPY dags/ /opt/airflow/dags/
WORKDIR /opt/airflow
