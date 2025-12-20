# Dockerfile
FROM bitnami/spark:3.5

USER root

# Copy requirements và cài đặt
COPY requirements_streaming.txt /app/requirements_streaming.txt
RUN pip install --no-cache-dir -r /app/requirements_streaming.txt

# Copy source code
COPY src /app/src

# Set working directory
WORKDIR /app

USER 1001