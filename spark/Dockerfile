FROM bitnami/spark:3.5.0

# 작업 디렉토리
WORKDIR /app

# Python 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt