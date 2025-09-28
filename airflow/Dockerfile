FROM apache/airflow:2.9.0

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.12.txt"

# root 권한으로 전환
USER root

# docker CLI 설치 + 호스트 docker 그룹(GID=113)에 airflow 유저 추가
RUN apt-get update && apt-get install -y docker.io && \
    groupmod -g 113 docker && usermod -aG docker airflow && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 다시 airflow 유저로 실행
USER airflow
