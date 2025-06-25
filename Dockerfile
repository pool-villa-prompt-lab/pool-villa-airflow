FROM apache/airflow:3.0.2

USER root

RUN apt-get update && apt-get install -y wget gnupg ca-certificates \
    libatk-bridge2.0-0 libgtk-3-0 libxss1 libasound2 libnss3 libxcomposite1 \
    libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 libcups2 libx11-xcb1 \
    libx11-dev libxcb-dri3-0 libdrm2 libxext6 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps

