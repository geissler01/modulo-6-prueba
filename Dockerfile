# IMAGEN
FROM apache/airflow:3.2.0-python3.12

# user para instalar dependencias del sistema (c/c++)
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ENV PATH="/"

ARG EXTRA_REQUIREMENTS=""

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 7. DIRECTORIO DE TRABAJO
WORKDIR /opt/airflow