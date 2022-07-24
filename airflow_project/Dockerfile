FROM apache/airflow:2.3.3
WORKDIR /airflow
COPY requirements.common.txt .
RUN su airflow
RUN pip install -r requirements.txt