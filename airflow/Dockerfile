FROM apache/airflow:2.3.3

ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

WORKDIR ${AIRFLOW_HOME}

COPY project ./project
COPY .airflowignore requirements.common.txt ./

#CMD ["ln", "-s", "/toss/project", "/opt/airflow/dags"]
#CMD ["ln", "-s", "/toss/.airflowignore", "/opt/airflow/dags"]

RUN pip install -r requirements.common.txt