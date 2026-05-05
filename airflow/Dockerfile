FROM apache/airflow:3.2.1-python3.12

COPY airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

COPY pyproject.toml README.md /opt/airflow/project/
COPY src /opt/airflow/project/src

RUN pip install --no-cache-dir -e /opt/airflow/project