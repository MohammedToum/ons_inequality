FROM apache/airflow:3.2.1-python3.12

USER airflow

COPY --chown=airflow:root airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root

COPY pyproject.toml README.md /opt/airflow/project/

COPY src /opt/airflow/project/src

RUN chown -R airflow:root /opt/airflow/project \
    && chmod -R u+rwX,g+rX /opt/airflow/project
USER airflow

RUN pip install --no-cache-dir -e /opt/airflow/project