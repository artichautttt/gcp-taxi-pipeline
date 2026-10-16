FROM apache/airflow:3.3.2-python3.11

# Pipeline dependencies, installed next to Airflow (versions from requirements.txt).
RUN pip install --no-cache-dir \
    requests==2.34.2 google-cloud-bigquery==3.45.2 python-dotenv==1.2.3 pyarrow==25.0.1

# dbt in its own virtualenv: avoids dependency conflicts with Airflow.
USER root
RUN mkdir /opt/dbt-venv && chown airflow: /opt/dbt-venv
USER airflow
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-bigquery==1.12.1
