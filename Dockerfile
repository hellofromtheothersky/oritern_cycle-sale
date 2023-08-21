FROM apache/airflow:2.6.3
RUN pip install --user --upgrade pip
RUN pip install apache-airflow-providers-microsoft-mssql