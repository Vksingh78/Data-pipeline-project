FROM apache/airflow:2.8.1
USER root
RUN apt-get update && apt-get install -y default-jre-headless && apt-get clean
USER airflow
RUN pip install pyspark