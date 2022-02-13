FROM python:3.8.12-bullseye

RUN pip install "apache-airflow[postgres,google]==2.2.3"
RUN pip install apache-airflow-providers-google==6.3.0

EXPOSE 8080

CMD ["sh", "-c", "airflow webserver & airflow scheduler"]