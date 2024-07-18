FROM python:3.9

RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

RUN apt-get update -yqq &&\
    apt-get install sudo -yqq &&\
    sudo apt-get update -yqq && \
    sudo apt install -yqq default-jdk

RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \
    && apt-get clean

# Airflow setup
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__PARALLELISM=3
ENV AIRFLOW__CORE__DAG_CONCURRENCY=3
ENV AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=3
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=True
ENV AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL=60
ENV AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=150
ENV PYTHONPATH=/opt/airflow

#COPY ./dags $AIRFLOW_HOME/dags/
COPY requirements.txt ./
RUN pip3 install --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

RUN apt-get install vim -y

#COPY --chmod=+x start.sh /opt/airflowinit/start.sh
COPY start.sh /opt/airflowinit/start.sh
RUN chmod +x /opt/airflowinit/start.sh
#RUN sed -i $'s/\r$//' /opt/airflowinit/start.sh
RUN sed -i 's/\r$//' /opt/airflowinit/start.sh
EXPOSE 8080

CMD ["/usr/bin/supervisord"]