# Imagem base do Airflow com Python 3.11
FROM apache/airflow:2.9.3-python3.11

USER root

# Instala Java 17 (para PySpark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN mkdir -p /opt/airflow/data /opt/airflow/logs \
    && chown -R airflow:root /opt/airflow/data /opt/airflow/logs

USER airflow

# Copia e instala requirements adicionais
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copia o código e DAGs
COPY src /opt/airflow/src
COPY airflow/dags /opt/airflow/dags

# Define variáveis de ambiente úteis
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow
ENV SPARK_LOCAL_IP=127.0.0.1

# Copia Makefile customizado
COPY Makefile /opt/airflow/Makefile
COPY .env /opt/airflow/.env

EXPOSE 8080

RUN make init-db && \
    make create-user && \
    make upgrade-db

CMD ["airflow", "standalone"]