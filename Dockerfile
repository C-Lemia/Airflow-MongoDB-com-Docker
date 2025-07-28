FROM apache/airflow:2.8.1-python3.11

# Instalar pacotes 
COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Adicionar scripts ao PYTHONPATH
ENV PYTHONPATH="/opt/airflow/scripts:${PYTHONPATH}"
