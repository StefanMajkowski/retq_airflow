FROM apache/airflow:2.5.1

COPY ./requirements.txt ./requirements.txt

RUN python3 -m pip install --upgrade pip
RUN pip install -r requirements.txt