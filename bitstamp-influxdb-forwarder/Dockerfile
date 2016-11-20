FROM python:2.7.12-alpine

WORKDIR /bitstamp-influxdb-forwarder

COPY requirements.txt ./
COPY bitstamp-influxdb-forwarder.py ./

RUN pip install -r requirements.txt

CMD python bitstamp-influxdb-forwarder.py
