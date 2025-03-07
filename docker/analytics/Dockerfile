FROM ubuntu:24.04

RUN apt-get update && apt-get upgrade -y && apt-get install -y python3-pip

WORKDIR /venv/
COPY requirements.txt  /venv/

RUN pip install -r requirements.txt --break-system-packages

# Disable Ray's data collection
RUN ray disable-usage-stats
