FROM ubuntu:24.04

RUN apt-get update && apt-get upgrade -y && apt-get install -y wget python3-pip

# Install PDI
RUN echo "deb [ arch=amd64 ] https://repo.pdi.dev/ubuntu noble main" | tee /etc/apt/sources.list.d/pdi.list > /dev/null
RUN wget -O /etc/apt/trusted.gpg.d/pdidev-archive-keyring.gpg https://repo.pdi.dev/ubuntu/pdidev-archive-keyring.gpg
RUN chmod a+r /etc/apt/trusted.gpg.d/pdidev-archive-keyring.gpg /etc/apt/sources.list.d/pdi.list
RUN apt-get update && apt-get install -y pdidev-archive-keyring libpdi-dev pdiplugin-all 

# Install MPI
RUN apt-get install -y libopenmpi-dev

# Install ray
#RUN pip install --break-system-packages ray[client]==2.42.1

WORKDIR /venv/
COPY requirements.txt  /venv/

RUN pip install -r requirements.txt  --break-system-packages

# Disable Ray's data collection
RUN ray disable-usage-stats

# TODO delete (to debug network issues)
#RUN apt-get install -y iputils-ping

#docker run --rm -it --shm-size=2gb --network=host -v "$(pwd)":/workspace -w /workspace 'doreisa_simulation:latest' /bin/bash