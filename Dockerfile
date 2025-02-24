FROM ubuntu:24.04

RUN apt-get update && apt-get upgrade -y

RUN apt-get install -y wget

# Install PDI
RUN echo "deb [ arch=amd64 ] https://repo.pdi.dev/ubuntu noble main" | tee /etc/apt/sources.list.d/pdi.list > /dev/null
RUN wget -O /etc/apt/trusted.gpg.d/pdidev-archive-keyring.gpg https://repo.pdi.dev/ubuntu/pdidev-archive-keyring.gpg
RUN chmod a+r /etc/apt/trusted.gpg.d/pdidev-archive-keyring.gpg /etc/apt/sources.list.d/pdi.list
RUN apt-get update && apt-get install -y pdidev-archive-keyring libpdi-dev pdiplugin-all 

# Install pipx
#RUN apt-get install -y pipx && pipx ensurepath

# Install poetry with pipx
#RUN pipx install poetry

#COPY poetry.lock pyproject.yoml /

#WORKDIR /project/
#RUN poetry install --no-interaction --no-ansi

# Install MPI
RUN apt-get install -y libopenmpi-dev

WORKDIR /venv/
COPY requirements.txt  /venv/

RUN apt-get install -y python3-pip

RUN pip install -r requirements.txt  --break-system-packages

# Install Poetry and the Python project
#RUN apt-get install -y curl
#RUN curl -sSL https://install.python-poetry.org | python3 -

# TODO delete shell change?
#SHELL ["/bin/bash", "-c"]
#ENV PATH="$PATH:~/.local/bin"
#RUN poetry config virtualenvs.create false

#WORKDIR /venv
#COPY poetry.lock pyproject.toml /venv/


#RUN poetry install --no-interaction --no-ansi --no-root --break-system-packages
