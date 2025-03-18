FROM python:3.11-slim
ENV PYTHONIOENCODING utf-8

# install gcc and required libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    libssl-dev \
    openssl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install flake8

COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY /src /code/src/
COPY /tests /code/tests/
COPY /scripts /code/scripts/
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]
