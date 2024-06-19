FROM python:3.12

WORKDIR /st
COPY . .

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false
RUN poetry install --no-interaction