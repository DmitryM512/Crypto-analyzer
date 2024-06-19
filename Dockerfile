FROM python:3.12

WORKDIR /signals
COPY application/poetry.lock .
COPY application/pyproject.toml .
COPY application ./application
COPY cli.py .

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false
RUN poetry install --no-interaction

EXPOSE 80








