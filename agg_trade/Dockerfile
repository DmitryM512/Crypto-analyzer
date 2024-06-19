FROM python:3.12

WORKDIR /agg_trade
COPY . .

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false
RUN poetry install --no-interaction