FROM python:3.11-slim

COPY . /dbt

RUN apt update && apt install -y libpq-dev python3-dev gcc
RUN pip install -r /dbt/requirements.txt

WORKDIR /dbt/process_sales_data

ENTRYPOINT [ "dbt" ]