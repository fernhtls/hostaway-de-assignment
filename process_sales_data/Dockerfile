FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
COPY process_sales_data.py .
# Adding the sales data to image
# not good but just getting it as a seed in dbt for example
COPY generate_sales_data.csv . 
RUN apt update && apt install -y libpq-dev python3-dev gcc
RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "/app/process_sales_data.py" ]