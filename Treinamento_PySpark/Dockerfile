FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install pyspark==3.3.1 google-cloud-bigquery
CMD ["python", "main.py"]