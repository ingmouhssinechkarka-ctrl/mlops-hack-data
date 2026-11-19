FROM python:3.12-slim
RUN pip install --no-cache-dir mlflow==3.18.0 psycopg2-binary boto3
COPY start.sh /start.sh
CMD ["sh", "/start.sh"]