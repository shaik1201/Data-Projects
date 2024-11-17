FROM python:3.9-slim

WORKDIR /app

COPY producer.py /app

RUN pip install --no-cache-dir requests confluent-kafka

CMD ["python", "/app/producer.py"]