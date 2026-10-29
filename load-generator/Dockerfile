FROM python:3.12.12-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
COPY session.py kafka_probe.py resource_probe.py .
