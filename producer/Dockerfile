FROM python:3.9-slim

RUN useradd -m appuser

WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .


USER appuser

CMD ["python", "producer.py"]
