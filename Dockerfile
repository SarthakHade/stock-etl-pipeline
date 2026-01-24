FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --default-timeout=120 -r requirements.txt

COPY . .

CMD ["python", "ETL/main.py"]
