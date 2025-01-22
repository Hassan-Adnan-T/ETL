FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
VOLUME /app/secrets
COPY . .

CMD ["python", "etl_script.py"]