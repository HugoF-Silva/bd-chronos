FROM python:3.11-slim

WORKDIR /app

COPY app/ .

RUN pip install --upgrade pip
RUN pip install -r ./requirements.txt

EXPOSE 8080

CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "main:app", "-b", "0.0.0.0:8080", "--workers", "4"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
