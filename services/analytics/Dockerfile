FROM python:3.12-slim
WORKDIR /app
COPY services/analytics/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/analytics /app
ENV PYTHONUNBUFFERED=1
CMD ["python", "server.py"]
