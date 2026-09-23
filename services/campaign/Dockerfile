FROM python:3.12-slim
WORKDIR /app
COPY services/campaign/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/campaign /app
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
