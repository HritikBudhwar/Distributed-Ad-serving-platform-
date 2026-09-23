FROM python:3.12-slim
WORKDIR /app
COPY services/privacy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto /proto
COPY services/privacy /app
RUN python -m grpc_tools.protoc -I/proto --python_out=/app --grpc_python_out=/app /proto/adpulse.proto
ENV PYTHONUNBUFFERED=1
CMD ["python", "server.py"]
