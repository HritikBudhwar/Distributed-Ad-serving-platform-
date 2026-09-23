FROM python:3.12-slim
WORKDIR /app
COPY services/indexer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY proto /proto
COPY packages/common /packages/common
RUN pip install --no-cache-dir /packages/common
COPY services/indexer /app
RUN python -m grpc_tools.protoc -I/proto --python_out=/app --grpc_python_out=/app /proto/adpulse.proto
ENV PYTHONUNBUFFERED=1
CMD ["python", "server.py"]
