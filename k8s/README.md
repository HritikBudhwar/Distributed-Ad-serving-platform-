# Use: kubectl apply -k k8s after `docker compose build` and tagging images as adpulse/<svc>:local
# Postgres in infra.yaml does not auto-load sql/; apply compose locally or exec psql with sql/*.sql
# Streaming is Apache Kafka (apache/kafka:3.7.1) plus kafka-exporter for consumer lag.
