.PHONY: up down test bench explain logs

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=100

test:
	pip install -q pytest ./packages/common
	PYTHONPATH=services/indexer pytest
	cd services/serving && PROTO_FILE=../../proto/adpulse.proto cargo test

bench:
	python3 scripts/bench.py --n 200 --concurrency 50
	python3 scripts/bench.py --n 1000 --concurrency 100

explain:
	docker compose exec -T postgres psql -U adpulse -d adpulse -f - < sql/explain_queries.sql

chaos:
	bash scripts/chaos.sh

locust:
	locust -f loadtest/locustfile.py --host http://localhost:8000

k8s-apply:
	kubectl apply -k k8s
