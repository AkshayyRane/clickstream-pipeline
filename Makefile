.PHONY: up down logs topic-create topic-list venv-simulator venv-consumer producer consumer test

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# Explicit topic creation with 3 partitions (auto-create would give us 1).
# More partitions = more parallel consumers later, and it's a talking point
# ("how would you scale this consumer?").
topic-create:
	docker compose exec redpanda rpk topic create clickstream-events -p 3

topic-list:
	docker compose exec redpanda rpk topic list

venv-simulator:
	python3 -m venv simulator/.venv
	simulator/.venv/bin/pip install -r simulator/requirements.txt

venv-consumer:
	python3 -m venv consumer/.venv
	consumer/.venv/bin/pip install -r consumer/requirements.txt

producer:
	simulator/.venv/bin/python -m simulator.producer

consumer:
	consumer/.venv/bin/python -m consumer.kafka_to_bronze

test:
	simulator/.venv/bin/python -m pytest tests/ -v
