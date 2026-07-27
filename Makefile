.PHONY: up down logs topic-create topic-list venv-simulator venv-consumer venv-batch producer consumer test test-simulator test-batch airflow-up airflow-down airflow-logs batch-download batch-transform batch-quality-check venv-dbt dbt-deps dbt-run dbt-test dbt-build dbt-docs venv-dashboard dashboard venv-dev lint

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

venv-batch:
	python3 -m venv batch_source/.venv
	batch_source/.venv/bin/pip install -r batch_source/requirements.txt

producer:
	simulator/.venv/bin/python -m simulator.producer

consumer:
	consumer/.venv/bin/python -m consumer.kafka_to_bronze

# Host-side runs of the same scripts the Airflow DAG calls -- handy for
# debugging the batch leg without going through the Airflow UI.
batch-download:
	batch_source/.venv/bin/python -m batch_source.download_dataset

batch-transform:
	batch_source/.venv/bin/python -m batch_source.transform_to_bronze

batch-quality-check:
	batch_source/.venv/bin/python -m batch_source.quality_checks

# Airflow (webserver + scheduler + Postgres) runs under the "airflow" Compose
# profile, separate from `make up`, since the streaming and batch/orchestration
# legs are independent until they converge in dbt.
airflow-up:
	docker compose --profile airflow up -d

airflow-down:
	docker compose --profile airflow down

airflow-logs:
	docker compose --profile airflow logs -f

test-simulator:
	simulator/.venv/bin/python -m pytest tests/test_event_generator.py -v

test-batch:
	batch_source/.venv/bin/python -m pytest tests/test_quality_checks.py tests/test_transform_to_bronze.py -v

test: test-simulator test-batch

venv-dbt:
	python3 -m venv warehouse/.venv
	warehouse/.venv/bin/pip install -r warehouse/requirements.txt

dbt-deps:
	warehouse/.venv/bin/dbt deps --project-dir warehouse

dbt-run:
	warehouse/.venv/bin/dbt run --project-dir warehouse --profiles-dir warehouse

# Not folded into `make test` -- dbt tests need real bronze data on disk
# (Phases 1-2), unlike the pure-unit test-simulator/test-batch suites.
dbt-test:
	warehouse/.venv/bin/dbt test --project-dir warehouse --profiles-dir warehouse

dbt-build:
	warehouse/.venv/bin/dbt build --project-dir warehouse --profiles-dir warehouse

dbt-docs:
	warehouse/.venv/bin/dbt docs generate --project-dir warehouse --profiles-dir warehouse
	warehouse/.venv/bin/dbt docs serve --project-dir warehouse --profiles-dir warehouse

venv-dashboard:
	python3 -m venv dashboard/.venv
	dashboard/.venv/bin/pip install -r dashboard/requirements.txt

# Reads warehouse/clickstream.duckdb read-only -- run `make dbt-build` at
# least once first so there's a warehouse to read.
dashboard:
	dashboard/.venv/bin/streamlit run dashboard/app.py

venv-dev:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements-dev.txt

lint:
	.venv/bin/ruff check .
