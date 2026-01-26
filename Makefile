SHELL := /bin/bash
PROJECT_NAME := analytics_platform

.PHONY: up down logs fmt lint test dbt-run dbt-test ge-check airflow-init airflow-trigger

up:
	docker compose -f infra/docker-compose.yml up -d --build

down:
	docker compose -f infra/docker-compose.yml down

logs:
	docker compose -f infra/docker-compose.yml logs -f

fmt:
	python -m ruff format src

lint:
	python -m ruff check src

init:
	bash scripts/bootstrap.sh

dbt-run:
	dbt run --project-dir dbt

dbt-test:
	dbt test --project-dir dbt

ge-check:
	great_expectations checkpoint run analytics_checkpoint --config great_expectations/great_expectations.yml

airflow-init:
	docker compose -f infra/docker-compose.yml run --rm airflow-webserver airflow db init
	docker compose -f infra/docker-compose.yml run --rm airflow-webserver airflow users create \
		--username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin

airflow-trigger:
	docker compose -f infra/docker-compose.yml run --rm airflow-webserver airflow dags trigger analytics_eng_e2e

test:
	python -m pytest
