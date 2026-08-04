.PHONY: up down logs test lint dbt-parse dag-test fmt

up:            ## start the local stack
	docker compose --env-file .env -f docker/docker-compose.yml up -d

down:          ## stop it and remove volumes
	docker compose --env-file .env -f docker/docker-compose.yml down -v
			   ## stop it and keep volumes (retains history in airflow)
	docker compose --env-file .env -f docker/docker-compose.yml down

logs:
	docker compose -f docker/docker-compose.yml logs -f

test: dag-test dbt-parse   ## everything CI runs

dag-test:      ## DAGs import cleanly and follow house rules
	cd airflow && python -m pytest tests/ -v

dbt-parse:     ## dbt project compiles without a warehouse connection
	cd dbt && dbt deps && dbt parse --profiles-dir ../ci

lint:
	ruff check airflow/
	sqlfluff lint dbt/models databricks/ddl

fmt:
	ruff format airflow/
	sqlfluff fix dbt/models