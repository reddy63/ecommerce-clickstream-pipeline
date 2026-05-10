.PHONY: up down restart logs test lint dbt-run dbt-test governance-check clean

# ── Infrastructure ──────────────────────────────────────────────
up:
	docker compose -f docker-compose.infra.yml up -d
	docker compose -f docker-compose.app.yml up -d
	@echo "✅  All services started"

down:
	docker compose -f docker-compose.app.yml down
	docker compose -f docker-compose.infra.yml down

restart: down up

logs:
	docker compose -f docker-compose.app.yml logs -f --tail=100

# ── dbt ──────────────────────────────────────────────────────────
dbt-run:
	cd dbt_project && dbt run --profiles-dir .

dbt-test:
	cd dbt_project && dbt test --profiles-dir .

dbt-docs:
	cd dbt_project && dbt docs generate && dbt docs serve --profiles-dir .

# ── Quality & Linting ────────────────────────────────────────────
lint:
	black --check --diff .
	isort --check-only --diff .
	flake8 . --max-line-length=120 --exclude=venv

format:
	black .
	isort .

test:
	pytest tests/ -v --tb=short

# ── Data Governance ──────────────────────────────────────────────
governance-check:
	@echo "Validating schemas..."
	@python -c "import json,os; [json.load(open(f'data_governance/schemas/{f}')) for f in os.listdir('data_governance/schemas') if f.endswith('.json')]; print('Schemas OK')"
	@echo "Validating contracts..."
	@python -c "import yaml,os; [yaml.safe_load(open(f'data_governance/data_contracts/{f}')) for f in os.listdir('data_governance/data_contracts')]; print('Contracts OK')"

# ── Cleanup ──────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -rf dbt_project/target dbt_project/dbt_packages

help:
	@grep -E '^[a-zA-Z_-]+:' Makefile | awk -F: '{printf "  make %-18s\n", $$1}'
