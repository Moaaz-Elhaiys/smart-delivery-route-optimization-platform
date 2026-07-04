# Makefile
.PHONY: setup test lint run-airflow run-dashboard

setup:
	uv sync
	cp .env.example .env

test:
	uv run pytest tests/ -v --tb=short

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff format .
	uv run ruff check --fix .

run-airflow:
	-@pids=$$(lsof -t -i:8793); if [ -n "$$pids" ]; then kill -9 $$pids; fi
	-@pids=$$(lsof -t -i:8080); if [ -n "$$pids" ]; then kill -9 $$pids; fi
	export AIRFLOW_HOME="$(PWD)/airflow" && \
	export PYTHONPATH="$(PWD):$$PYTHONPATH" && \
	uv run airflow webserver --port 8080 & \
	uv run airflow scheduler

run-dashboard:
	uv run streamlit run dashboard/app.py