# This file is created at startup after the profiling process is created. It's needed to
# run the pyspy profiler against.
PROFILER_PID_FILE:=/tmp/profiling_pid
src := $(shell find src/ -name "*.py" -type f)

.PHONY: server
.PHONY: lint format format-fix test setup version_metadata help requirements default help

default:help

server: ## Run the dev server
	cd src && python -m ai.whylabs.container.startup

pyspy: ## Run profiler on the dev server
	sudo env "PATH=$(PATH)" py-spy record -o profile.svg --pid $(shell cat /tmp/profiling_pid)

docker: requirements.txt ## Build the docker container
	docker build . -t whylabs/whylogs:python-latest

docker-push: ## Push the docker container to docker hub
	docker push whylabs/whylogs:python-latest

load-test-500:
	./hey -z 10s -n 1000 -c 4 -m POST -D data/data-500.csv 'http://localhost:8000/pipe'

benchmark:
	./hey -z 10s -n 1000 -c 4 -m POST -D data/short-data.csv -T 'application/json' 'http://localhost:8000/mp'

requirements: requirements.txt

requirements.txt: pyproject.toml
	poetry export -f requirements.txt > requirements.txt

lint: ## Check for type issues with mypy
	poetry run mypy src/

format: ## Check for formatting issues
	poetry run black --check --line-length 120 src
	autoflake --check --in-place --remove-unused-variables $(src)

format-fix: ## Fix formatting issues
	poetry run black --line-length 120 src
	autoflake --in-place --remove-unused-variables $(src)

setup: ## Install dependencies with poetry
	poetry install

test: ## Run unit tests
	pytest

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
