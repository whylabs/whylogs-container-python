.PHONY: server
.PHONY: lint format format-fix test setup version_metadata help requirements default help

default:help

server: ## Run the dev server
	cd src && python -m ai.whylabs.container.startup

pyspy: ## Run profiler on the dev server
	# No good way of getting the pid of the profiling processs automatically, need to look it up and insert below 
	sudo env "PATH=$PATH" py-spy record -o profile.svg --pid $(PROFILING_PID)

docker:
	docker build . -t whylabs/whylogs:python-latest

docker-push:
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

format-fix: ## Fix formatting issues
	poetry run black --line-length 120 src

setup:
	poetry install

test: ## Run unit tests
	pytest

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
