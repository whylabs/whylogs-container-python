# This file is created at startup after the profiling process is created. It's needed to
# run the pyspy profiler against.
PROFILER_PID_FILE:=/tmp/profiling_pid
project_name := whylogs_container
schema_file = docs/_build/html/openapi.json
src := $(shell find $(project_name)/ -name "*.py" -type f)
it := -it
env_file := ./envs/personal.env
GIT_SHA := $(shell git rev-parse  HEAD)
examples := $(shell find example_repo/examples/ -mindepth 1 -maxdepth 1 -type d)

version := 3.0.0

image_name := whylogs-container:$(version)
image_name_latest := whylogs-container:latest

llm_image_name := langkit-container:$(version)
llm_image_name_latest := langkit-container:latest

policy_validator_name := policy-validator:$(version)
policy_validator_name_latest := policy-validator:latest

.PHONY: server server-llm docker docker-llm docker-push docker-llm-push integ python-client pip-install-python-client
.PHONY: lint format format-fix test setup version_metadata help default help docs clean all
.PHONY: bump-patch bump-minor bump-major bump-release bump-build update-example-locks update-example-client-versions update-example-client-pypi
.PHONY: test-examples fix dist openapi_def yaml-policy-schema policy-editor publish-policy-editor-s3 check-policy-editor-s3 policy-format policy-format-fix
.PHONY: policy-editor-standalone publish-policy-editor-s3-prod publish-policy-editor-s3-dev check-policy-editor-s3-prod check-policy-editor-s3-dev
.PHONY: policy-validator

default:help

server: ## Run the dev server
	LLM_CONTAINER=$(LLM_CONTAINER) poetry run bash -c 'export $$(cat $(env_file) | grep -v ^# | xargs); python -m $(project_name).whylabs.container.startup'

server-llm: LLM_CONTAINER=True
server-llm: server ## Run the dev server for the llm container mode

policy-validator: ## Run the policy validator service
	poetry run python -m policy_validator.startup

debug: ## Run the dev server in debug mode
	poetry run bash -c 'export $$(cat $(env_file) | grep -v ^# | xargs); python -m debugpy --listen localhost:5678 -m $(project_name).whylabs.container.startup'

pyspy: ## Run profiler on the dev server
	sudo env "PATH=$(PATH)" py-spy record -o profile.svg --pid $(shell cat /tmp/profiling_pid)

dist: ## Build the distribution
	poetry build
	
docker: ## Build the docker container
	docker build -f Dockerfile.main . -t $(image_name_latest)
	docker tag $(image_name_latest) $(image_name)

docker-llm: ./whylabs_asset_cache ## Build the docker container
	@echo "WARNING you may need a lot of memory to build this container"
	docker build -f Dockerfile.llm --secret id=openai_api_key,env=OPENAI_API_KEY . -t $(llm_image_name_latest) $(extra_args)
	docker tag $(llm_image_name_latest) $(llm_image_name)

./whylabs_asset_cache:
	tar -xzf whylabs_asset_cache.tar.gz

docker-llm-no-cache: extra_args=--no-cache
docker-llm-no-cache: docker-llm ## Build the docker container with --no-cache in docker

docker-policy-validator: ## Build the docker container
	docker build -f Dockerfile.policy_validator --secret id=whylabs_api_key,env=WHYLABS_API_KEY --secret id=pypi_api_key,env=PYPI_API_KEY . -t $(policy_validator_name_latest)
	docker tag $(policy_validator_name_latest) $(policy_validator_name)

asset-cache: ## Download all assets that the container uses locally
	poetry run python -m whylogs_container.whylabs.container.metrics.library

docker-llm-run:  ## Run the docker container
	docker run $(it) --rm -p 127.0.0.1:8000:8000 --env-file $(env_file) $(llm_image_name)

docker-run:
	docker run $(it) --rm -p 127.0.0.1:8000:8000 --env-file $(env_file) $(image_name)

docker-policy-validator-run: ## Run the policy validator container
	docker run $(it) --rm -p 127.0.0.1:8000:8000 $(policy_validator_name)

docs:  ## Build the documentation
	LLM_CONTAINER=1 WHYLABS_API_KEY=1 WHYLABS_ORG_ID=1 static_secret=1 disable_auth=True poetry run bash -c 'cd docs && make clean html'
	touch docs/_build/html/.nojekyll

policy_editor_dir := ./policy-editor
policy_editor_files := $(shell find policy-editor/ -type f -not -path "*/node_modules/*" -not -path "*/dist/*")

$(policy_editor_dir)/node_modules:
	cd $(policy_editor_dir) && npm install

$(policy_editor_dir)/dist/production: $(policy_editor_dir)/node_modules $(policy_editor_files)
	cd $(policy_editor_dir) && env VERSION=$(version) make build

policy-editor: $(policy_editor_dir)/dist/production ## Build the policy editor typescript project

$(policy_editor_dir)/dist/standalone/policy:
	poetry run python -m whylogs_container.whylabs.container.policy.__init__ > $(policy_editor_dir)/dist/standalone/policy

$(policy_editor_dir)/dist/standalone: $(policy_editor_dir)/node_modules $(policy_editor_files) $(policy_editor_dir)/dist/standalone/policy
	cd $(policy_editor_dir) && env VERSION=$(version) make build-standalone

policy-editor-standalone: $(policy_editor_dir)/dist/standalone ## Build the standalone s3 version of the policy editor

publish-policy-editor-s3-prod: policy_bucket=s3://secure-policy-editor.whylabsapp.com
publish-policy-editor-s3-prod: publish-policy-editor-s3 ## Publish the policy editor to s3 prod

publish-policy-editor-s3-dev: policy_bucket=s3://secure-policy-editor.development.whylabsdev.com
publish-policy-editor-s3-dev: publish-policy-editor-s3 ## Publish the policy editor to s3 dev

publish-policy-editor-s3: ## Publish the policy editor to s3 dev
	cd $(policy_editor_dir) && aws s3 sync ./dist/standalone $(policy_bucket)/$(version) --delete

check-policy-editor-s3-prod: policy_bucket=secure-policy-editor.whylabsapp.com
check-policy-editor-s3-prod: check-policy-editor-s3 ## Use curl to confirm that the policy editor exists

check-policy-editor-s3-dev: policy_bucket=secure-policy-editor.development.whylabsdev.com
check-policy-editor-s3-dev: check-policy-editor-s3 ## Use curl to confirm that the policy editor exists

check-policy-editor-s3: ## Use curl to confirm that the policy editor exists
	curl --fail -s -o /dev/null -w "%{http_code}" https://$(policy_bucket)/$(version)/index.html

policy-format:  ## Check the policy editor code formatting issues
	cd $(policy_editor_dir) && make format

policy-format-fix:  ## Fix policy editor code formatting issues
	cd $(policy_editor_dir) && make format-fix

clean:
	rm -rf ./dist whylogs-container-client/dist/
	cd ./policy-editor && make clean

openapi_def: $(schema_file)  ## Generate the openapi schema

$(schema_file): $(src)
	@$(call i, Generating the openapi schema)
	mkdir -p $(shell dirname $(schema_file))
	LLM_CONTAINER=1 WHYLABS_API_KEY=f static_secret=f s3_profile_upload_bucket=some-bucket \
				  poetry run python -m whylogs_container.whylabs.container.startup --mode openapi-gen -f $(schema_file)

python-client: $(schema_file) policy-editor  ## Update the generated python client
	poetry run openapi-python-client generate --overwrite \
		--path $(schema_file) \
		--fail-on-warning \
		--config ./openapi-generator.yaml
	@# Just remove the existing dist each time to make it easier to setup locally.
	cd ./whylogs-container-client && rm -rf ./dist && poetry install && poetry build && ls -al ./dist
	# Don't allow updates to the README from the auto generation logic
	git checkout whylogs-container-client/README.md whylogs-container-client/README.md.sub whylogs-container-client/pyproject.toml

yaml-policy-schema:  ## Generate the JSON schema that represents our yaml policy
	@poetry run python -m whylogs_container.whylabs.container.policy.__init__

pip-install-python-client:
	poetry run pip uninstall whylogscontainer-client -y || true
	poetry run pip install ./whylogs-container-client/dist/*.whl

load-test-500:
	./hey -z 10s -n 1000 -c 4 -m POST -D data/data-500.csv 'http://localhost:8000/pipe'

benchmark:
	./hey -z 10s -n 1000 -c 4 -m POST -D data/short-data.csv -T 'application/json' 'http://localhost:8000/mp'

metric-benchmark: ## Run the metric benchmark to get a latency report for the different metrics
	poetry run python -m scripts.benchmarking.metric_benchmark

bump-patch: ## Bump the patch version (_._.X) everywhere it appears in the project
	@$(call i, Bumping the patch number)
	poetry run bumpversion patch --allow-dirty

bump-minor: ## Bump the minor version (_.X._) everywhere it appears in the project
	@$(call i, Bumping the minor number)
	poetry run bumpversion minor --allow-dirty

bump-major: ## Bump the major version (X._._) everywhere it appears in the project
	@$(call i, Bumping the major number)
	poetry run bumpversion major --allow-dirty

bump-release: ## Convert the version into a release variant (_._._) everywhere it appears in the project
	@$(call i, Removing the dev build suffix)
	poetry run bumpversion release --allow-dirty

bump-build: ## Bump the build number (_._._-____XX) everywhere it appears in the project
	@$(call i, Bumping the build number)
	poetry run bumpversion build --allow-dirty

set-version: ## Set the version to a specific value. Usage: make set-version VERSION=X.Y.Z
	@$(call i, Setting version to $(VERSION))
	poetry run bumpversion --new-version $(VERSION) --allow-dirty patch

update-example-locks:  ## Update each of the example repo lock files
	@$(foreach example,$(examples),cd $(example) && poetry lock && cd ../../..;)

update-example-client-versions: python-client  ## Update each of the example repos with the latest local client version and upate lock files
	@$(foreach example,$(examples),cd $(example) && poetry add --group dev ../../../whylogs-container-client/dist/whylogs_container_client-$(version)-py3-none-any.whl && poetry lock --no-update && cd ../../..;)

update-example-pre-release:  ## Update each of the example repos to use the current python package versions and update lock files. For use before a release.
	@$(foreach example,$(examples),cd $(example) && make pre-release && rm -rf .venv && cd ../../..;)

lint: ## Check for type issues with pyright
	@{ echo "Running pyright\n"; poetry run pyright; PYRIGHT_EXIT_CODE=$$?; } ; \
	{ echo "\nRunning ruff check\n"; poetry run ruff check; RUFF_EXIT_CODE=$$?; } ; \
	{ echo "\nRunning docsub check\n"; poetry run python -m scripts.docsub --check; DOCSUB_EXIT_CODE=$$?; } ; \
	{ echo "\nRunning ban checks\n"; ./scripts/bans.sh; BANS_EXIT_CODE=$$?; } ; \
	exit $$(($$PYRIGHT_EXIT_CODE + $$RUFF_EXIT_CODE + $$DOCSUB_EXIT_CODE + $$BANS_EXIT_CODE))


lint-fix:
	poetry run ruff check --fix

format: ## Check for formatting issues
	poetry run ruff format --check

format-fix: ## Fix formatting issues
	poetry run ruff format

docsub:  ## Update the documentation snippets
	poetry run python -m scripts.docsub

docsub-check:  ## Check the documentation snippets and fail if they need updating
	poetry run python -m scripts.docsub --check

fix: lint-fix format-fix docsub ## Fix all linting and formatting issues

install: ## Install dependencies with poetry
	poetry install --extras llm

test: ## Run unit tests
	poetry run pytest -vvv -s -o log_level=INFO -o log_cli=true tests/

test-examples: ## Run example tests
	@$(foreach example,$(examples),cd $(example) && make install build test && cd ../../..;)

requirements.txt: poetry.lock  ## Generate requirements.txt
	poetry export -f requirements.txt -o requirements.txt --without-hashes --extras llm

integ: ## Run integration tests
ifdef test
	poetry run pytest integ/ -k $(test)
else
	poetry run pytest -vvv -o log_level=INFO -o log_cli=true integ/
endif

help: ## Show this help message.
	@echo 'usage: make [target] ...'
	@echo
	@echo 'targets:'
	@egrep '^(.+)\:(.*) ##\ (.+)' ${MAKEFILE_LIST} | sed -s 's/:\(.*\)##/: ##/' | column -t -c 2 -s ':#'
