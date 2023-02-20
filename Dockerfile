## Install/build dependencies from apt and pip

FROM ubuntu:22.04 as core_dependencies
RUN apt-get update && apt-get install -y python3.10 ca-certificates
# Copy src
COPY src /opt/whylogs-container/src
COPY poetry.lock /opt/whylogs-container/
COPY pyproject.toml /opt/whylogs-container/
COPY README.md /opt/whylogs-container/
COPY whylabs_toolkit-0.0.1-py3-none-any.whl /opt/whylogs-container/

## Install/build pip dependencies
FROM core_dependencies as python_dependencies
RUN apt-get install -y curl build-essential python3-dev
# Install poetry
RUN curl -sSL https://install.python-poetry.org | python3.10 -
WORKDIR /opt/whylogs-container
RUN /root/.local/bin/poetry config virtualenvs.in-project true
RUN /root/.local/bin/poetry install --without=dev
# Pandas deploys a ton of tests to pypi
RUN rm -rf .venv/lib/python3.10/site-packages/pandas/tests

## Drop extra things needed only to install/build and reinstall runtime requirements
FROM core_dependencies
WORKDIR /opt/whylogs-container
COPY --from=python_dependencies /opt/whylogs-container ./
ENTRYPOINT [ "/bin/bash", "-c", "source .venv/bin/activate; cd src; python3.10 -m ai.whylabs.container.startup" ]
