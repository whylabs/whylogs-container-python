##
## Install/build dependencies from apt and pip
##
FROM ubuntu:22.04 as core_dependencies
RUN apt-get update && apt-get install -y python3.10 ca-certificates

##
## Install/build pip dependencies
##
FROM core_dependencies as python_dependencies
RUN apt-get install -y curl build-essential python3-dev
# Install poetry
RUN curl -sSL https://install.python-poetry.org | python3.10 -
# Copy poetry files over for python dependencies
COPY poetry.lock /opt/whylogs-container/
COPY pyproject.toml /opt/whylogs-container/
WORKDIR /opt/whylogs-container
RUN /root/.local/bin/poetry config virtualenvs.in-project true
RUN /root/.local/bin/poetry install --no-root --without=dev
# Pandas deploys a ton of tests to pypi
RUN rm -rf .venv/lib/python3.10/site-packages/pandas/tests

##
## Copy required files from previous steps and copy src over
##
FROM core_dependencies
WORKDIR /opt/whylogs-container
COPY --from=python_dependencies /opt/whylogs-container ./
COPY src /opt/whylogs-container/src
EXPOSE 8000
ENTRYPOINT [ "/bin/bash", "-c", "source .venv/bin/activate; cd src; python3.10 -m ai.whylabs.container.startup" ]
