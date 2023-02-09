FROM ubuntu:22.04

RUN apt-get update
RUN apt-get install -y python3.10 curl build-essential python3-dev
RUN curl -sSL https://install.python-poetry.org | python3.10 -

COPY src /opt/whylogs-container/src
COPY poetry.lock /opt/whylogs-container/
COPY pyproject.toml /opt/whylogs-container/
# Because poetry needs it...
COPY README.md /opt/whylogs-container/ 

WORKDIR /opt/whylogs-container
RUN /root/.local/bin/poetry config virtualenvs.in-project true
RUN /root/.local/bin/poetry install

RUN apt-get remove -y build-essential curl python3-dev
RUN apt-get autoremove -y
RUN rm -rf /root/.cache

WORKDIR /opt/whylogs-container/src
ENTRYPOINT [ "/root/.local/bin/poetry", "run", "python", "-m", "ai.whylabs.container.startup" ]