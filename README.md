# whylogs/langkit Container

This is the whylogs and langkit container, a container that hosts [whylogs](https://github.com/whylabs/whylogs) behind a REST interface that
can be used to generate whylogs profiles for data monitoring and validate LLM prompts/responses. This container is a good choice for any
whylogs user who doesn't want to embed the whylogs or langkit into their code base to directly, or prefers to keep them separate from
their main application for any reason. It will handle the logic of profiling and periodically uploading data to WhyLabs for you.

## Links

- [General Documentation][whylabs.ai docs]
- [Docker whylogs Image](https://hub.docker.com/r/whylabs/whylogs/tags)
- [Docker langkit Image](https://hub.docker.com/r/whylabs/langkit/tags)
- [Python Docs](https://whylabs.github.io/whylogs-container-python-docs/index.html)
- [REST API Overview](https://whylabs.github.io/whylogs-container-python-docs/whylogs-container-python.html#operation/log_docs_log_docs_post)
- [Environment Config Variables][env vars]

## Configuration and Running

For most general purpose documentation on running and configuring the container,
see [general documentation][whylabs.ai docs]. To run the container:

```bash
# For the normal container (no LLM support)
docker run -p 127.0.0.1:8000:8000 --env-file local.env -it whylabs/whylogs:3.0.0

# For the LLM container (extra models, langkit installed)
docker run -p 127.0.0.1:8000:8000 --env-file local.env -it whylabs/langkit:3.0.0

# Mac ARM users will probably need to specify a platform
docker run -p 127.0.0.1:8000:8000 --platform linux/amd64 --env-file local.env -it whylabs/whylogs:3.0.0
```

It depends on having a `local.env` file for configuration. See [the docs][env vars] for a full list of env variables.

```bash
static_secret=password
s3_profile_upload_bucket=my-profile-upload-bucket
```

Or, for running the application directly without using docker

```
# depends on ./env/personal.env existing with env config

# for non-llm
make server

# for llm
OPENAI_API_KEY=<api-key> make server-llm
```

## Building

Building the docker images can be done via

```bash
# For the non llm version
make docker

# For the llm version. Make sure you give docker ~6gb of ram and you have ~6gb of disk
# space, it downloads a lot of models.
OPENAI_API_KEY=<api-key> make docker-llm
```

## Developing

Most useful commands are targets in the Makefile. While doing local development,
you can run `make server` to build and run the server locally (without Docker).
Configuration is done via environment variables and you'll need to set some of
them to get the service working.

If you're running this from bash/zsh then you can add this to your environment
by running.

```bash
set -a
source local.env
set +a
```

Run `make help` for other useful targets. Mostly, you can depend on GitHub CI to
run the right tests and checks for each PR.

[whylabs.ai docs]: https://docs.whylabs.ai/docs/integrations-whylogs-container
[env vars]: https://whylabs.github.io/whylogs-container-python-docs/generated/whylogs_container.whylabs.container.server_config.html#whylogs_container.whylabs.container.server_config.ServerConfig

### Dev Container Development (Mac)

NOTE: Make sure you set the global resource limits in Docker Desktop UI to >=6gb memory, >=4 cpu cores.

There are several libraries that don't work on mac. You can build the docker container and run it to test things but it takes a while and
its annoying to iterate. Microsoft's Dev Container system works well as an alternative. To get it setup, install the Dev Container extension
(from Microsoft) in VSCode and interact with it via the command palette (cmd+shift+p). If you launch VSCode from this project then it will
auto detect the `.devcontainer` dir and ask if you want to open it. It should "just work" from there. Typically, you won't have to do much
with your setup after that besides the occasional "Dev Container: rebuild" in the command palette if things get weird and need to be reset.
You'll be able to launch bash/zsh terminal sessions via VSCode in the container with this project mounted.

Make sure you have a `.devcontainer/devcontainer.env` file with the required api keys first.

```
WHYLABS_API_KEY=...
GITLAB_API_KEY=...
```

To run the server from the Dev Container terminal use

```
# depends on ./env/personal.env existing with env config
OPENAI_API_KEY=<api-key> make server-llm
```

Just work as you normally would outside of the container using whatever tools you prefer and use the container to run the server. By
default, it will be accessible via `localhost:8000`, just as if you were doing it locally on Linux. If you need to manage the container
(delete it, stop it, etc) then its easiest to do it via Docker Desktop or CLI after exiting VSCode. Dev Containers are really a glorified
wrapper around Docker configured by a json file.
