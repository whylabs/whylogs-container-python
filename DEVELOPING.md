# Install Dependencies

We have dependencies that are pulled from private repositories in GitLab. You can get the token from our 1password account under
[whylabs container pypi read token](https://start.1password.com/open/i?a=MNFHRX2GLNH5ZHI6RCE6O7XB2U&h=my.1password.com&i=aqbmbdrl7cvgb3drndslncnuji&v=4k54gbbmb2o3dsmvl7gebhwlpq).
Then you can configure poetry to use this for the `whylabs_container_gitlab` package source.

```
poetry config http-basic.whylabs_container_gitlab __token__
```

We also pull down private assets from the platform during the build. You should request access to our production `org-0` model and create an
api key that you can use in commands that require it, like so

```
WHYLABS_API_KEY=.... PYPI_API_KEY=... make docker-llm
```

You should be using a password manager with a CLI (like `pass`) to prevent saving passwords in your shell's history as well.

```
WHYLABS_API_KEY=(pass show whylabs-org-0-api-key) PYPI_API_KEY=(pass show gitlab-pypi-token) make docker-llm
```

# Generated Client

If you make any changes to the REST APIs then you may need to regenerate the python client.

```bash
make python-client
```

You should commit any changes that you see in git after running that command. It will update/add new routes if anything changed. If you
forgot to do this step then the build process will fail to remind you. If you want to test the integration changes locally with the new
client then you can run this:

```bash
make pip-install-python-client
```

This just installs the latest client using pip in your current virtualenv so it gets used when you run the integ tests with `make integ`.

# Releasing

You'll probably want to start with a regular PR to `master` that bumps the version. See `make help` for all of the various bump commands.

```bash
make bump-patch && make bump-release # Assuming this is a patch bump
git add -p # Make sure everything looks ok
git commit -m 'Bumping for release'

# do a PR as normal and after merging, also push to the release branch
git push origin HEAD:release
```

After this PR clears `master`, create a PR that merges `master' -> 'release` in GitHub. Everything happens automatically after the release
pr merges. The `master` branch should always be ahead of or equal to the `release` branch. The release job will upload the new container
images to Docker Hub with the version as the tag, as well as a release to PyPi for the client with the same version.

The [example repo](https://github.com/whylabs/langkit-container-examples?tab=readme-ov-file) is generated from the example_repo dir. After
the release, you should append a release notes section to the example repo's readme, [like
this](https://github.com/whylabs/langkit-container-examples?tab=readme-ov-file#release-109-notes).

## Dev Builds

If you want to release a dev build then the process is similar, but you won't run `bump-release`, you'll leave the version as a dev build
and push it to the `dev-release` branch instead.

```bash
make bump-patch
git add -p # Make sure everything looks ok
git commit -m 'Bumping for dev release'

# do a PR as normal and after merging, also push to the dev-release branch
git push origin HEAD:dev-release
```

This will omit some steps, like publishing the generated images with the `latest` tag and updating the public docs/example repo.

# Updating Documentation

This repo uses a tool called docsub which allows you to create snippets in code that can be insert into `.sub` files and "rendered" into a
normal file. For example, the following is what docsub snippet looks like.

```python
# DOCSUB_START snippet_name
def some_function():
    pass
# DOCSUB_END
```

This snippet can be inserted into a file that ends in `.sub`. Take `foo.md.sub` for example.

```markdown
This is some markdown file that shows the snippet above

<!-- DOCSUB_INSERT snippet_name -->
```

There are several `.sub` files in this repo, especially in the `example_repo` and `whylabs_docs` folders. The `.sub` files are mostly used
to render markdown that have snippets from unit tests to guarantee that the snippets are correct. If you want to update docs then you should
actually update the `.sub` files and then run `make fix` to update the rendered files. If you forget to run `make fix` then the CI will
fail.

The container is also the source of truth for the container docs that we host on the WhyLabs doc site. As part of release CI we push the
files in `whylabs_docs` to the doc repo, so you can't update certain pages in the doc site directly, you'll have to update them here and
then go through the release process. In general, if you ever want to reference any sample code then you should make sure that the sample is
taken from a docsub snippet, which might mean that you need to create a unit test as an excuse for that code to exist. For example, commit
`7a25adfc8cc452a99f9b509fad6bb4830bbe9cbe` added yaml policy files to one of the examples in order to use them as snippets in docs. Those
yaml files will fail the container startup in that particular example if they don't actually work.
