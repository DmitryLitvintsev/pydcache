# Releasing pydcache

The version lives in one place, `pydcache/__init__.py`. Releases are
published to PyPI by GitHub Actions (`.github/workflows/publish.yaml`)
when a version tag is pushed, using PyPI Trusted Publishing, so no API
token is needed.

## Making a release

From a clean, up to date `main`:

```
bump-my-version bump patch     # 0.1.26 -> 0.1.27  (or: minor, major)
git push --follow-tags
```

`bump-my-version` updates the version, commits (`release X.Y.Z`) and
creates the tag `X.Y.Z`. It refuses to run if there are uncommitted
changes to tracked files. Preview a release with
`bump-my-version bump patch --dry-run -v`.

Pushing the tag starts the workflow. It runs the tests, builds the
package, checks that the tag matches the package version, runs
`twine check`, and publishes. Follow it in the repository's *Actions* tab.

Tools, installed once: `pip install bump-my-version` (or
`uv tool install bump-my-version`). They are also in the `dev` extra.

## One-time setup

1. On https://pypi.org, open *Your projects* → `pydcache` → *Manage* →
   *Publishing*, and add a GitHub trusted publisher:
   - Owner: `DmitryLitvintsev`
   - Repository name: `pydcache`
   - Workflow name: `publish.yaml`
   - Environment name: `pypi`
2. Optional: in GitHub *Settings* → *Environments* → `pypi`, add
   yourself as a *required reviewer*. Each release then waits for one
   click of approval before uploading. The environment is created
   automatically on the first run.
3. After the first successful automated release, revoke the old API
   token on PyPI and remove it from `~/.pypirc`.

## If the publish fails

PyPI never accepts the same version twice, so a version that was
actually uploaded can't be re-released; bump again instead.

If the workflow failed **before** uploading (e.g. tests failed or the
tag does not match), fix the problem, then move the tag:

```
git tag -d X.Y.Z && git push origin :refs/tags/X.Y.Z   # delete tag
# commit the fix, then re-tag and push
git tag -a X.Y.Z -m "pydcache X.Y.Z" && git push --follow-tags
```
