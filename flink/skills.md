# Flink Connector Development Skills

## Pull Request Requirements

### Repository
- All newly created PRs must target the repository:
  - `github.com/delta-io/delta`

### Pre-push Validation
Before pushing any PR, always run the following sbt tasks successfully:

```bash
build/sbt "flink / javafmt"
build/sbt "flink / Test / javafmt"
build/sbt "flink / test"
build/sbt "flink / Compile / doc"
```

### PR Tracking
For every newly created PR:
- Add the PR link to the tracking issue:
  - https://github.com/delta-io/delta/issues/5901

## Development Expectations

- Ensure formatting is clean before push.
- Ensure all Flink tests pass locally before opening or updating a PR.
- Ensure generated documentation compiles successfully.
- Keep PR descriptions clear and scoped to a single logical change whenever possible.
