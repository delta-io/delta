# AGENTS.md — Delta Lake

Instructions for AI agents working in this repository: writing code, reviewing
pull requests, or answering questions about it. This file covers the whole
repo. Some areas have their own, more detailed `AGENTS.md`; when a task
touches one of those areas, read that file too. The areas are listed in the
sections below.

## Repository layout

| Path | What it holds |
|---|---|
| `PROTOCOL.md` | The Delta transaction log protocol specification. |
| `protocol_rfcs/` | Proposed, accepted and rejected RFCs for protocol changes. |
| `spark/`, `spark-unified/`, `spark-connect/`, `spark-shared-tests/` | Delta Lake on Apache Spark. |
| `kernel/` | Delta Kernel (Java). |
| `flink/` | Flink connector. |
| `connectors/` | Shared connector test assets (e.g. golden tables). |
| `iceberg/`, `hudi/` | UniForm (Iceberg and Hudi compatibility). |
| `storage/`, `storage-s3-dynamodb/` | `LogStore` implementations. |
| `sharing/` | Delta Sharing integration. |
| `python/` | Python APIs. |
| `docs/`, `examples/`, `benchmarks/` | Documentation, examples and benchmarks. |
| `build.sbt`, `project/`, `build/sbt` | sbt build. Run sbt through `build/sbt`. |

## General conventions

- Code style follows the
  [Apache Spark Scala Style Guide](https://spark.apache.org/contributing.html)
  (see `CONTRIBUTING.md`). Scalastyle rules are in `scalastyle-config.xml`.
- PR titles start with a component tag such as `[Spark]`, `[Kernel]`,
  `[Flink]`, `[UniForm]`, `[Storage]`, `[Build]` or `[PROTOCOL]`.
- Keep one concern per PR. In particular, don't mix protocol spec changes with
  engine code (see the next section).
- Major features (more than about 100 changed lines excluding tests, or any
  user-facing behavior change) need a GitHub issue agreed on first
  (`CONTRIBUTING.md`).
- Commits must be signed off (`git commit -s`) under the Developer
  Certificate of Origin.

## Protocol and RFC changes

**This applies whenever a change touches `PROTOCOL.md` or anything under
`protocol_rfcs/`**, whether you are reviewing a PR or writing the change, and
including PRs that edit only `PROTOCOL.md` with no RFC.

Before doing anything else, read [`protocol_rfcs/AGENTS.md`](protocol_rfcs/AGENTS.md)
in full and follow it. It covers both writing and reviewing, in five parts:
1. **Process:** the PR types, and one checklist per type (new RFC, update,
   acceptance, rejection, direct spec edit, editorial).
2. **Protocol design rules:** readers-and-writers vs writers-only features,
   reader and writer requirements, compatibility, and interactions with other
   features.
3. **Writing the spec text:** drafting from
   [`protocol_rfcs/template.md`](protocol_rfcs/template.md), normative
   language, examples, and `PROTOCOL.md` style.
4. **Reviewing a protocol PR:** the procedure and the required output format.
5. **Self-review** before opening the PR.

Don't write or review a protocol change from this summary alone.

The short version, so it isn't missed:

- The protocol is a standard implemented by many independent engines.
  Anything that could make an old or other-engine client silently return wrong
  results or corrupt a table is a blocking issue.
- New table features, new action fields, and new requirements on readers or
  writers need an RFC under `protocol_rfcs/`. They can't go straight into
  `PROTOCOL.md`.
- Spec changes go in their own PR, separate from engine code.
