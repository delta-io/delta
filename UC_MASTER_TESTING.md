# Building Delta Against Pinned Unity Catalog Master

Delta master depends on Unity Catalog APIs that are not yet in a released UC version. To make CI and local builds reproducible, the exact UC commit Delta builds against is pinned in [`project/unitycatalog-pin.sha`](project/unitycatalog-pin.sha). This doc explains what that means for local development, and how to bump the pin.

## TL;DR for local dev

Run this **once** per clean checkout (and once more each time the pin is bumped):

```bash
bash project/scripts/setup_unitycatalog_main.sh
```

After that, regular sbt works as you'd expect:

```bash
build/sbt compile
build/sbt sparkUnityCatalog/test
build/sbt kernelUnityCatalog/test
# …etc.
```

The setup script is idempotent: on re-invocation it checks a marker under `~/.ivy2/local` and exits in under a second if the pinned SHA's jars are already published. The slow rebuild only fires when the pin moves (or when you pass `UC_FORCE=1`).

No `-DunityCatalogVersion=…` flag is needed: `build.sbt` reads the pinned SHA and composes the version string itself.

### Why the Ivy version encodes the SHA

UC is published locally as `<base>-<pinnedSha>` — e.g. `0.5.0-SNAPSHOT-a7683a23063dab9b5faa534a38b3a9080461e62f` — rather than the bare `0.5.0-SNAPSHOT` that UC's `version.sbt` declares. Encoding the SHA in the version string is what keeps stale jars from silently resolving: when the pin bumps, the Ivy coordinate changes, so sbt is forced to find the new jars (and the setup script must have run to publish them).

## Why UC master instead of a release?

Delta and Unity Catalog are developed in lockstep on several features. Delta master often needs APIs that have been merged to UC `main` but not yet shipped in a UC release. Rather than waiting for the next UC release to land each feature, we pin a specific UC commit, build it locally, and use it as a pre-release dependency.

Pinning (vs. tracking a floating UC `main`) is what keeps this tolerable: every Delta PR builds against the same UC commit, so an unrelated UC-side change can't silently break Delta CI in the middle of another contributor's PR.

## What if I'm not touching UC integration at all?

You still need to run the setup script once, because `sparkUnityCatalog` is part of `sparkGroup` (the test group the `Delta Spark` workflow exercises), so plain `build/sbt compile` transitively pulls in the UC dependency.

If the one-time build is genuinely painful for your workflow, tell us — we can look into publishing UC master snapshots to a shared Maven repo so local builds can resolve them without building from source.

## Running against a non-pinned ref (experiments)

You can override the ref via environment variables; the marker optimization turns off for non-default refs, so every invocation rebuilds:

```bash
UC_REF=main bash project/scripts/setup_unitycatalog_main.sh           # floating UC main
UC_REF=abc1234 bash project/scripts/setup_unitycatalog_main.sh        # specific SHA
UC_REPO=git@github.com:myfork/unitycatalog.git \
  UC_REF=my-branch bash project/scripts/setup_unitycatalog_main.sh    # your UC fork
```

When you override `UC_REF`, the setup script publishes as `<base>-<that-ref>`, which won't match the coordinate `build.sbt` derives from the pin file. Pass `build/sbt -DunityCatalogVersion=$(cat /tmp/unitycatalog/.uc-version) …` for the duration of that experiment.

## Bumping the pin

1. Pick a newer SHA from [`unitycatalog/unitycatalog` commits on main](https://github.com/unitycatalog/unitycatalog/commits/main).
2. Edit `project/unitycatalog-pin.sha`, replacing only the SHA line.
3. Run the setup script locally to verify the new UC commit still builds:
   ```bash
   bash project/scripts/setup_unitycatalog_main.sh
   ```
4. Check the UC base version it prints. If `version.sbt` on UC has changed (e.g. `0.5.0-SNAPSHOT` → `0.6.0-SNAPSHOT`), also edit `build.sbt`'s `unityCatalogBaseVersion` to match, in the same commit — otherwise the composed `<base>-<sha>` coordinate won't match what the setup script publishes and sbt resolution fails.
5. Run the UC tests:
   ```bash
   build/sbt sparkUnityCatalog/test kernelUnityCatalog/test
   ```
6. Open a focused PR (pin + build.sbt default, nothing else). If CI stays green, merge. If it fails, the failure is attributable to changes between the old and new UC SHAs, which makes triage easy.

## Troubleshooting

**`sbt` complains it can't resolve `io.unitycatalog:unitycatalog-spark_…:0.5.0-SNAPSHOT-<sha>`.**
Run `bash project/scripts/setup_unitycatalog_main.sh`. The marker under `~/.ivy2/local/.unitycatalog-pin` was missing or didn't match the pinned SHA, so the publish step hadn't happened (or was for a stale pin).

**The `<sha>` in the error message is an old one.**
Your working tree's `project/unitycatalog-pin.sha` doesn't match what `build.sbt` is reading. Either you have a stale file (pull latest) or sbt is reading from the wrong directory (run sbt from the repo root).

**The `<base>` part of the error doesn't match UC's `version.sbt`.**
Someone bumped the pin to a UC commit with a newer `version.sbt` but forgot to update `unityCatalogBaseVersion` in `build.sbt`. Fix in one line.

**I changed the pin and the setup script still emits the old version.**
Stale marker. Pass `UC_FORCE=1` or delete `~/.ivy2/local/.unitycatalog-pin`, then re-run the setup script.

**CI passes but local fails (or vice versa).**
Check your `~/.ivy2/local/.unitycatalog-pin` SHA matches `project/unitycatalog-pin.sha`. If different, rerun the setup script.
