# Building Delta Against Pinned Unity Catalog Master

Delta master depends on Unity Catalog APIs that are not yet in a released UC version. To make CI and local builds reproducible, the exact UC commit Delta builds against is pinned in [`project/unitycatalog-pin.sha`](project/unitycatalog-pin.sha). This doc explains what that means for local development, and how to bump the pin.

> **Release branches** use a released UC version instead and skip everything below. See the [Release-branch mode](#release-branch-mode) section for the one-line switch and the CI adjustments that go with it.

## TL;DR for local dev

Just run sbt the way you normally would:

```bash
build/sbt sparkUnityCatalog/testOnly io.delta.unity.SomeSuite
build/sbt kernelUnityCatalog/test
build/sbt compile
```

That's it. `build.sbt` hooks an `ensurePinnedUnityCatalog` task into the `update` task of the two UC-dependent projects (`sparkUnityCatalog`, `kernelUnityCatalog`). On a clean checkout, the first sbt invocation that touches either project notices the pinned UC jars aren't in `~/.ivy2/local`, runs `project/scripts/setup_unitycatalog_main.sh` inline (you'll see `[UC setup] …` lines streaming in the sbt log), and proceeds. Takes a few minutes the first time, <1s every other time.

You can also run the script directly if you want — the auto-trigger and manual invocation are the same code path:

```bash
bash project/scripts/setup_unitycatalog_main.sh
```

No `-DunityCatalogVersion=…` flag is needed: `build.sbt` reads the pinned SHA and composes the version string itself.

To opt out of the auto-trigger (and instead get a clear error pointing at the script):

```bash
build/sbt -Ddelta.autoBuildPinnedUnityCatalog=false sparkUnityCatalog/test
```

### How the idempotency check works

The setup script always does a shallow SHA-scoped clone of UC to read `version.sbt` (takes ~1s), then composes the Ivy coordinate `<base>-<pinnedSha>` — e.g. `0.5.0-SNAPSHOT-a7683a23063dab9b5faa534a38b3a9080461e62f`. It then checks whether the canonical Ivy artifact path

```
~/.ivy2/local/io.unitycatalog/unitycatalog-client/<coordinate>/ivys/ivy.xml
```

already exists. That's the exact path sbt itself uses for resolution, so "the file is there" and "sbt can already resolve this dep" are the same statement — no separate marker file, no secondary state to get out of sync. If it's there, the expensive sbt publish steps are skipped. If it's missing (fresh clone, or the pin moved, or someone nuked `~/.ivy2`), we rebuild.

Encoding the SHA in the coordinate is also what eliminates the stale-jar gap on pin bumps: the coordinate changes even when UC's `version.sbt` didn't move, so a stale publish from a prior pin can never resolve silently.

## Why UC master instead of a release?

Delta and Unity Catalog are developed in lockstep on several features. Delta master often needs APIs that have been merged to UC `main` but not yet shipped in a UC release. Rather than waiting for the next UC release to land each feature, we pin a specific UC commit, build it locally, and use it as a pre-release dependency.

Pinning (vs. tracking a floating UC `main`) is what keeps this tolerable: every Delta PR builds against the same UC commit, so an unrelated UC-side change can't silently break Delta CI in the middle of another contributor's PR.

## What if I'm not touching UC integration at all?

You still get the UC build once — `sparkUnityCatalog` is part of `sparkGroup`, and any sbt command that triggers cross-project resolution (`publishM2`, `++ <scala>`, aggregate tasks) goes through the UC projects' `update` task, which runs the auto-trigger. On a fresh checkout that means the first sbt invocation takes a few extra minutes; subsequent invocations are unaffected.

If the one-time build is genuinely painful, disable the auto-trigger with `-Ddelta.autoBuildPinnedUnityCatalog=false` for individual invocations that you know don't need UC.

## Running against a non-pinned ref (experiments)

Override the ref via environment variables. The setup script computes a different coordinate for the override, so the Ivy-cache check naturally falls through to a rebuild (unless that same override was already published):

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

## Release-branch mode

When cutting a Delta release branch that should build against a *released* UC version rather than the pinned master SHA:

1. In `build.sbt`, set `unityCatalogReleaseVersion` to the released version (e.g. `Some("0.5.0")`). Leave `unityCatalogBaseVersion` alone; it's only used in pinned-master mode.
2. Remove the `Set up pinned Unity Catalog` step from each CI workflow that has one (`spark_test`, `kernel_unitycatalog_test`, `build`, `kernel_test`, `unidoc`, `spark_examples_test`). In release mode the composite action just wastes CI time publishing jars sbt won't use.
3. Optional: delete `project/unitycatalog-pin.sha`, `project/scripts/setup_unitycatalog_main.sh`, and `.github/actions/setup-unitycatalog/` on the release branch. They're dead code in release mode, and removing them makes that fact obvious to readers. (If you keep them, `build.sbt` will ignore them — `pinnedUnityCatalogSha` is lazy and is only read when `unityCatalogReleaseVersion` is `None`.)

After step 1 alone, `build/sbt compile` on the release branch resolves `io.unitycatalog:unitycatalog-*:<released-version>` from Maven Central exactly like any other library, and `ensurePinnedUnityCatalog` is a no-op (the task body is skipped when `unityCatalogReleaseVersion` is set).

## Troubleshooting

**`sbt` complains it can't resolve `io.unitycatalog:unitycatalog-spark_…:0.5.0-SNAPSHOT-<sha>`.**
The auto-trigger is either disabled (`-Ddelta.autoBuildPinnedUnityCatalog=false`) or you're sbt-resolving from a scope that doesn't go through `sparkUnityCatalog` / `kernelUnityCatalog`'s `update`. Run `bash project/scripts/setup_unitycatalog_main.sh` directly.

**The `<sha>` in the error message is an old one.**
Your working tree's `project/unitycatalog-pin.sha` doesn't match what `build.sbt` is reading. Either you have a stale file (pull latest) or sbt is running from the wrong directory (run from the repo root).

**The `<base>` part of the error doesn't match UC's `version.sbt`.**
Someone bumped the pin to a UC commit with a newer `version.sbt` but forgot to update `unityCatalogBaseVersion` in `build.sbt`. Fix in one line.

**The setup script doesn't rebuild when I expect it to.**
Pass `UC_FORCE=1` to force a rebuild regardless of the Ivy cache. If something's wrong with the published jars, you can also delete the offending coordinate's directory under `~/.ivy2/local/io.unitycatalog/unitycatalog-{client,server,spark}/` and the next sbt invocation will republish.

**CI passes but local fails (or vice versa).**
Check that `~/.ivy2/local/io.unitycatalog/unitycatalog-client/` has a directory named for the composed coordinate your `project/unitycatalog-pin.sha` implies. If not, run the setup script.
