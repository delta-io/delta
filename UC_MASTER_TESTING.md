# Testing Delta Against Unity Catalog Master

Delta's sparkUnityCatalog and kernelUnityCatalog modules are exercised in CI against both a released Unity Catalog version *and* a specific commit on the Unity Catalog `main` branch. This doc explains what that means for local development.

## TL;DR

| You are… | What you need to do |
| --- | --- |
| A Delta dev **not** touching the `spark/unitycatalog` or `kernel/unitycatalog` modules | **Nothing.** The default `build/sbt` build resolves the released UC version from Maven Central. No change to your workflow. |
| A Delta dev working on UC integration, and the code still builds against released UC | **Nothing required**, but you can opt into the pinned UC master SHA by running one script (below). |
| A Delta dev writing code that uses UC APIs not yet released | Run the setup script once (see below). Then pass `-DunityCatalogVersion=<snapshot>` when building/testing. |

## Why we pin a UC SHA

The `Delta Spark (UC Master)` CI job builds Unity Catalog from source so Delta gets early signal on upcoming UC incompatibilities. If that job built from a floating `main`, an unrelated UC commit could turn Delta CI red in the middle of somebody else's PR. Pinning to a specific SHA removes that variable. The pin is bumped deliberately, in its own PR, so any new failures are attributable to that bump.

The pinned SHA lives in [`project/unitycatalog-pin.sha`](project/unitycatalog-pin.sha) as a single source of truth that both CI and the local setup script read.

## What stays the same for local dev

`build.sbt` still defaults to the most recent *released* UC version (`unityCatalogVersion = "0.4.1"` at time of writing). That means:

* `build/sbt compile`, `build/sbt spark/test`, etc. work with zero setup, as before.
* `build/sbt sparkUnityCatalog/test` and `build/sbt kernelUnityCatalog/test` run against released UC, as before.
* No new mandatory setup step, no submodules, no slow first builds.

## When you **do** need to run against UC master locally

Two cases:

1. You want to reproduce a failure from the `Delta Spark (UC Master)` CI job.
2. You are writing Delta code that needs a Unity Catalog API which isn't in the latest release yet. In this case your change will fail the default (released-UC) build — that's expected, and it means UC has to cut a release (or we have to wait to land your change) before Delta master can depend on it. In the meantime, you iterate locally against the pinned UC SHA.

### One-time setup

```bash
# From the Delta repo root. Takes a few minutes the first time.
bash project/scripts/setup_unitycatalog_main.sh
```

What this does:

* Clones UC at the SHA pinned in `project/unitycatalog-pin.sha` into `/tmp/unitycatalog` (override with `UC_DIR=...`).
* Runs `sbt publishLocal` / `publishM2` for the `client`, `server`, and `spark` UC modules so they are resolvable from your local Maven / Ivy caches.
* Writes the UC version string (e.g. `0.5.0-SNAPSHOT`) to `/tmp/unitycatalog/.uc-version`.

### Running Delta tests against the pinned UC master build

```bash
UC_VERSION=$(cat /tmp/unitycatalog/.uc-version)
build/sbt -DunityCatalogVersion=$UC_VERSION \
    sparkUnityCatalog/test kernelUnityCatalog/test
```

That `-DunityCatalogVersion` flag overrides the default in `build.sbt` for the duration of the sbt invocation only. Nothing is edited in your working tree, so you can freely switch back to the released-UC default by dropping the flag.

### Overriding the ref

For experimentation you can build a different UC ref without editing the pin file:

```bash
UC_REF=main bash project/scripts/setup_unitycatalog_main.sh            # latest main
UC_REF=abc1234 bash project/scripts/setup_unitycatalog_main.sh         # specific SHA
UC_REPO=git@github.com:myfork/unitycatalog.git \
  UC_REF=my-branch bash project/scripts/setup_unitycatalog_main.sh     # your fork
```

CI does **not** use these overrides — CI always builds the pinned SHA.

## Bumping the pinned SHA

1. Pick a newer SHA from [`unitycatalog/unitycatalog` commits on main](https://github.com/unitycatalog/unitycatalog/commits/main).
2. Replace the SHA line in `project/unitycatalog-pin.sha`.
3. Run the setup script + UC tests locally (the commands at the top of the pin file show the exact invocation).
4. Open a small PR that only changes the pin. If `Delta Spark (UC Master)` stays green, merge. If it fails, the failure is attributable to changes between the old and new UC SHAs, which makes triage easy.

## FAQ

**Q: What happens if Delta code starts using a UC API that isn't in the released UC version, and I don't run the setup script?**
The default build fails to compile `sparkUnityCatalog` / `kernelUnityCatalog`. The error message from sbt will name the missing UC symbol. Run the setup script and re-run sbt with `-DunityCatalogVersion=<snapshot>`; that's the signal to switch into UC-master mode.

**Q: Can I just always build against the pinned UC master locally?**
Yes — run the setup script once, then always pass `-DunityCatalogVersion=<snapshot>`. You can add that flag to a shell alias or to `SBT_OPTS`. Nothing in the repo forces you either way.

**Q: Why isn't this automated via an sbt task?**
An sbt task that shells out to `setup_unitycatalog_main.sh` on demand is possible, but it makes `build/sbt` depend on git, network, and a UC build for devs who never touch UC integration. Keeping setup explicit keeps the blast radius contained and makes failures easier to diagnose. If the UC integration surface grows enough that this stops being true, automating it is a follow-up.

**Q: Why is the build.sbt default not bumped to the UC master snapshot?**
Because every Delta dev would then need to run the setup script before anything builds, even on changes unrelated to UC. As long as Delta master compiles cleanly against released UC, keeping the released version as the default is pure upside for local dev velocity. The day a Delta PR *has* to use a UC master API is the day to revisit this — and at that point, bumping the default in `build.sbt` is a one-line change.
