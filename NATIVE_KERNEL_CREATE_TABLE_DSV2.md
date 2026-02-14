## **Problem Statement: Native Delta Kernel CREATE TABLE in DSv2**

### **Objective**

Implement a **metadata-only** `CREATE TABLE` operation within the Delta-Spark **V2 Connector**
that uses **Delta Kernel primitives for all commits**.

The implementation must support **Unity Catalog (CCv2)** managed tables and **path-based**
external tables, while being **extensible** for future DDL operations (CTAS/RTAS/CREATE TABLE LIKE).

### **Supported Scenarios**

The implementation handles these `CREATE TABLE` invocations when
`DeltaSQLConf.V2_ENABLE_MODE = STRICT`.

1. **Catalog-Managed Table**
   - **Context**: user creates a table in a UC-enabled catalog / a catalog-managed environment
   - **Mechanism**: route the commit through the UC committer (CCv2) via `UCCatalogManagedClient`
   - **Syntax**:

```sql
CREATE TABLE my_catalog.default.my_table (
  id INT,
  name STRING
) USING delta;
```

2. **Path-Based External Table**
   - **Context**: user creates a table at a specific filesystem location using the `delta.` prefix
   - **Mechanism**: use Kernel’s default filesystem committer to write `_delta_log` at the path
   - **Syntax**:

```sql
CREATE TABLE delta.`/tmp/delta-table` (
  id INT,
  name STRING
) USING delta;
```

### **Technical Requirements**

1. **Routing**: override `createTable` in the unified `DeltaCatalog` to intercept requests in
   STRICT mode and route them to the V2/Kernel implementation.
2. **Kernel integration**: use `TableManager.buildCreateTableTransaction(...)` to construct the
   transaction for non-UC tables.
3. **CCv2 support**: inject the catalog committer for UC-managed tables by building the create
   transaction through `UCCatalogManagedClient.buildCreateTableTransaction(...)`.
4. **Extensibility**:
   - decouple request resolution (schema, partitions, properties, path) from transaction execution
   - keep a design that can later support CTAS/RTAS with minimal refactoring (i.e., reuse the same
     planned state and committer decision)

---

### **Background (why this change was needed)**

In `STRICT` mode, Delta’s unified catalog already **returns V2 tables** (a V2 `SparkTable`),
but historically **`CREATE TABLE` still committed via the V1 create pipeline**.

For **UC-managed tables**, the long-term direction is **Unity Catalog as the single source of
truth** (table identity/governance + commit coordination via CCv2). When `CREATE TABLE` commits via
the legacy V1 stack, it becomes harder to reason about whether a managed-table create is
exercising the intended Kernel/CCv2 path vs. Delta’s V1 transaction stack.

This change makes `CREATE TABLE` in STRICT mode **execute the commit via Kernel** (and via
`UCCatalogManagedClient` when UC-managed). We use `commitInfo.engineInfo` only as a
**test/diagnostic signal** to prove which stack authored the initial Delta log commit.

---

### **ELI10 mental model**

Think of `DeltaCatalog` as the **front desk** for “make me a new Delta table”.

- If STRICT mode is **off**, we tell the existing V1 implementation to handle everything.
- If STRICT mode is **on** and this is a Delta table, we:
  - compute where the table should live (path-based or warehouse location)
  - ask Kernel to write the initial `_delta_log` entries (protocol + metadata only)
  - for catalog-based tables: register the table in Spark’s metastore after the Kernel commit

If you only remember one thing:
- **STRICT + Delta provider** → Kernel writes `_delta_log/000...00.json`
- **NONE / non-Delta provider** → V1 pipeline writes `_delta_log/...`

---

### **Architecture / flow**

#### **Before (existing behavior): STRICT returned V2 tables, but CREATE committed via V1 (`CreateDeltaTableCommand`)**

This is the “why the commit authoring path was ambiguous” flow: even in STRICT mode,
`CREATE TABLE` committed via the V1 pipeline, so the initial Delta log commit was authored by V1
internals instead of Kernel (and, for UC-managed tables, instead of the Kernel+CCv2 path).

```text
SQL: CREATE TABLE my_catalog.default.my_table ... USING delta
        │
        ▼
Spark SQL parser/analyzer resolves catalog "my_catalog"
        │
        ▼
DeltaCatalog (CatalogExtension)
  delegate = io.unitycatalog.spark.UCSingleCatalog   (Unity Catalog catalog plugin)
        │
        ▼
AbstractDeltaCatalog#createTable (V1 implementation)
        │
        ▼
createDeltaTable(...)
  - resolve LOCATION / warehouse default
  - respect PROP_IS_MANAGED_LOCATION when delegate is UC
  - filter Spark catalog properties
  - translate UC table id key (UC_TABLE_ID_KEY_OLD → UC_TABLE_ID_KEY)
        │
        ▼
CreateDeltaTableCommand (V1)
  - DeltaLog / OptimisticTransaction
  - commit protocol + metadata (metadata-only CREATE has no AddFile actions)
        │
        ▼
_delta_log/00000000000000000000.json
  commitInfo.engineInfo ≠ Kernel-*/kernel-spark-dsv2
```

#### **After (new behavior): STRICT routes CREATE to Kernel (metadata-only), with optional CCv2 commit path**

In STRICT mode, we now intercept `createTable` and force the commit to go through Kernel
(`TableManager` or `UCCatalogManagedClient`).

```
SQL: CREATE TABLE ... USING delta
        │
        ▼
Spark SQL parser/analyzer resolves target catalog (e.g., "my_catalog")
        │
        ▼
  DeltaCatalog#createTable   (spark-unified)
  delegate may be io.unitycatalog.spark.UCSingleCatalog (UC catalogs)
        │
        ├─ if V2_ENABLE_MODE != STRICT  →  super.createTable (V1)
        │
        ├─ if provider != delta         →  super.createTable (V1)
        │
        ▼
  STRICT + delta provider
        │
        ├─ path identifier (delta.`/path`)
        │     └─ DeltaKernelStagedCreateTable(commit @ /path)  (spark/v2)
        │
        └─ catalog identifier (db.table)
              ├─ V2CreateTableHelper.buildCatalogTableSpec()
              │     └─ resolve LOCATION (or warehouse default)
              ├─ DeltaKernelStagedCreateTable(commit @ resolved path) (spark/v2)
              └─ V2CreateTableHelper.registerTable() (Spark SessionCatalog)
```

The UC-managed branch happens *inside* `DeltaKernelStagedCreateTable`:

```text
DeltaKernelStagedCreateTable#commitStagedChanges
  │
  ├─ if UCCommitCoordinatorClient.UC_TABLE_ID_KEY is present in (filtered) properties:
  │     ├─ resolve UC config via UCCommitCoordinatorBuilder.getCatalogConfigMap(spark)
  │     │    (derived from Spark conf like:
  │     │     spark.sql.catalog.<catalogName> = io.unitycatalog.spark.UCSingleCatalog
  │     │     spark.sql.catalog.<catalogName>.uri / .token / auth config ...)
  │     ├─ UCClient = UCTokenBasedRestClient(ucUri, tokenProvider)
  │     └─ UCCatalogManagedClient.buildCreateTableTransaction(...).commit(emptyIterable)
  │
  └─ else:
        └─ TableManager.buildCreateTableTransaction(...).commit(emptyIterable)

Result: _delta_log/000...00.json has commitInfo.engineInfo ending with /kernel-spark-dsv2
```

Key point: we **don’t** route through Spark’s `stageCreate*` entry points yet; CTAS/RTAS staging
remains V1 today. We still unify the Kernel create logic in one place (a staged-table object) so
future write support can “snap in”.

---

### **Implementation overview**

#### **1) Unified routing entry point: `DeltaCatalog#createTable`**

File: `spark-unified/src/main/java/org/apache/spark/sql/delta/catalog/DeltaCatalog.java`

Behavior:
- gates on `new DeltaV2Mode(conf).shouldCatalogReturnV2Tables()` (STRICT mode)
- only intercepts when `properties.get(TableCatalog.PROP_PROVIDER)` is a Delta provider
- for catalog-based identifiers, `V2CreateTableHelper.buildCatalogTableSpec(...)` checks table
  existence up front so `CREATE TABLE` fails before any Kernel commit side effects
- routes by identifier type:
  - **path-based**: commit directly to `ident.name()`, no catalog registration hook
  - **catalog-based**: resolve `(CatalogTable, tablePath)` via `V2CreateTableHelper`, then
    commit via Kernel and register into `SessionCatalog` after success

#### **2) Kernel-backed “staged” implementation: `DeltaKernelStagedCreateTable`**

File: `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/DeltaKernelStagedCreateTable.java`

What it is:
- a Kernel-backed implementation of Spark’s `StagedTable`
- **today**: used internally by `DeltaCatalog#createTable` to perform metadata-only create
- **future**: intended scaffolding for CTAS/RTAS once we implement `SupportsWrite`

What it does in `commitStagedChanges()`:

- **Plan inputs** (pure “request resolution”):
  - Spark schema → Kernel schema via
    `SchemaUtils.convertSparkSchemaToKernelSchema(...)`
  - Spark `Transform[]` partitions → Kernel `DataLayoutSpec` (identity-only for now)
  - property filtering to match V1 semantics (strip Spark catalog keys before persisting)
  - translate legacy UC table ID property key →
    `UCCommitCoordinatorClient.UC_TABLE_ID_KEY`

- **Choose commit path**:
  - if UC table id property is present:
    - resolve UC config for the active catalog plugin via
      `UCCommitCoordinatorBuilder.getCatalogConfigMap(spark)`
    - open a `UCClient` (`UCTokenBasedRestClient`)
    - build the create transaction through
      `UCCatalogManagedClient.buildCreateTableTransaction(...)`
      (this is what wires in the UC committer / CCv2)
  - else:
    - build the create transaction through
      `TableManager.buildCreateTableTransaction(...)`

- **Commit**:
  - set `engineInfo` marker to `kernel-spark-dsv2` (Kernel prefixes it with its own version)
  - `builder.build(engine).commit(engine, CloseableIterable.emptyIterable())`
    (empty iterable → metadata-only: no `AddFile` actions)

- **Post-commit hook**:
  - for catalog-based creates, we run a hook to register the Spark `CatalogTable` only after the
    Kernel commit succeeds

Why the UC client lifetime matters:
- The committer produced by `UCCatalogManagedClient` holds onto the `UCClient`, so we keep the
  `UCClient` open for the entire duration of `Transaction.build()` and `Transaction.commit()`.

#### **3) Catalog registration helper: `V2CreateTableHelper`**

File: `spark-unified/src/main/scala/org/apache/spark/sql/delta/catalog/V2CreateTableHelper.scala`

Why it exists:
- `CatalogTable` is a Scala type with Scala-friendly construction; building it directly from Java
  in `DeltaCatalog.java` is awkward.

Responsibilities:
- `buildCatalogTableSpec(...)`: returns `(CatalogTable, tablePath)`
  - resolves location using `PROP_LOCATION` or Spark’s warehouse default
  - sets `CatalogTableType` as MANAGED vs EXTERNAL using V1-compatible semantics
  - converts identity partition transforms to `partitionColumnNames`
  - filters properties with the same keys filtered in V1
- `registerTable(...)`: calls `spark.sessionState.catalog.createTable(...)`

---

### **Properties, partitioning, and semantics**

- **Metadata-only**:
  - commit contains only protocol + metadata actions (no files)
- **Property filtering** (matches V1 filtering):
  - does **not** persist Spark catalog keys like:
    - `TableCatalog.PROP_PROVIDER`, `PROP_LOCATION`, `PROP_COMMENT`, `PROP_OWNER`, `PROP_EXTERNAL`
    - `path`, `option.path`
  - source of truth for “which keys are filtered”: V1 `AbstractDeltaCatalog#createDeltaTable`
    (see `spark/src/main/scala/org/apache/spark/sql/delta/catalog/AbstractDeltaCatalog.scala`)
- **Partitioning**:
  - supports only identity partitioning (`PARTITIONED BY (col1, col2, ...)`)
  - expression transforms (bucket, years, etc.) are rejected for now
- **Error semantics**:
  - `CREATE TABLE` fails if the table already exists
  - `CREATE TABLE IF NOT EXISTS` is idempotent (no additional commit)

---

### **Verification and tests**

Because STRICT mode already returns a V2 `SparkTable`, unit tests need a simple way to prove the
commit was authored by the intended stack. For this prototype we use `commitInfo.engineInfo` as a
practical **verification proxy** in the Delta log:

- we set `engineInfo = "kernel-spark-dsv2"`
- Kernel prefixes it in the actual JSON (e.g. `Kernel-<ver>/kernel-spark-dsv2`)
- tests assert suffix match on the commitInfo’s `engineInfo`

This is not the long-term product contract: for UC-managed tables the goal is UC as the source of
truth, and future integration tests should validate UC catalog / CCv2 behavior directly.

Test file:
- `spark-unified/src/test/scala/org/apache/spark/sql/delta/catalog/DeltaCatalogSuite.scala`

Test coverage includes:
- STRICT mode:
  - path-based `CREATE TABLE` → engineInfo ends with `/kernel-spark-dsv2`
  - catalog-based `CREATE TABLE ... LOCATION ...` → engineInfo + property filtering
  - managed `CREATE TABLE` (no LOCATION) → engineInfo at resolved warehouse location
- NONE mode:
  - `CREATE TABLE` does **not** use the Kernel engineInfo marker
- Negative/idempotency:
  - `CREATE TABLE` fails when target exists
  - `CREATE TABLE IF NOT EXISTS` does not create `_delta_log/000...01.json`

Local commands:

```bash
build/sbt 'sparkV2/javafmtAll'
build/sbt 'spark/testOnly org.apache.spark.sql.delta.catalog.DeltaCatalogSuite'
```

---

### **Why we did *not* route through `stageCreate*` (yet)**

Spark’s `stageCreate*` APIs exist to support multi-step DDL that includes writes (CTAS/RTAS), where:
- Spark needs a `StagedTable` that can produce a write builder (`SupportsWrite`)
- Spark can then either commit staged changes or abort if the write fails

Today, the V2 `SparkTable` is **read-only** (no `SupportsWrite`), so routing CTAS/RTAS through V2
would either break or require a much larger change. Instead we:

- keep **Spark’s staging entry points** on the **V1** pipeline for now (so CTAS/RTAS keep working)
- still unify the Kernel create-table logic in a single staged-table implementation
  (`DeltaKernelStagedCreateTable`) that we can later extend with `SupportsWrite`

This gives us the “unified tunnel” building blocks without changing staging routing prematurely.

---

### **FAQ**

#### **Does this use a staged-table shape so CTAS/RTAS/LIKE are easier later?**
Yes. `DeltaKernelStagedCreateTable` implements Spark’s `StagedTable` and captures the resolved
state (schema/layout/properties/path + committer decision). It’s not wired to `stageCreate*` yet,
but it’s intentionally the “future landing zone” for `SupportsWrite`.

#### **Does UC-managed CREATE use the correct Kernel+UC create builder?**
Yes. If the UC table id property is present, we build the create transaction via
`UCCatalogManagedClient.buildCreateTableTransaction(...)` while keeping the `UCClient` open for
the entire transaction commit. This is the path that wires the CatalogCommitter (CCv2).

#### **Does V1 still use `CreateDeltaTableCommand`?**
Yes. Any case that falls back to `super.createTable(...)` (non-STRICT, non-Delta provider, or
future operations still routed to V1 staging) continues to use the existing V1 pipeline from
`AbstractDeltaCatalog`, which uses Delta’s V1 commands (including `CreateDeltaTableCommand`).

