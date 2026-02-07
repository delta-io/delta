## Native Delta Kernel metadata-only `CREATE TABLE` in Spark DSv2 (STRICT)

> Note: I wasn’t able to load the referenced Google Doc (auth-gated), so this document focuses on
> the **technical design and the exact flows** implemented in this branch. If you paste the
> instruction/template content, I can reformat this to match it precisely.

### Scope

This design describes how **metadata-only** `CREATE TABLE ... USING delta` is executed when the
Delta connector is configured in **DSv2 STRICT mode**, and how we intentionally keep CTAS/RTAS on
the existing DSv1 staging path.

- **Implemented (STRICT mode only)**:
  - Catalog-based `CREATE TABLE` (managed + external)
  - Path-based `CREATE TABLE delta.\`/abs/path\``
  - UC-managed commit coordination via **Kernel UC client** (CCv2) when UC table markers exist
- **Not implemented (still DSv1)**:
  - CTAS / RTAS / `stageCreate*` routing
  - `REPLACE TABLE` / `CREATE TABLE LIKE`

### Terminology

- **DSv1**: “legacy” Delta execution path in `spark/` (uses `DeltaLog`, `CreateDeltaTableCommand`, etc.).
- **DSv2 / Kernel**: “kernel-backed” path in `spark/v2/` (uses Kernel `TableManager`, Kernel `Transaction`).
- **STRICT mode**: `spark.databricks.delta.v2.enableMode = STRICT` (see `DeltaSQLConf.V2_ENABLE_MODE`).
- **Path-based table**: `CREATE TABLE delta.\`/abs/path\` ...` (identifier namespace `delta`, name is absolute path).
- **Catalog-based table**: `CREATE TABLE db.table ...` (identifier resolved in `SessionCatalog`).
- **UC-managed**: catalog properties include a UC table-id marker (e.g. `io.unitycatalog.tableId`).

---

### Configuration gate: how STRICT mode is detected

- **SQL conf**: `DeltaSQLConf.V2_ENABLE_MODE` (defined in `spark/src/main/scala/.../DeltaSQLConf.scala`)
  - Values: `AUTO`, `NONE`, `STRICT`
- **Centralized interpretation**: `org.apache.spark.sql.delta.DeltaV2Mode`
  - `shouldCatalogReturnV2Tables()` returns `true` only in `STRICT`

When you see:
- **`V2_ENABLE_MODE = STRICT`**

We respond with:
- **Route supported DDLs through Kernel-backed DSv2 implementation** (this document), otherwise fall back to DSv1.

---

## Before (DSv1): Spark SQL → DeltaCatalogV1/AbstractDeltaCatalog → `CreateDeltaTableCommand`

This is the pre-existing (and still used) DSv1 flow in `spark/src/main/scala/.../catalog/AbstractDeltaCatalog.scala`.

### DSv1 flow diagram (metadata-only `CREATE TABLE`)

Example SQL:

```sql
CREATE TABLE default.t1 (id INT, name STRING)
USING delta
LOCATION '/tmp/delta/t1';
```

High-level execution chain:

```
Spark SQL (parser/analyzer)
  -> V2CommandExec / CreateTableExec (Spark)
    -> TableCatalog.createTable(...)
      -> org.apache.spark.sql.delta.catalog.AbstractDeltaCatalog#createTable(...)
        -> AbstractDeltaCatalog#createDeltaTable(...)
          - filters Spark-internal props
          - converts transforms -> partition columns
          - decides tableType (MANAGED vs EXTERNAL)
          - builds CatalogTable descriptor
          - (optional) builds WriteIntoDelta for CTAS/RTAS
          -> org.apache.spark.sql.delta.commands.CreateDeltaTableCommand(...)
            -> commits delta log via DSv1 machinery (DeltaLog/OptimisticTransaction)
          -> loadTable(...)
```

Key DSv1 classes/methods:
- `org.apache.spark.sql.delta.catalog.AbstractDeltaCatalog#createTable`
- `org.apache.spark.sql.delta.catalog.AbstractDeltaCatalog#createDeltaTable` (private helper)
- `org.apache.spark.sql.delta.commands.CreateDeltaTableCommand`

### DSv1 flow diagram (CTAS / staged create)

Example SQL:

```sql
CREATE TABLE default.t2
USING delta
LOCATION '/tmp/delta/t2'
AS SELECT 1 AS id;
```

```
Spark SQL
  -> uses StagingTableCatalog APIs for CTAS/RTAS
    -> AbstractDeltaCatalog#stageCreate(...)
      -> StagedDeltaTableV2 (implements StagedTable with SupportsWrite)
        -> StagedDeltaTableV2#newWriteBuilder(...) captures write options + query
        -> StagedDeltaTableV2#commitStagedChanges()
          -> AbstractDeltaCatalog#createDeltaTable(..., sourceQuery = Some(df), ...)
            -> CreateDeltaTableCommand + WriteIntoDelta
```

Key DSv1 classes/methods:
- `AbstractDeltaCatalog#stageCreate`, `#stageReplace`, `#stageCreateOrReplace`
- inner class `StagedDeltaTableV2` (`StagedTable` + `SupportsWrite`)

---

## After (DSv2 STRICT): Spark SQL → unified `DeltaCatalog` override → Kernel commit

The unified catalog entrypoint is:
- `spark-unified/src/main/java/org/apache/spark/sql/delta/catalog/DeltaCatalog.java`

In STRICT mode, we intercept **only**:
- provider is Delta (`TableCatalog.PROP_PROVIDER` is `delta`)
- operation is **metadata-only** `CREATE TABLE` (Spark calls `createTable(...)`, not staging APIs)

### DSv2 STRICT routing logic (unified catalog)

`DeltaCatalog#createTable(...)` (Java) routing:

```
DeltaCatalog#createTable(ident, schema, partitions, properties)
  if !DeltaV2Mode.shouldCatalogReturnV2Tables():    // not STRICT
    -> super.createTable(...)                       // DSv1

  if provider != delta:
    -> super.createTable(...)                       // non-Delta

  if isPathIdentifier(ident):
    -> Kernel path-based commit (no catalog registration)
  else:
    -> Kernel commit + SessionCatalog registration
```

Key classes/methods:
- `org.apache.spark.sql.delta.catalog.DeltaCatalog#createTable` (Java override)
- `org.apache.spark.sql.delta.DeltaV2Mode#shouldCatalogReturnV2Tables`
- `org.apache.spark.sql.delta.catalog.SupportsPathIdentifier#isPathIdentifier` (from DSv1 base class)

---

## DSv2 STRICT: end-to-end commit pipeline (Kernel staged DDL table)

The DSv2 commit execution is centralized in:
- `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/DeltaKernelStagedDDLTable.java`

This class implements Spark’s `StagedTable` so it matches Spark’s DDL abstraction, even though
we currently construct/commit it directly from `DeltaCatalog#createTable(...)`.

### Common planned state

`DeltaKernelStagedDDLTable` constructor (planning phase) resolves:
- Hadoop conf + `Engine`: `DefaultEngine.create(spark.sessionState().newHadoopConf())`
- Schema conversion:
  - Spark `org.apache.spark.sql.types.StructType`
  - → Kernel `io.delta.kernel.types.StructType` via `SchemaUtils.convertSparkSchemaToKernelSchema`
- Partition layout:
  - Spark `Transform[]`
  - → Kernel `DataLayoutSpec` (identity transforms only)
- Property filtering:
  - Strips `location`, `provider`, `comment`, `option.path`, etc.
  - Normalizes UC table-id key old → new (`UCCommitCoordinatorClient.UC_TABLE_ID_KEY_OLD` → `UC_TABLE_ID_KEY`)
- UC resolution (optional):
  - If `filteredTableProperties` contains `UCCommitCoordinatorClient.UC_TABLE_ID_KEY`
  - Resolve `UCTableInfo` using `UCUtils.buildTableInfo(...)`

### Commit execution

`commitStagedChanges()`:
- Calls `commitKernelTransaction(CloseableIterable.emptyIterable())`
- “empty data actions” is the metadata-only contract

`commitKernelTransaction(dataActions)`:
- If UC info present → UC/CCv2 commit path
- Else → filesystem commit path
- Maps Kernel `io.delta.kernel.exceptions.TableAlreadyExistsException` to Spark’s
  `org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException` via `sneakyThrow`
- Runs optional `postCommitHook` after a successful commit (catalog registration for catalog-based tables)

---

## DSv2 STRICT flow: catalog-based tables (external + managed)

Example SQL (external):

```sql
CREATE TABLE default.ext_tbl (id INT)
USING delta
LOCATION '/tmp/delta/ext_tbl';
```

Example SQL (managed, no LOCATION):

```sql
CREATE TABLE default.managed_tbl (id INT)
USING delta;
```

### Catalog-based DSv2 STRICT diagram

```
Spark SQL
  -> TableCatalog.createTable(ident=Identifier.of(["default"], "ext_tbl"), ...)
    -> spark-unified DeltaCatalog#createTable(...)
      -> V2CreateTableHelper.buildCatalogTableSpec(...)
           input: SparkSession, Identifier, StructType, Transform[], Map<String,String>
           output: (CatalogTable tableDesc, String tablePath)
           - tableExists precheck (throws Spark TableAlreadyExistsException)
           - compute managed vs external based on location + PROP_IS_MANAGED_LOCATION
           - compute locationUri: explicit LOCATION or SessionCatalog.defaultTablePath(...)
           - build CatalogTable(provider="delta", storage.locationUri=...)
      -> new DeltaKernelStagedDDLTable(..., tablePath, postCommitHook = registerTable)
           -> commitStagedChanges()
             -> commitKernelTransaction(empty dataActions)
               -> (UC?) commitUCManagedCreate(...) else commitFilesystemCreate(...)
               -> postCommitHook.run()
                    -> V2CreateTableHelper.registerTable(...)
                         - EXTERNAL: SessionCatalog.createTable(tableDesc, ignore=false, validateLocation=true)
                         - MANAGED:  SessionCatalog.createTable(tableDesc, ignore=false, validateLocation=false)
      -> loadTable(ident)
```

Why `validateLocation=false` for managed tables:
- In this design, the Kernel commit happens **before** SessionCatalog registration.
- Spark’s SessionCatalog validates managed table locations must not already exist.
- But Kernel’s create-table commit must create `_delta_log/` under the table directory.
- Therefore, for managed tables we register with `validateLocation=false` to avoid rejecting the
  already-created directory.

### Example “payloads” (catalog-based)

Inputs from Spark into `DeltaCatalog#createTable(...)`:

```text
ident:
  namespace = ["default"]
  name      = "ext_tbl"

schema:
  StructType(id: int)

partitions:
  []   // or [IdentityTransform(FieldReference("p"))]

properties (representative):
  provider  -> "delta"                  (TableCatalog.PROP_PROVIDER)
  location  -> "file:/tmp/delta/ext_tbl" (TableCatalog.PROP_LOCATION)   // external only
  comment   -> "..."                    (optional)
  ... user table properties ...
```

Outputs of `V2CreateTableHelper.buildCatalogTableSpec(...)`:

```text
tableDesc:
  CatalogTable(
    identifier = TableIdentifier("ext_tbl", Some("default")),
    tableType  = EXTERNAL or MANAGED,
    storage.locationUri = Some(file:/tmp/delta/ext_tbl or warehouse default path),
    schema     = Spark StructType(...),
    provider   = Some("delta"),
    partitionColumnNames = Seq(...),
    properties = Map(filtered user props),
    comment    = Option(...)
  )

tablePath:
  "file:/tmp/delta/ext_tbl"   // string form of locationUri
```

---

## DSv2 STRICT flow: path-based external tables

Example SQL:

```sql
CREATE TABLE delta.`/tmp/delta/path_tbl` (id INT)
USING delta;
```

### Path-based DSv2 STRICT diagram

```
Spark SQL
  -> TableCatalog.createTable(ident=Identifier.of(["delta"], "/tmp/delta/path_tbl"), ...)
    -> spark-unified DeltaCatalog#createTable(...)
      -> isPathIdentifier(ident) == true
      -> new DeltaKernelStagedDDLTable(..., tablePath = ident.name(), postCommitHook = null)
          -> commitStagedChanges()
      -> loadTable(ident)
```

Key difference vs catalog-based:
- There is **no** `SessionCatalog` registration step.
- The identifier is purely a “path identifier” and the commit writes `_delta_log/` directly under the path.

---

## DSv2 STRICT flow: UC-managed tables (CCv2)

UC-managed behavior is selected by **presence of UC table-id marker** in table properties:
- `io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY`
  - new key (example: `io.unitycatalog.tableId`)
- Legacy key is normalized:
  - `UC_TABLE_ID_KEY_OLD` → `UC_TABLE_ID_KEY`

### UC-managed DSv2 STRICT diagram (catalog-based)

```
DeltaCatalog#createTable(...)
  -> DeltaKernelStagedDDLTable planned state includes ucTableInfoOpt
     (resolved via UCUtils.buildTableInfo(tableId, tablePath, spark, catalogName))

DeltaKernelStagedDDLTable#commitKernelTransaction(empty actions)
  -> commitUCManagedCreate(tableInfo, empty actions)
     -> new UCTokenBasedRestClient(tableInfo.getUcUri(), TokenProvider.create(authConfig))
     -> new UCCatalogManagedClient(ucClient)
     -> ucCatalogClient.buildCreateTableTransaction(
          tableId, tablePath, kernelSchema, ENGINE_INFO)
     -> commit(builder, empty actions)
          - builder.withTableProperties(filteredTableProperties)
          - builder.withDataLayoutSpec(optional)
          - txn = builder.build(engine)
          - txn.commit(engine, empty actions)
  -> postCommitHook registers table in SessionCatalog (same as non-UC catalog-based flow)
```

Example UC-related properties (representative):

```text
properties:
  provider                  -> "delta"
  io.unitycatalog.tableId   -> "<uuid>"     // UC_TABLE_ID_KEY
  spark.sql.catalog.<cat>... -> ...         // UC auth/endpoint resolved from Spark configs
  ...
```

Important invariant:
- For UC-managed tables, **Unity Catalog remains the source of truth** for table identity/metadata.
- The Delta log `commitInfo.engineInfo` is treated as a **diagnostic/provenance signal** for testing
  and debugging (see tests below).

---

## Provenance / verification signal: `engineInfo`

Kernel commits stamp:
- `engineInfo` ends with `io.delta.spark.internal.v2.catalog.DeltaKernelStagedDDLTable.ENGINE_INFO`
- current constant value: `kernel-spark-dsv2`

The E2E tests validate that `commitInfo.engineInfo` in `00000000000000000000.json`:
- **is Kernel** for STRICT metadata-only CREATE TABLE
- **is not Kernel** for STRICT CTAS (falls back to DSv1 staged path)

---

## Tests added (E2E)

Test suite:
- `spark-unified/src/test/scala/org/apache/spark/sql/delta/catalog/DeltaCatalogSuite.scala`

What it asserts (STRICT mode):
- Catalog-based external `CREATE TABLE ... LOCATION ...` commits via Kernel (`engineInfo`)
- Path-based `CREATE TABLE delta.\`/abs/path\`` commits via Kernel
- CTAS falls back to DSv1 (engineInfo is not kernel)
- Managed `CREATE TABLE` (no LOCATION) commits via Kernel and registers successfully

---

## Appendix: key implementation files

- STRICT routing:
  - `spark-unified/src/main/java/org/apache/spark/sql/delta/catalog/DeltaCatalog.java`
- CatalogTable planning + registration (Java, by request):
  - `spark-unified/src/main/java/org/apache/spark/sql/delta/catalog/V2CreateTableHelper.java`
- Kernel staged DDL table:
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/catalog/DeltaKernelStagedDDLTable.java`
- UC config resolution:
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/snapshot/unitycatalog/UCUtils.java`
  - `spark/v2/src/main/java/io/delta/spark/internal/v2/snapshot/unitycatalog/UCTableInfo.java`

