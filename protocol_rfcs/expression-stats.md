# Expression Statistics
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/8247**

This protocol change adds the writer table feature `expressionStats-dev`. It lets a table keep
per-file minimum, maximum and null-count statistics for expressions, such as `upper(name)`, without
storing their values as columns, so readers can skip files for predicates over an expression, as
they do with per-column statistics.

The change has three parts:

- a registry in the `delta.expressionStats` metadata domain, listing the expressions the table keeps
  statistics for, each under a random registration ID;
- per-file entries in the file statistics, keyed by registration ID;
- reader and writer requirements for these entries, including where a table that supports
  `adaptiveMetadata` keeps them.

The protocol defines no function, because the same function can compute different results in
different engines. Even `upper` has inputs that a query doesn't show: the Unicode version its
library follows and, in some engines, a configuration setting that chooses the library. Newer
Unicode versions give some letters an uppercase form they lacked, so two engines, or one engine
before and after an upgrade, can return different results for the same string. A registration makes
every such input explicit. Its function name identifies exactly one behavior: what the function
computes for every value of its arguments. Any input that selects among behaviors, such as the
Unicode version or a session's time zone, is part of the function name or one of the registration's
literal arguments. A function that Iceberg's specifications define, such as `truncate`, is named by
its Iceberg function reference. An implementation should name any other function in a catalog in
which only its owner defines names, as Iceberg's expressions spec lets engines document a catalog
name for their built-in functions. A reader uses an entry only if it evaluates the predicate's
function with the behavior that the entry's registration identifies. Every entry must describe that
behavior's results. As long as implementations name behaviors by that rule, implementations that
compute a function differently never use each other's entries.

## Motivation

Per-column statistics bound a column's values, not an expression's values over it, so filters on
expressions can't use them for data skipping. Take the predicate `upper(name) = 'ZOE'` and a file
whose `name` values are `active` and `Inactive`. In binary order, the bounds of `name` are
`Inactive` and `active`, and a value such as `Zoe`, whose upper case is `ZOE`, sorts between them,
so they cannot rule the file out. The bounds of `upper(name)` are `ACTIVE` and `INACTIVE`. Since
`ZOE` is above the maximum, a reader with these bounds could skip the file. Typical expressions in
such filters compute case-insensitive keys, numbers or dates parsed from strings, and geospatial
cell IDs.

Today such expressions get statistics in two ways, and neither is enough. A generated column changes
the schema, stores the expression's result in every row, and keeps the expression as a SQL string in
a dialect that the protocol does not specify. Statistics that one engine keeps privately are not
durable, because other writers can drop them when they re-add a file, and other engines can't use
them. This RFC makes statistics for expressions part of the protocol, so that writers that can't
compute them still keep them when they re-add a file unchanged or write a checkpoint that keeps file
statistics. Every reader that computes an expression with the behavior its registration identifies
can use them, under the conditions of [Reader
Requirements](#reader-requirements-for-expression-statistics).

## Feature summary

- Table feature name: `expressionStats-dev`. The `-dev` suffix, which `protocol_rfcs/README.md`
  recommends, marks the feature as experimental until the RFC is accepted; it is then dropped. The
  suffix matters because the format may still change.
- Readers and writers, or writers only: writers only. A reader that ignores the domain and the
  entries returns the same results. The feature adds no action type, data file or column, and
  "Clients must ignore such unrecognized fields" ([Protocol Evolution][protocol-evolution]). The
  reader requirements bind only readers that choose to use entries, so the domain imposes no
  requirement on readers ([Domain Metadata][domain-metadata]), as the reader requirements of [Row
  Tracking][row-tracking] apply only when row IDs or row commit versions are requested. The writer
  feature keeps entries durable. On a re-add, statistics from the latest version should replace
  earlier ones ([Add File and Remove File][add-file-and-remove-file]), so a writer that doesn't
  implement the feature could drop entries that other writers collected.
- Minimum protocol versions: writer version 7. The reader version is unchanged.
- Required features: `domainMetadata`.
- Features it can't be used with: `icebergWriterCompatV1` and `icebergWriterCompatV3`, until
  separate changes add this feature to their allowlists; and the proposed `icebergNativeV4`
  ([delta-io/delta#7374][iceberg-native-v4]), until its allowlist includes this feature.
- Features with rules of their own: `adaptiveMetadata`. On a table that supports both, the entries
  of files in its metadata tree are kept in `content_stats`, in the stats structs for the field IDs
  to which the registry maps their entry keys (see Expression Statistics with Adaptive Metadata).
- Supported vs enabled: there is no table property. When the feature is supported, it is active.
- Table properties: none.

## Compatibility

| Case | Outcome |
|-|-|
| Old reader, table with this feature | Works correctly for a reader that supports the rest of the table's protocol, because it ignores the domain and the entries. Typed checkpoint fields use only result types that the table's protocol allows, so a reader without, for example, `timestampNtz` already fails cleanly at the protocol check. |
| Old writer, table with this feature | Fails cleanly: the writer feature check rejects it. |
| New reader, table without this feature | Works correctly. Readers use entries only on tables that support the feature. |
| New writer, table without this feature | Works correctly. It may add the feature in any commit, unless the table uses a feature listed in the Feature summary as one it can't be used with. |
| Existing table that enables the feature (mix of old and new files) | Works correctly. Files without entries are read as before, and readers ignore any content of a `stats.expressionStats` member that doesn't have the form Per-file Entries defines. |
| Checkpoint writer that doesn't know the feature | Fails cleanly: the writer feature keeps it from writing the table. |
| Reader that doesn't implement this feature, table that also supports `adaptiveMetadata` | Works correctly: it ignores the stats structs in `content_stats` for field IDs that no column has, as [Protocol Evolution][protocol-evolution] requires clients to ignore unrecognized fields. |
| Writer that changes column values in valid rows without rewriting the file, as the proposed [Column Updates RFC][column-updates] allows | Works correctly: such a re-add is not unchanged, so it may drop the source's entries (Writer Requirements). |

**Delta Kernel.** For acceptance, `protocol_rfcs/README.md` asks for at least some discussion,
ideally a prototype, of the feature's feasibility in Delta Kernel. Kernel leaves expression
evaluation to the engine that uses it, so a reader built on Kernel may ignore the registry and the
entries. To use an entry, it checks the conditions of [Reader
Requirements](#reader-requirements-for-expression-statistics). In that check, the implementation
that evaluates the function is the engine that uses Kernel, and that engine must evaluate the
predicate's function with the behavior that the registration's function name identifies. Kernel's
data skipping today handles predicates that compare a column with a literal, so using entries needs
a new skipping path for predicates that apply a function to columns.

No Writer Requirement needs a writer to evaluate a registration's function. A writer that can't
evaluate it keeps still-valid entries on unchanged re-adds, in checkpoints and in log compaction
files. It sets `stats.tightBounds` to `false` after a delete through a deletion vector and, on a
table that also supports `adaptiveMetadata`, follows [Expression Statistics with Adaptive
Metadata](#expression-statistics-with-adaptive-metadata). Computing entries is only recommended, for
a writer's own registrations.

## Interactions with other features

| Feature | Interaction |
|-|-|
| [Column Mapping][column-mapping] | Column arguments are ID references to a column's `delta.columnMapping.id` in modes `id` and `name`, as Iceberg's stored expressions reference columns by field ID. In mode `none`, they are name references (see Registry). In modes `id` and `name`, a rename keeps registrations valid, but a dropped and re-added column, which gets a new `delta.columnMapping.id`, makes them stale. Enabling column mapping makes registrations with name references stale. On `adaptiveMetadata` tables, entry keys' field IDs come from `delta.columnMapping.maxColumnId`. |
| [Deletion Vectors][deletion-vectors] | Entries follow `stats.tightBounds`, as per-column statistics do (see Per-file Entries). A re-add that makes a deleted row valid again is not unchanged, so it may drop the source's entries. |
| [Checkpoints][checkpoints] (classic, multi-part, [V2][v2-checkpoint], sidecars) | Still-valid entries are kept in `stats`, and in `stats_parsed` when the checkpoint writes that field, typed by each registration's `resultType` (see Per-file Entries and Writer Requirements). On a table that supports `adaptiveMetadata`, writers write none of these checkpoints except that feature's standalone checkpoints (see Expression Statistics with Adaptive Metadata). |
| [Log Compaction Files][log-compaction-files] | Compaction files keep the `expressionStats` member of each `add` unchanged. |
| [Metadata Cleanup][metadata-cleanup] and VACUUM | None: the feature adds no files. |
| [VACUUM Protocol Check][vacuum-protocol-check] | None: the feature adds no files, and the check covers this writer feature like any other. |
| [Version Checksum File][version-checksum-file] (`.crc`) | None: the registry is a `domainMetadata` action, and entries are part of `add` statistics, which the existing checksum rules cover. |
| [Action Reconciliation][action-reconciliation] | None beyond domain reconciliation: the latest registry wins, and entries follow their `add`. |
| [Row Tracking][row-tracking] | None: a re-add that only publishes entries follows Row Tracking's rules for recommitted `add` actions. Materialized row tracking columns aren't in the schema, so they can't be arguments. |
| [Change Data Feed][change-data-feed] | Change data files have no statistics. A re-add that only publishes entries sets `dataChange` to `false` (see Writer Requirements). This RFC doesn't change how change data readers treat such a re-add. The rule for reading changes from a version without `cdc` actions has no `dataChange` condition. That gap affects statistics-only re-adds and is outside this RFC. |
| [Append-only Tables][append-only-tables] | None: a re-add that only publishes entries changes no data and sets `dataChange` to `false` (see Writer Requirements), which append-only tables allow. |
| [Domain Metadata][domain-metadata] | The registry is the system-controlled domain `delta.expressionStats`, which writers that don't implement the feature must not modify. |
| [In-Commit Timestamps][in-commit-timestamps] | None: the feature adds no field to `commitInfo`. |
| [Type Widening][type-widening] | Widening an argument column changes its type, so the registration becomes stale. |
| [Generated Columns][generated-columns], [`CHECK` Constraints][check-constraints], [Column Invariants][column-invariants], [Default Columns][default-columns], [Identity Columns][identity-columns] | None: entries only describe values, and registrations don't constrain them. |
| [Clustered Table][clustered-table] and partitioning | A clustering rewrite keeps no entries; the writer collects new ones for its own registrations, as Writer Requirements recommend. Partition columns can be arguments; each row's value is the partition value. |
| [Per-file Statistics][per-file-statistics] and data skipping | Entries hold the statistics that Per-file Statistics defines for a column of the result type, with the same encoding, tightness and reader handling (see Per-file Entries). |
| [Iceberg Compatibility V1][iceberg-compat-v1] and [V2][iceberg-compat-v2], [IcebergCompatV3][iceberg-compat-v3] (proposed), [IcebergWriterCompatV1][iceberg-writer-compat-v1] (proposed), `icebergWriterCompatV3` (no published RFC yet; its allowlist is in the [Delta Kernel source][kernel-iceberg-writer-compat-v3]) | IcebergCompatV1, V2 and V3 don't exclude this feature. IcebergWriterCompatV1 and `icebergWriterCompatV3` allow only the features on their allowlists, and this one isn't on them yet. Entries aren't keyed by column, so a conversion that maps per-column statistics to Iceberg has no column to map them to. |
| [Catalog-managed Tables][catalog-managed-tables] | None: registry updates are ordinary commits. |
| CLONE and RESTORE | A clone or RESTORE that copies registrations copies them with their IDs, except where Removing the feature forbids it. Restored and cloned `add` actions keep their still-valid entries, except those whose field ID mapping a clone can't keep (see [Expression Statistics with Adaptive Metadata](#expression-statistics-with-adaptive-metadata)). |
| [Collated String Type][collated-string-type] (proposed), [Variant Data Type][variant-data-type], [Timestamp without Timezone][timestamp-ntz], other types | A collation change makes a registration stale. Entries are separate from the Collated String Type RFC's per-column `statsWithCollation`, and a collation library version that changes results is another behavior. A variant column can be an argument, and `variant` can be a result type. An entry with a variant result can have the statistics that Per-file Statistics defines for a variant column: a null count and, where the table enables variant shredding, per-path bounds. A result type must be one the table's protocol allows, such as `timestamp_ntz` only with `timestampNtz`, and must be primitive on a table that also supports `adaptiveMetadata` (see Registry). |
| [Iceberg V4 Adaptive Metadata Tree][adaptive-metadata] (`adaptiveMetadata`, proposed) | Supported together if that RFC allows stats structs for the field IDs that the registry maps. It gives them only to leaf columns today ("Only leaf (primitive) columns get stats structs"). Each entry key gets a field ID from `delta.columnMapping.maxColumnId`, and `content_stats` carries the entry as if it were a column's statistics, under that RFC's rules for per-column statistics, including its `tightStats` flag, which marks a file's per-column statistics as tight or wide (see Expression Statistics with Adaptive Metadata). |
| [Column Updates][column-updates] (proposed) | A re-add that changes values in valid rows is not unchanged, so it may drop the source's entries (Writer Requirements). |
| [Materialize Partition Columns][materialize-partition-columns] | None: an entry over a partition column uses the file's partition value, which that feature says readers should keep reading from the `add` action. Its rules that partition columns get no file-level statistics concern the column's own statistics, not entries over it. |
| [Checkpoint Protection][checkpoint-protection] (proposed) | None beyond that feature's rule: a writer that it allows to checkpoint an earlier version must support that version's protocol, so it follows this feature's checkpoint requirements when that protocol includes Expression Statistics. |
| [Column Mapping Usage Tracking][column-mapping-usage-tracking] (proposed) | None beyond the Column Mapping row: that feature's rule that a new column's physical name contain a UUID, once a column has been dropped or renamed, doesn't affect registrations, whose column arguments are ID references in modes `id` and `name`. |
| [Concurrent Identity Columns][concurrent-identity-columns] (proposed) | None: it changes how identity values are allocated, not what an entry means. Identity columns can be arguments. |
| [Interval Types][interval-types], [Nanosecond Timestamps][nanosecond-timestamps], [User-Defined Types][user-defined-types], [File Data Type][file-type] (all proposed) | Interval and nanosecond timestamp columns can be arguments. Results of these types get the statistics that Per-file Statistics, as those RFCs change it, allows for such a column, and an interval result gets only a null count. The Interval Types RFC bans bounds and data skipping for interval columns and keeps their null counts "since they do not require interpreting interval values". Entries over interval arguments with other result types are outside that ban, because a behavior fixes how the function interprets interval values. Reader Requirements apply that RFC's rule on interval partition values. User-defined type and `file` columns can be neither arguments nor results, as their types are neither primitive nor `variant`. |
| [Geospatial Types][geospatial-types] (proposed in delta-io/delta#8240) | None yet: the protocol defines no geospatial type. Once it does, the argument and result-type rules apply as for any other type. |
| [Clustering by Expressions][clustering-expressions] (proposed in delta-io/delta#6737) | Conflicts as written: that RFC also writes statistics into a member named `expressionStats` of `stats`, but as members keyed by its own signatures. This feature's readers ignore those members, and its writers may drop them (see Per-file Entries). The two RFCs need to agree, for example by building that feature's statistics on this feature's entries. |
| [Iceberg Native V4][iceberg-native-v4] (proposed in delta-io/delta#7374) | Can't be used together yet: that RFC rejects enablement while a writer feature outside its allowlist is present, and the allowlist doesn't include this feature. |
| [Parquet Encryption][parquet-encryption] (proposed in delta-io/delta#7470) | That RFC forbids per-file statistics for encrypted columns, and column statistics in Delta metadata files under uniform encryption (all columns encrypted with the footer key), because they may reveal sensitive information. This RFC extends that rule to entries: on a table with both features, writers must not publish an entry whose registration has a column argument that is encrypted in the entry's data file, nor any entry for a data file written with uniform encryption (see Writer Requirements). |
| [Invisible Columns][invisible-columns] (proposed in delta-io/delta#7732) | On a table with that feature, column arguments resolve against that RFC's complete data schema, which merges the visible and invisible schemas, so they can name invisible columns. Those columns get per-column statistics under the normal rules, and entries over them follow this RFC's rules. |
| Float16 (delta-io/delta#6557), unsigned integer (#7106), DECFLOAT (#7722) and parameterized-precision timestamp (#7135) types (all proposed) | Each adds primitive types, so a column of one can be an argument, and the type can be a result type where the table's protocol allows it. An entry gets the statistics that Per-file Statistics allows for a column of the type, unless the type's RFC limits them (DECFLOAT allows only a null count). A literal of one of these types has no encoding until the encoding table in [Registry](#registry) lists it, so a registration with such a literal is stale. |

## Concurrency

Two overlapping transactions that both write the `delta.expressionStats` domain conflict ([Domain
Metadata][domain-metadata]), so registry updates are never lost. A writer that adds a registration
equal to an existing one (see [Writer Requirements](#writer-requirements-for-expression-statistics))
creates a duplicate with another ID. Both are valid, and a reader can use the entries of either, so
the duplicate costs only space and the work of computing its entries. A registration identifies one
behavior, so entries for a behavior that no registration identifies yet need a new registration.
This happens, for example, after an upgrade that changes a writer's behavior. The writer that
publishes those entries can add the registration in the same commit. The registry changes whenever
writers start publishing entries for a new behavior, not only when the table gets a new expression.
If a writer publishes entries while another commit removes their registration or makes it stale,
readers ignore those entries. Registration IDs are 8 random bytes, so two writers choose the same ID
only with negligible probability, and no writer needs to reserve one. A re-add that publishes
entries is checked for conflicts like any other re-add of a file.

On a table that also supports `adaptiveMetadata`, the commit that publishes the first entries under
a new entry key also raises `delta.columnMapping.maxColumnId`, which changes the `metaData` action.
Two overlapping transactions that both change that property conflict, as [Expression Statistics with
Adaptive Metadata](#expression-statistics-with-adaptive-metadata) requires. This holds whether each
is a log commit or a manifest commit as the `adaptiveMetadata` RFC defines them, even where that RFC
lets a manifest commit incorporate an overlapping log commit. Otherwise both would assign field IDs
above the same value of that property, and the conflict keeps a field ID from being assigned twice.
Entry keys get field IDs only when writers first publish entries under a registration, or when a
table with entries starts to support `adaptiveMetadata`. The protocol doesn't require a transaction
that changes no metadata to fail because an overlapping commit changed only
`delta.columnMapping.maxColumnId`, so an implementation can let it commit.

## Alternatives considered

- Statistics under a function name that doesn't identify one behavior, such as an `upper` that each
  engine uses for its own `upper`. Readers would return wrong results whenever two engines, or two
  versions of one engine, compute the function differently.
- Leaving the choice of function names entirely to implementations. Two implementations that each
  named their own `upper` function `upper` would share a name for different behaviors, and their
  readers would use each other's entries. A catalog in which only one owner defines names, as
  [Function Names](#function-names) recommends, avoids that without any coordination.
- Requiring a published definition for every function name. Every implementation would then have to
  publish its behaviors. Readers still couldn't rely on a definition, because nothing checks that an
  implementation computes what its definition says. An owner that wants to publish the definitions
  of its catalog's functions can do so outside the table.
- Defining each function in the protocol. This is impossible where the behavior depends on a
  library, or on rules that only an implementation defines. Where Iceberg's specifications already
  define a function, such as `truncate`, the protocol refers to that definition through the
  function's Iceberg function reference (see [Function Names](#function-names)) instead.
- Naming the functions that Iceberg's specifications define in each implementation's own catalog.
  Implementations that evaluate such a function identically would then use each other's entries only
  if they agreed on a name outside the protocol.
- Requiring or recommending that writers publish entries for every currently valid registration that
  they implement, so that writers with different settings converge on the registered behaviors. A
  registration can give a setting, such as a time zone, as a literal argument, so that every writer
  that implements such a registration can compute its entries. Each such registration would then
  cost each of those writers one more evaluation of the function per row. Writers are instead asked
  to publish entries for their own registrations, and may publish others (see [Writer
  Requirements](#writer-requirements-for-expression-statistics)). The cost is that a reader uses
  only the entries of writers whose settings and release give the same registrations as its own,
  unless a writer chooses to compute others.
- Generated columns. They work today, but change the schema and store the expression's result in
  every row.
- No table feature. The protocol rules it out: "Any system-controlled domain that imposes additional
  requirements on the writer is a breaking change, and must be part of a writer table feature"
  ([Writer Requirements for Domain Metadata][writer-requirements-for-domain-metadata]). Without one,
  writers that don't know entries would also drop them on re-adds. Nothing would stop a writer from
  keeping them while changing a file's values, for example through a re-add that makes a deleted row
  valid again, which would give wrong results.
- A reader-writer feature. It would lock out readers that merely don't use entries, and gain
  nothing, because the reader requirements bind only readers that use entries.
- Merging the input files' entries when files are rewritten. Every compacting writer would need a
  merge algorithm. Entries of behaviors that no writer computes any more would stay forever, and
  bounds would be wide after clustering.
- Keeping entries on every re-add that keeps the data file. This is unsafe when the re-add makes
  valid a row that the existing bounds don't cover.
- Keeping a registration valid when [Type Widening][type-widening] widens a column argument. Readers
  convert the values of files written before the change to the wider type, which can change a
  function's result. The shortest round-trip decimal string of the `float` nearest 0.1 is `0.1`, and
  that of the same value as a `double` is `0.10000000149011612`. Whether results change depends on
  the behavior, which the protocol doesn't define, so the registration becomes stale. A later
  revision can relax this rule safely, as long as such a registration keeps its recorded type, which
  writers can't change. Readers built to this specification would still treat it as stale and not
  use its entries.
- Reading entries as wide whatever `tightBounds` says. Under the one tightness flag per file that
  [apache/iceberg#18585][iceberg-tight-stats] adopted for Iceberg format version 4, every file with
  entries would then have to be marked as not tight.
- Literal values in function names, such as `h3_cell_7`. Every value would need its own name, and
  nothing would say which part of a name is a value.
- A JSON form of this RFC's own for the expression, mapped to Iceberg's. An implementation that
  maintains Iceberg metadata would need the mapping in both directions, and the mapping can't be
  one-to-one, because Delta has integer types that Iceberg lacks. With Iceberg's serialization, in
  column mapping modes `id` and `name`, a writer that maintains Iceberg metadata can carry the
  references and literals of `expr` into a stats-only field's expression unchanged. Only the
  registration ID, the recorded types and collations and the result type stay outside `expr`.
- Forbidding a table to support both Expression Statistics and `adaptiveMetadata`. No table that
  adopts `adaptiveMetadata` could keep expression statistics.
- Leaving the mapping of entries to `adaptiveMetadata`. Only this feature knows which entry keys
  exist and when a reader may use an entry.
- A separate counter for field IDs in the registry, in a range that column IDs never use. Assigning
  a field ID would then update only the domain, not the metadata. But Column Mapping would have to
  keep column IDs out of that range. The approach would also differ from the [File Data Type
  RFC][file-type], which takes the field IDs of a file column's sub-fields from
  `delta.columnMapping.maxColumnId`, and from Iceberg's [stats-only fields][iceberg-stats-fields],
  whose IDs come from the table's `last-column-id`. The cost of using `maxColumnId`, a metadata
  change in the first commit under each new entry key, is small (see Concurrency).
- Keeping the registry when the feature is removed, so that entries survive a removal. While the
  feature is removed, no writer is bound by its rules for entries, so an entry can become untrue,
  for example through a re-add that makes a deleted row valid again. Readers would use that entry
  once the feature is added back. An enablement epoch, recorded in the registry and in each entry,
  would keep such entries out at a higher cost. Removing the registry follows the precedent of [Type
  Widening][type-widening], whose removal also requires cleaning up the feature's metadata.
- Declaring each entry key as an Iceberg [stats-only field][iceberg-stats-fields] now, so that
  Iceberg writers keep the entries on a table that is also maintained as an Iceberg table. No table
  with this feature could carry such a field yet. This RFC gives entry keys field IDs only on tables
  that support `adaptiveMetadata`, whose RFC gives Iceberg engines no access to the table, and
  `icebergNativeV4` can't be used together with this feature yet. A later revision can add the
  declaration once a table with this feature can carry such a field. Its expression would be the
  registration's `expr`, unchanged, because the function name already identifies the entry's
  behavior.

## Open questions

None.

--------

> ***Add a new section after the [Type Widening][type-widening] section.***

# Expression Statistics

The Expression Statistics feature lets a table keep per-file statistics on expressions, such as
`upper(name)`. Writers register the expressions they keep statistics for in the table's registry,
and files store a minimum, a maximum and a null count of their results. Readers can use these
statistics to skip files, as they use the per-column statistics of [Per-file
Statistics][per-file-statistics]. The same function can compute different results in different
implementations. For example, the result of `upper` depends on the version of the Unicode standard
that the implementing library follows, because newer versions give some letters an uppercase form.
So a registration's function name identifies one behavior: what the function computes for every
value of its arguments. A reader uses only the entries of registrations whose behavior is the one
with which it evaluates the predicate's function. Whether a reader can use an entry depends on the
predicate as the reader evaluates it, after any rewrite the reader applies. [Reader
Requirements](#reader-requirements-for-expression-statistics) give the conditions.

The key words MUST, MUST NOT, SHOULD and MAY in this section are to be interpreted as described in
[BCP 14][bcp14] when, and only when, they appear in all capitals.

## Enablement

To support this feature:
- The table MUST be on Writer Version 7.
- The feature `expressionStats-dev` MUST exist in the table `protocol`'s `writerFeatures`.
- The feature `domainMetadata` MUST exist in the table `protocol`'s `writerFeatures`.

The feature has no table property: when it is supported, it is active. A writer MAY add the feature
in any commit, including the commit that adds the first registration, unless [Compatibility with
other features](#compatibility-with-other-features) forbids it. A writer MUST NOT include a
`domainMetadata` action for the `delta.expressionStats` domain whose `removed` field is `false` in a
commit after which the table does not support the feature. Writers MAY leave the files already in
the table without entries.

## Terminology

- An **expression** is a function applied to specific columns, and possibly to literal values, such
  as `upper(name)` or `substring(name, 1, 3)`. A registration records each literal value, such as a
  format string or a resolution, as a **literal argument**: an argument whose value the registration
  records with its type, instead of reading it from each row (see [Registry](#registry)). A function
  can also be a composition of functions, such as
  `cast(variant_get(v, '$.ts', 'string') AS timestamp)`. Such a composition is registered under one
  function name, with the composition's column and literal arguments in order (here `v`, `'$.ts'`
  and `'string'`), plus any literal argument that the function's definition adds for a setting that
  the composition depends on, such as the time zone that the cast uses. Its behavior is then that of
  the whole composition.
- A **registration** names one expression in the table's registry, under a **registration ID**.
- A **behavior** of a function is the full mapping, for a row, from the values of its arguments
  (column values and literal values alike) to the function's result for that row, including NULL
  handling and errors. For a given behavior, the result depends only on those values, so one
  behavior covers every literal value. A setting that changes a function's results, such as a
  session's time zone, selects another behavior, unless the function takes the setting as a literal
  argument. A function whose result for a row can change between evaluations with the same argument
  values, such as one that reads the current time, has no behavior in this sense. So no function
  name identifies it, and no entry describes it.
- A **function name** is the function reference in a registration's expression (see
  [Registry](#registry)). It identifies one behavior, so it fixes every setting that changes the
  function's results, unless the function takes the setting as a literal argument (see [Function
  Names](#function-names)). This specification defines no function: the behavior that a function
  name identifies is the one that Iceberg's specifications give a function of the reserved catalog
  `iceberg_functions`, and otherwise the one that the name's **owner**, the one party that defines
  names in the name's catalog, gives it.
- An implementation **implements** a registration if it evaluates the registration's expression, for
  every value of its column arguments (see [Registry](#registry)), with the behavior that the
  registration's function name
  identifies, applied to those values and to the values of the registration's literal arguments.
- A registration is one of an implementation's **own registrations** under given settings if the
  implementation implements it, and every setting or version that the registration fixes, such as a
  time zone or a Unicode version, has the value that the implementation uses under those settings
  when a query applies the function without stating that value: the session's time zone for a cast,
  or the Unicode version that the implementation's `upper` follows. A registration fixes a setting
  or a version through its function name, or through a literal argument that, by the definition of
  its function name, stands for it. Own registrations depend on the implementation and its settings,
  not on who owns the function name. Under a session that uses UTC, for instance, a registration of
  a cast whose time zone argument is `UTC` is one of the implementation's own registrations. One
  whose time zone argument is `America/Los_Angeles` isn't, although the implementation may implement
  both. A registration that fixes no setting or version, such as one of
  `iceberg_functions.truncate`, is one of the own registrations of every implementation that
  implements it.
- An **entry** is one file's minimum, maximum and null count, each optional, for one registration.
- An **entry key** names an entry in a file's statistics: the ID of its registration (see [Per-file
  Entries](#per-file-entries)).
- A **valid row** of a logical file is a row of its data file that its deletion vector, if any, does
  not delete.
- A **re-add** is an `add` action for a data file that the table contains or once contained, at the
  same path and without rewriting the file, for example to change its deletion vector or its
  statistics, or to restore it.
- The **default collation** of a string is the collation it has when the schema records none, which
  compares strings by the binary order of their UTF-8 encoding (see the [Collated String Type
  RFC][collated-string-type]).
- The **statistics population** of an entry is the set of rows whose results the entry describes. It
  contains every valid row of the logical file, and it can also contain rows that the file's
  deletion vector deletes.
- A **still-valid entry** is an entry whose registration is in the registry and valid: for a reader,
  in the snapshot it reads, and for a writer, in its current snapshot (see [Registry](#registry)).

A registration pins every input that changes an expression's results. The protocol defines its
column arguments, with the types and collations that it records, and its literal arguments, and
checks the column arguments against the schema. Any other input, such as a library version or a
session's time zone, is either fixed by the definition that the function name identifies or given as
a literal argument.

Three actions use these terms:

- **Registering an expression:** a writer adds a registration to the registry (see
  [Registry](#registry)). Its function name identifies the expression's behavior; registering
  computes no entry and changes no data file.
- **Collecting statistics for a registration:** a writer that implements the registration, when it
  writes or re-adds a data file, computes an entry for the registration and publishes it under the
  registration's ID (see [Per-file Entries](#per-file-entries)). [Writer
  Requirements](#writer-requirements-for-expression-statistics) recommend this for a writer's own
  registrations and allow it for the others it implements.
- **Using entries:** a reader skips files, or computes a result, from the entries of a registration
  whose expression the predicate computes as the reader evaluates it (see [Reader
  Requirements](#reader-requirements-for-expression-statistics)).

A writer can collect, and a reader can use, a registration's entries only if it implements the
registration.

## Schema

The feature adds two objects. The **registry** is the JSON object that the `configuration` string of
the [domain metadata][domain-metadata] action for the domain `delta.expressionStats` serializes. The
`expressionStats` object is part of a file's statistics, and [Per-file Entries](#per-file-entries)
defines it together with its form in checkpoints. The registry has the following fields:

Field Name | Data Type | Description | optional/required
-|-|-|-
expressions | Array of registrations | The table's registrations, which [Registry](#registry) defines. | optional
fieldIds | Map[String, Long] | Maps entry keys to field IDs, on a table that also supports `adaptiveMetadata` (see [Expression Statistics with Adaptive Metadata](#expression-statistics-with-adaptive-metadata)). | optional

A table has no registrations in any of these cases:

- it has no `delta.expressionStats` domain;
- the domain's `configuration` is not a JSON object;
- the registry has no field `expressions`, or that field is not an array.

A missing `fieldIds` field means an empty map.

## Registry

The registry's field `expressions` holds the table's registrations (see [Schema](#schema)).

A registration has the following fields:

Field Name | Data Type | Description | optional/required
-|-|-|-
id | String | The registration ID: 8 random bytes, written as 16 lowercase hexadecimal characters. | required
expr | Object | The expression: an apply expression in the [JSON serialization][iceberg-expressions-json] of [Iceberg's expressions spec][iceberg-expressions], in the form described below. | required
resultType | String | The data type of the function's result: a primitive type or `variant`, written as the table schema writes the type ([Schema Serialization Format][schema-serialization-format]), with no collation. | required
referenceTypes | Map[String, Object] | The recorded type and collation of each column that `expr` references, keyed by reference key (see below). | required

The field `expr` has the form `{"type": "apply", "function": F, "arguments": [A, ...]}`. `F` is the
registration's **function name**: a function reference in any form that Iceberg's serialization
allows, such as `{"catalog": "iceberg_functions", "identifier": ["truncate"]}` (see [Function
Names](#function-names)). Two function names are equal if they have the same catalog (or both have
none) and the same identifier parts in the same order, in whichever form Iceberg's serialization
writes them. A string is a one-part identifier with no catalog. For readability, this specification
writes a function name with a catalog as the catalog and the identifier's parts joined with `.`,
such as `iceberg_functions.truncate`. The arguments `A` are in the order of the function's
parameters, and at least one is a column argument. Each is one of the following:

- A **column argument**: a reference to a column. When the column mapping mode is `id` or `name`, it
  is an **ID reference**, `{"type": "reference", "id": N}`, where `N` is the column's
  `delta.columnMapping.id`, and its **reference key** is `N` as a decimal string without leading
  zeros. Otherwise it is a **name reference**, `{"type": "reference", "name": S}`, where `S` is the
  names along the column's [field path][field-path] joined with `.`, and its reference key is `S`. A
  field path through an array element or a map key or value is not allowed.
- A literal argument: `{"type": "literal", "value": V, "data-type": T}`, where `T` is an Iceberg
  type from the table below and `V` is a value of that type. NULL, NaN and infinite values have no
  encoding.

No other expression, such as a nested apply, is an argument. Each member of `referenceTypes` has the
following fields:

Field Name | Data Type | Description | optional/required
-|-|-|-
type | String | The column's data type, which is the type of the values the function receives: a primitive type or `variant`, written like `resultType`. | required
collation | String | The collation of a string-typed column, written exactly as the table schema records it (see the [Collated String Type RFC][collated-string-type]). Absent for the default collation. | optional

A literal argument's `data-type` is the Iceberg type in the second column of the following table,
and its **Delta type** is the type in the first column. Its `value` is encoded as Iceberg's [JSON
single-value serialization][iceberg-single-value] encodes a value of that Iceberg type:

Type | data-type | Encoding of value | Example
-|-|-|-
`boolean` | `boolean` | JSON boolean | `true`
`integer` | `int` | JSON integer | `7`
`long` | `long` | JSON integer | `7`
`float`, `double` | `float`, `double` | JSON number | `1.5`
`decimal(p,s)` | `decimal(P,S)` | JSON string with exactly `s` digits after the decimal point, and no decimal point when `s` is 0 | `"14.20"`
`string` | `string` | JSON string | `"yyyy-MM-dd"`
`binary` | `binary` | JSON string of hexadecimal digits | `"000102ff"`
`date` | `date` | JSON string, ISO-8601 | `"2017-11-16"`
`timestamp` | `timestamptz` | JSON string, ISO-8601 with microseconds and the offset `+00:00` | `"2017-11-16T22:31:08.123456+00:00"`
`timestamp_ntz` (timestamp without time zone) | `timestamp` | JSON string, ISO-8601 with microseconds and no offset | `"2017-11-16T22:31:08.123456"`

Delta's `byte` and `short` have no Iceberg type, so no literal argument has these types. Where the
table says more than Iceberg's text, the table applies. Readers and writers MUST decode an integral
or decimal literal to its exact value, not through a binary floating-point type, which can't hold
every `long` or `decimal` value. Writers compute entries from the decoded values of the literal
arguments. Two literal arguments are equal if they have the same `data-type` and equal decoded
values of that type, with no conversion to another type. So `1.5` and `1.50` are equal `double`
literals. For `float` and `double`, `0.0` and `-0.0` are different values. Strings are equal only if
their UTF-8 bytes are. A string literal argument has the default collation. How the function
combines it with a collated argument is part of the function's behavior.

On a table with column mapping mode `none`, whose struct column `address` has a `string` field
`city` with the collation `ICU.tr`, the following `configuration`, shown decoded, registers
`upper(name)`, `lower(name)` and `upper(address.city)`. The function names are those of an
implementation that names its functions in the catalog `example_engine`, and whose `upper` and
`lower` take the version of the Unicode standard that they follow as a literal argument, here
`15.1`. The third registration records the collation. The IDs, catalog, version and collation
identifier are illustrative.

```json
{
  "expressions": [
    {"id": "9c41d2e07a35b816",
     "expr": {"type": "apply",
              "function": {"catalog": "example_engine", "identifier": ["upper"]},
              "arguments": [{"type": "reference", "name": "name"},
                            {"type": "literal", "value": "15.1", "data-type": "string"}]},
     "resultType": "string",
     "referenceTypes": {"name": {"type": "string"}}},
    {"id": "2be07f13c9d4a560",
     "expr": {"type": "apply",
              "function": {"catalog": "example_engine", "identifier": ["lower"]},
              "arguments": [{"type": "reference", "name": "name"},
                            {"type": "literal", "value": "15.1", "data-type": "string"}]},
     "resultType": "string",
     "referenceTypes": {"name": {"type": "string"}}},
    {"id": "e5a3910c7d24bf68",
     "expr": {"type": "apply",
              "function": {"catalog": "example_engine", "identifier": ["upper"]},
              "arguments": [{"type": "reference", "name": "address.city"},
                            {"type": "literal", "value": "15.1", "data-type": "string"}]},
     "resultType": "string",
     "referenceTypes": {"address.city": {"type": "string", "collation": "ICU.tr"}}}
  ]
}
```

In the log, the `configuration` is this object serialized as a JSON string. With only the first
registration, the action is:

```json
{"domainMetadata":{"domain":"delta.expressionStats","configuration":"{\"expressions\":[{\"id\":\"9c41d2e07a35b816\",\"expr\":{\"type\":\"apply\",\"function\":{\"catalog\":\"example_engine\",\"identifier\":[\"upper\"]},\"arguments\":[{\"type\":\"reference\",\"name\":\"name\"},{\"type\":\"literal\",\"value\":\"15.1\",\"data-type\":\"string\"}]},\"resultType\":\"string\",\"referenceTypes\":{\"name\":{\"type\":\"string\"}}}]}","removed":false}}
```

On a table with column mapping mode `name`, where the column `name` has `delta.columnMapping.id` 3,
a registration of `substring(name, 1, 3)` has one column argument and two literal arguments (the ID
is illustrative; the function is in the catalog of the example above):

```json
{"id": "71c0e4b2a9d35f86",
 "expr": {"type": "apply",
          "function": {"catalog": "example_engine", "identifier": ["substring"]},
          "arguments": [{"type": "reference", "id": 3},
                        {"type": "literal", "value": 1, "data-type": "int"},
                        {"type": "literal", "value": 3, "data-type": "int"}]},
 "resultType": "string",
 "referenceTypes": {"3": {"type": "string"}}}
```

In column mapping modes `id` and `name`, `expr` has the form of the expression of an Iceberg
[stats-only field][iceberg-stats-fields], which uses only ID references. A reader that implements
the registration can use its entries for the predicate `substring(name, 1, 3) = 'act'`, where `1`
and `3` are `integer` literals, but not for `substring(name, 1, 4) = 'acti'`, whose second literal
argument differs (see [Reader Requirements](#reader-requirements-for-expression-statistics)).

A registration is **valid** in a snapshot if all of the following hold, and **stale** otherwise:

- it has every required field, each field it has is of the type and form described, and no other
  registration in the registry has the same `id`;
- its `resultType` is a primitive type or `variant`, and the table's protocol allows a column of
  that type in that snapshot;
- on a table that supports `adaptiveMetadata`, its `resultType` is a primitive type, because that
  feature's RFC gives stats structs only to leaf (primitive) columns, and `variant` is not a
  primitive type;
- every argument of `expr` is a column argument or a literal argument, with exactly the members of
  that form, and at least one is a column argument;
- for every literal argument, `data-type` is one of the Iceberg types in the table above, and
  `value` is a valid encoding of a value of that type;
- for every column argument, the reference is an ID reference when the column mapping mode is `id`
  or `name` and a name reference otherwise, and `referenceTypes` has a member whose key is exactly
  its reference key and whose `type` is a primitive type or `variant`;
- for every column argument, the reference resolves in that snapshot's schema, through struct
  fields, to exactly one field: the field whose `delta.columnMapping.id` is `N`, or the field whose
  names along its field path, joined with `.`, are `S`. Names are compared regardless of casing, as
  [Schema Serialization Format][schema-serialization-format] makes them unique ("All column names
  must be unique regardless of casing"). A name that two fields share, such as `a.b` for a column
  named `a.b` and the field `b` of a struct column `a`, resolves to no field;
- for every column argument, the resolved field's data type equals the recorded `type`, and its
  collation is the recorded `collation`, or the default collation when `collation` is absent.

For a writer, the **current snapshot** is the snapshot that its commit produces or, when it writes a
checkpoint, the snapshot that the checkpoint represents. The registry is the registry of the current
snapshot, and a registration is **currently valid** if it is valid in the current snapshot.

## Function Names

Function names obey one rule:

> **A function name identifies exactly one behavior across all implementations.**

Two different behaviors never share a function name, whether they belong to different
implementations or to different releases of one implementation. A release that changes a function's
results, for example by following a newer Unicode version, uses another function name, or the
function takes the changed input as a literal argument, whose value then differs. One behavior can
have several function names. Implementations MUST choose and use function names so that the rule
holds, and readers MAY rely on it.

A function that [Appendix A of Iceberg's expressions spec][iceberg-functions] defines in the
reserved catalog `iceberg_functions`, such as `truncate`, has the function name `iceberg_functions.`
followed by the one element of its Iceberg identifier, spelled as Appendix A spells it (for example,
`iceberg_functions.truncate`). That name identifies the behavior that Iceberg's definition gives the
function for the arguments that the definition accepts, in the expressions spec and in the table
spec sections that Appendix A refers to, both at Iceberg commit `2234c549`, which this RFC cites. A
registration with such a name whose arguments the definition doesn't accept has no behavior, and
writers publish no entries for it (see below). Implementations MUST NOT use a function name in the
catalog `iceberg_functions` for any other behavior.

A writer MUST NOT publish an entry for a registration whose function name is in the catalog
`iceberg_functions` unless the definition accepts the registration's arguments: their number and
order, their types, and their kind, where a parameter such as `truncate`'s width must be a literal
argument. For these checks, a literal argument has the type of its `data-type`. A column argument
has the Iceberg type that the table in [Registry](#registry) maps its Delta type to, and a type that
the table doesn't list has no Iceberg type. A writer also MUST NOT publish one unless the
registration's `resultType` maps to the type that the definition gives for these arguments, and each
string column argument has the default collation.

Any other function name SHOULD be in a catalog in which only one party, the name's owner, defines
names, such as a catalog named after a domain name that the owner controls, as [Iceberg's
expressions spec][iceberg-expressions] lets engines document and use a catalog name for their
built-in functions. The owner decides which behavior each of its names identifies, and can publish
those definitions outside the table. A function name without a catalog, such as `upper`, refers to a
function of the catalog that stores the table, as Iceberg's expressions spec reads a reference
without a catalog. Several implementations can read and write a table in one catalog, so such a name
doesn't say which implementation's `upper` it means, and two implementations could use it for
different behaviors.

A reader that uses a registration's entries relies on every writer that computed them implementing
the registration. Nothing in the table shows whether a writer did, and readers are never required to
use entries, so each implementation decides, per function name, whether to use them.

## Per-file Entries

A file's [statistics][per-file-statistics] can contain an `expressionStats` object with the
following fields, each mapping entry keys to values:

Field Name | Data Type | Description | optional/required
-|-|-|-
minValues | Object | Each entry's minimum, encoded as [Per-file Statistics][per-file-statistics] encodes the minimum of a column whose type is the registration's `resultType`. | optional
maxValues | Object | Each entry's maximum, encoded likewise. | optional
nullCount | Object | Each entry's null count, a non-negative integer. | optional

An entry key is the ID of the entry's registration, exactly 16 lowercase hexadecimal characters, for
example `9c41d2e07a35b816`. A member whose name has another form is malformed and is not an entry.
Each of an entry's minimum, maximum and null count is optional. A `remove` action's statistics can
carry the same object, copied from the `add`; readers use entries only from `add` actions.

A reader that uses entries MUST ignore:

- any member of the `expressionStats` object other than these three fields;
- any of these fields whose value is not a JSON object; and
- any value that it can't read as a statistic of a column of the registration's `resultType`.

Writers MAY drop such members and values when they re-add a file or write a checkpoint. The
requirements for keeping still-valid entries in [Writer
Requirements](#writer-requirements-for-expression-statistics) do not apply to such members and
values.

An entry under the key `<id>` describes the results that the behavior identified by the function
name of registration `<id>` produces on the file's rows, for the registration's arguments: each
row's values of its column arguments, with the types and collations that the registration records,
and the values of its literal arguments. A partition column's value in a row is the file's partition
value, read as [Partition Value Serialization][partition-value-serialization] specifies. A variant
column's value is the logical variant value, however the file shreds it. Such an entry meets both of
these conditions:

- That behavior produces a result, which can be NULL but not an error, for every row of the entry's
  statistics population. If a row of the population has no result, the file has no entry under this
  key.
- The entry's `minValues`, `maxValues` and `nullCount` are the statistics that [Per-file
  Statistics][per-file-statistics] defines for a column whose type is the registration's
  `resultType` and whose value in each row is the row's result, a string result having the default
  collation. They have that section's encoding and its tight and wide meanings, including its
  footnote on shortened string bounds and truncated timestamp bounds. Readers interpret them as they
  interpret the statistics of such a column. An entry has only the statistics that Per-file
  Statistics, as other features change it, allows for such a column, and a reader uses them only
  where it may use such a column's statistics.

`stats.tightBounds` describes entries as it describes per-column statistics (on a table that
supports `adaptiveMetadata`, see also [Expression Statistics with Adaptive
Metadata](#expression-statistics-with-adaptive-metadata)). When it marks the statistics as tight,
each entry's statistics population is exactly the valid rows. When it marks them as wide, the
population can also include rows that the deletion vector deletes. A delete can make entries no
longer tight. So, on a table that supports this feature, a writer that deletes rows of a file with
entries through a deletion vector sets `stats.tightBounds` to `false`, unless it recomputes both the
per-column statistics and the entries, because every entry a writer publishes MUST meet this
definition.

For example, a two-row file of the table in the [Registry](#registry) example, with `name` values
`active` and `Inactive`, holds entries for the `upper(name)` and `lower(name)` registrations,
computed by an implementation that implements both. The decoded statistics are:

```json
{
  "numRecords": 2,
  "tightBounds": true,
  "minValues": {"name": "Inactive"},
  "maxValues": {"name": "active"},
  "nullCount": {"name": 0},
  "expressionStats": {
    "minValues": {"9c41d2e07a35b816": "ACTIVE", "2be07f13c9d4a560": "active"},
    "maxValues": {"9c41d2e07a35b816": "INACTIVE", "2be07f13c9d4a560": "inactive"},
    "nullCount": {"9c41d2e07a35b816": 0, "2be07f13c9d4a560": 0}
  }
}
```

A reader whose `upper` gives the results of `example_engine.upper` with the Unicode version 15.1 can
skip this file for the predicate `upper(name) = 'ZOE'`, because `ZOE` is greater than the maximum
`INACTIVE`. A reader whose `upper` follows another Unicode version gives other results for some
strings, so it cannot use this entry. It can use the entries of a registration whose literal
argument gives its own version, if the table has one.

In the `stats_parsed` field of a [checkpoint][checkpoints], `expressionStats` is a struct with the
fields `minValues`, `maxValues` and `nullCount`. Each of them is a struct with one field for each
entry key that is found in that field of the statistics of the checkpoint's `add` actions and whose
registration is in the registry and currently valid. The field is named by the key and is null for
an `add` without that entry. All parts and sidecars of one checkpoint MUST use the same fields: one
for each such key found in any of its `add` actions. A struct that would have no field is omitted,
and so is `expressionStats` when all three are. The type of a minimum or maximum is the `resultType`
of the key's registration, and a null count is a `long`. A reader that uses entries MUST treat an
`expressionStats` field of `stats_parsed` that does not have this form as absent. For the file
above:

```
|-- stats_parsed: struct
|    |-- ...
|    |-- expressionStats: struct
|    |    |-- minValues: struct
|    |    |    |-- 9c41d2e07a35b816: string
|    |    |    |-- 2be07f13c9d4a560: string
|    |    |-- maxValues: struct
|    |    |    |-- 9c41d2e07a35b816: string
|    |    |    |-- 2be07f13c9d4a560: string
|    |    |-- nullCount: struct
|    |    |    |-- 9c41d2e07a35b816: long
|    |    |    |-- 2be07f13c9d4a560: long
```

## Expression Statistics with Adaptive Metadata

This section applies to tables that support both Expression Statistics and
[`adaptiveMetadata`][adaptive-metadata], which requires column mapping in mode `id` or `name`. On
such a table, a file's statistics are in the `stats` of its `add` action while the file is only in
the log. Once the file is folded into the metadata tree, they are in the `content_stats` of its
manifest entry. `content_stats` holds stats structs under struct field IDs that the
`adaptiveMetadata` RFC derives from field IDs, and keeps a file's entries in the stats structs for
the field IDs to which the registry maps their keys.

For a file in the metadata tree, its `add` action in a snapshot (the one that the rules for re-adds
refer to) is the one that the `adaptiveMetadata` RFC reconstructs from its manifest entry, with its
entries taken from `content_stats` as described below. As the `adaptiveMetadata` RFC requires,
writers write no classic or V2 checkpoints of such a table, other than the standalone checkpoints
that it defines. A standalone checkpoint keeps the still-valid entries of the file actions it
carries, as [Writer Requirements](#writer-requirements-for-expression-statistics) require of every
checkpoint.

The registry's field `fieldIds` maps entry keys to field IDs, which are positive integers for which
the `adaptiveMetadata` RFC can derive a stats struct ID, as it does for a column ID. Because an
entry key is a registration ID, each registration has at most one field ID. A **mapping** is one
member of `fieldIds`. It is **usable** in a snapshot if its value is such a field ID and its key's
registration is in the registry.

For example, suppose that the table of [Registry](#registry) used column mapping mode `name`, so
that its registrations referenced columns by ID, and its `delta.columnMapping.maxColumnId` were 11.
A writer that first publishes entries under the two keys of the file in [Per-file
Entries](#per-file-entries) adds the following to the registry and sets
`delta.columnMapping.maxColumnId` to 13:

```json
"fieldIds": {"9c41d2e07a35b816": 12, "2be07f13c9d4a560": 13}
```

When the file is folded into the metadata tree, its `upper(name)` entry goes into the stats struct
for field ID 12 (struct field ID 12400, which is 10000 + 200 x 12), with the lower bound `ACTIVE`,
the upper bound `INACTIVE` and a null count of 0.

Where the `adaptiveMetadata` RFC's rules for per-column statistics in `stats` differ from [Per-file
Statistics][per-file-statistics], those rules apply to entries as to a column of the registration's
`resultType`. An entry is tight exactly when such a column's statistics in the same file would be,
and its bounds are encoded as that column's would be. The deletion-vector rule of [Per-file
Entries](#per-file-entries) applies to each flag with which the `adaptiveMetadata` RFC marks
per-column statistics as tight or wide, as it applies to `stats.tightBounds`.

On such a table, writers MUST follow these rules:

- A writer MUST NOT publish an entry under a key that has no usable mapping in the current snapshot.
  The writer that first publishes entries under a key adds that key's mapping in the same commit.
- A new mapping's field ID MUST be greater than `delta.columnMapping.maxColumnId` and than every
  field ID in the current snapshot's schema, including each field's `delta.columnMapping.id` and the
  IDs in `delta.columnMapping.nested.ids`. The same commit MUST set
  `delta.columnMapping.maxColumnId` to the largest field ID it assigned. Writers MUST treat two
  overlapping transactions that both change `delta.columnMapping.maxColumnId` as conflicting, even
  where the `adaptiveMetadata` RFC would let a manifest commit incorporate an overlapping log
  commit's changes.
- A field ID MUST NOT be assigned to two different entry keys, or to an entry key and a column or a
  nested field, across all versions of the table. Writers MUST NOT change the field ID of a mapping,
  and MAY remove a mapping whose registration is not in the registry. A writer that copies mappings
  from another table, for example when cloning it, MUST keep these rules in the target table and
  MUST drop any mapping it can't keep, with the entries under its key.
- The commit that makes a table support both features, by adding either one, MUST add a usable
  mapping for the key of every still-valid entry that an `add` action of the table carries.
- A writer that folds an `add` action into the metadata tree MUST carry each still-valid entry of
  the action into the stats struct for its key's field ID, without dropping or recomputing it. It
  MAY drop the action's entries that are not still valid. Each still-valid entry is converted as the
  `adaptiveMetadata` RFC converts the statistics of a nullable column of the registration's
  `resultType` with that field ID:
  - the minimum becomes the lower bound;
  - the maximum becomes the upper bound; and
  - the null count becomes the null value count.
- A writer that converts a manifest entry's `content_stats` back to `stats` MUST carry each
  still-valid entry from the stats struct for a usable mapping's field ID to that mapping's key,
  without dropping or recomputing it. It MAY omit entries from the statistics of a `remove` action.

A reader that uses an entry from `content_stats` MUST take it from the manifest entry of the data
file (`content_type` DATA), not from a `DATA_MANIFEST` entry, whose statistics summarize a leaf
manifest, and only from the stats struct for the field ID of the key's usable mapping. [Reader
Requirements](#reader-requirements-for-expression-statistics) apply to such an entry as to one in
`stats`.

## Reader Requirements for Expression Statistics

Readers need not read the registry or use entries. A reader MUST NOT use an entry to skip a file for
a predicate unless all of the following hold:

- the table supports the Expression Statistics feature;
- the entry is a [still-valid entry](#terminology);
- the predicate, as the reader evaluates it, applies a function, which can be a composition (see
  [Terminology](#terminology)), directly to the values of each column argument of the registration,
  as read under the snapshot's schema;
- no other operation, not even an implicit cast or a change of collation, comes between a column and
  the function, so that a string column argument reaches the function in the collation that the
  registration records;
- that function's result type, ignoring any collation, is the registration's `resultType`;
- for every value of the registration's column arguments, the predicate's function, under the
  settings with which the reader evaluates the predicate, gives the result that the registration's
  expression gives: the result of the behavior that the registration's function name identifies (see
  [Function Names](#function-names)), for those values and for the values of the literal arguments,
  whether the predicate gives those values as literals, such as `1` and `3` in
  `substring(name, 1, 3)`, or the reader's settings or release give them, such as a time zone or a
  Unicode version;
- no column argument of the registration is a partition column of an interval type, as the [Interval
  Types RFC][interval-types] says that readers must not eliminate files based on interval partition
  values;
- the entry's key is the registration's ID.

A reader finds the registrations whose entries it can use through their function names and literal
arguments. For example, if a registration's function takes the time zone as a literal argument and
the registration gives `America/Los_Angeles`, a reader whose predicate takes the time zone from its
session can use the registration's entries in a session that uses that zone, if the reader's
function gives the results that the function name identifies for that zone. In a session that uses
UTC, it can't, because its results differ. A reader uses an entry's statistics as it uses per-column
statistics of a column whose values are the function's results. A reader that uses an entry for
anything other than skipping a file, such as computing a minimum from statistics, MUST meet the same
conditions for the expression it computes. Such a reader MUST NOT treat an entry's values as exact
unless it could treat per-column statistics as exact, for example a null count when
`stats.tightBounds` marks the statistics as tight. Readers MUST ignore malformed keys. The minimum
and maximum of a string result are in binary order, so a reader MUST NOT use them for comparisons
that disagree with binary order.

## Writer Requirements for Expression Statistics

When Expression Statistics is supported (when the `writerFeatures` field of a table's `protocol`
action contains `expressionStats-dev`), then:

- A writer that adds a new registration MUST generate its ID as 8 random bytes, independently of any
  other ID. A registration ID MUST NOT name two different registrations across all versions of the
  table: a writer that copies a registration, for example when restoring or cloning a table, copies
  it with its ID unchanged, except where [Removing the feature](#removing-the-feature) forbids the
  copy.
- Writers SHOULD add only registrations that they implement, with the argument types that the
  function takes and the result type it produces for them.
- Writers MUST NOT change a registration, including any of its fields that this specification does
  not define, and MUST keep any member of the registry object that they don't recognize.
  Registrations are added and removed by commits that update the `delta.expressionStats` domain.
- Writers SHOULD reuse a currently valid registration equal to the one they would add, instead of
  adding another. Two registrations are equal if all of the following hold:
  - their function names are equal (see [Registry](#registry));
  - they have the same number of arguments;
  - in each position, they have either the same column argument or equal literal arguments; and
  - they have the same `resultType` and `referenceTypes`.
- Every entry a writer publishes MUST meet the definition in [Per-file Entries](#per-file-entries),
  however the writer obtained its values.
- On a table that also supports the `parquetEncryption` feature of the
  [Parquet Encryption RFC][parquet-encryption], a writer MUST NOT publish an entry whose
  registration has a column argument that is encrypted in the entry's data file, nor any entry for a
  data file written with uniform encryption (all columns encrypted with the footer key), as that RFC
  forbids per-file statistics for encrypted columns and, under uniform encryption, column statistics
  in Delta metadata files.
- A writer SHOULD publish, for every data file it writes, including files produced by rewrites, an
  entry for each of its own currently valid registrations under the settings with which it writes
  the file, such as its session's time zone (see [Terminology](#terminology)), unless a row of the
  entry's statistics population has no result (see [Per-file Entries](#per-file-entries)) or the
  rule above for Parquet Encryption forbids the entry.
- A writer MAY publish entries for any other currently valid registration that it implements. The
  requirements below for keeping entries apply whether or not it implements the entry's
  registration.
- A writer that re-adds a file unchanged MUST keep every still-valid entry of the **source** `add`,
  by copying it unchanged or by replacing it with a new entry under the same key. A re-add is
  unchanged when every valid row of the new `add` is a valid row of the source `add`, with the same
  value in every column. The source `add` is the file's `add` in the snapshot that the writer's
  commit is based on or, when the writer restores the file to an earlier table version, the file's
  `add` in that version. A re-add that changes the value of any column in a valid row of the new
  `add`, or that makes valid a row that the source `add` deletes, is not unchanged. Like a rewrite,
  it MAY drop the source's entries, and any entry it publishes MUST still meet the definition in
  [Per-file Entries](#per-file-entries).
- A writer that re-adds a file that the table contains, to publish entries without changing its
  valid rows or their values, MUST set `dataChange` to `false` on the `add` action, since such an
  action only adds statistics ([Add File and Remove File][add-file-and-remove-file]). On a table
  that also supports `adaptiveMetadata`, a commit whose actions only add statistics MUST also set
  `commitInfo.dataChange` to `false`, as the `adaptiveMetadata` RFC requires.
- A writer MAY drop an entry that is not still valid, whenever it re-adds a file or writes a
  checkpoint.
- A writer that writes a checkpoint MUST represent every still-valid entry of each `add` action in
  the checkpoint as the entry is, without dropping or recomputing it, in each form of statistics it
  writes for that action: the JSON `stats` field and the typed `stats_parsed` field, typed as
  described in [Per-file Entries](#per-file-entries).
- A writer that writes a [log compaction file][log-compaction-files] MUST keep the `expressionStats`
  object of each `add` action that it includes unchanged.
- On a table that also supports `adaptiveMetadata`, writers MUST also follow the rules of
  [Expression Statistics with Adaptive Metadata](#expression-statistics-with-adaptive-metadata).

## Removing the feature

In the version that removes `expressionStats-dev` from the `writerFeatures` field of the table's
`protocol` action, whatever command commits it, including a RESTORE, the writer MUST remove the
`delta.expressionStats` domain if the table has it, with a `domainMetadata` action whose `removed`
field is `true`. The writer MAY leave every entry unchanged. From that version on, no entry has a
registration, so no reader uses an entry. Because a registration ID never names two registrations
across all versions of the table, and a registration from before the removal can't be brought
back (see below), no later registration gives such an entry a meaning again. Readers of earlier
versions, for example through time travel, still see those versions' registries and entries, which
describe those versions' files.

When the feature is added again, the registry holds none of the registrations from before the
removal. A writer MUST NOT bring back into the registry a registration that the table had before the
feature was removed, for example when it restores an earlier version of the table. This is an
exception to the rule in [Writer Requirements](#writer-requirements-for-expression-statistics) that
a writer copies a registration with its ID unchanged. A clone into another table is not affected,
because it copies the registry and the `add` actions of one version of its source.

The requirements of [Writer Requirements](#writer-requirements-for-expression-statistics) and of
[Expression Statistics with Adaptive Metadata](#expression-statistics-with-adaptive-metadata),
including those for entries, apply only to tables that support the feature. While a table that
supports `adaptiveMetadata` does not support this feature, the stats structs of its entries also
stay in `content_stats`, where readers ignore them, because no usable mapping names their field IDs
and [Protocol Evolution][protocol-evolution] makes other clients ignore unrecognized fields.

## Compatibility with other features

- **Adaptive metadata.** A table can support both Expression Statistics and the `adaptiveMetadata`
  feature of the [Iceberg V4 Adaptive Metadata Tree RFC][adaptive-metadata]; [Expression Statistics
  with Adaptive Metadata](#expression-statistics-with-adaptive-metadata) gives the rules.
- **IcebergWriterCompatV1 and V3.** The [IcebergWriterCompatV1 RFC][iceberg-writer-compat-v1]
  requires every enabled feature of a table with `icebergWriterCompatV1` to be on its allowlist,
  which does not include `expressionStats-dev`, so such tables can support Expression Statistics
  only after a separate change adds the feature to that allowlist. The same applies to
  `icebergWriterCompatV3`, which has no published RFC yet and has an allowlist of its own that does
  not include `expressionStats-dev`.
- **Iceberg Native V4.** The proposed [Iceberg Native V4 RFC][iceberg-native-v4] rejects enablement
  while a writer feature outside its allowlist is present, and the allowlist doesn't include
  `expressionStats-dev`, so the two can't be used together yet.

## Examples

The sections above include the main examples. The two below show edge cases; their IDs and their
catalog, `example_engine`, are illustrative.

**A registration that becomes stale.** A table registers `abs(qty)`, as `example_engine.abs`, for
its `integer` column `qty`:

```json
{"id": "7d0c5e91a2b4f836",
 "expr": {"type": "apply",
          "function": {"catalog": "example_engine", "identifier": ["abs"]},
          "arguments": [{"type": "reference", "name": "qty"}]},
 "resultType": "integer",
 "referenceTypes": {"qty": {"type": "integer"}}}
```

If [Type Widening][type-widening] later changes `qty` to `long`, the reference resolves to a field
of type `long`, not the `type` that `referenceTypes` records, so the registration is stale from that
version on. Readers don't use entries under its ID, and writers may drop them. A writer that wants
statistics for `abs(qty)` again adds a new registration with a new random ID, such as
`0f6b2d8e4a1c9357`, whose `referenceTypes` records the type `long` for `qty` and whose `resultType`
is `long`.

**A file with no entry for one registration.** A table has two registrations of `to_date(s)` for its
`string` column `s`, both with the result type `date`: `example_engine.to_date(s, 'error')` under
the ID `51c2e8a07b9d3f46`, and `example_engine.to_date(s, 'null')` under the ID `c03a7e19d84b2f65`.
The second argument, a literal, says what the function does with a string that isn't a date:
`'error'` raises an error, and `'null'` returns NULL. A two-row file holds the `s` values
`2026-10-01` and `not a date`. For the first registration, the function raises an error for
`not a date`, so one row of the statistics population has no result, and the file has no entry under
its ID. For the second, the entry's minimum and maximum are both `2026-10-01`, and its null count
is 1. The decoded statistics are:

```json
{
  "numRecords": 2,
  "minValues": {"s": "2026-10-01"},
  "maxValues": {"s": "not a date"},
  "nullCount": {"s": 0},
  "expressionStats": {
    "minValues": {"c03a7e19d84b2f65": "2026-10-01"},
    "maxValues": {"c03a7e19d84b2f65": "2026-10-01"},
    "nullCount": {"c03a7e19d84b2f65": 1}
  }
}
```

## Changes to existing sections

Besides adding the new section above, three existing sections change, as shown below:

- Per-file Statistics, in the Appendix;
- Checkpoints, in the body;
- Writer Requirements for Column Mapping, in the body.

> ***Add a paragraph after the table of global statistics in [Per-file
> Statistics][per-file-statistics], before the paragraph on logical files with a deletion vector***

<ins>When the [Expression Statistics](#expression-statistics) feature is supported, a file's
statistics can also contain an `expressionStats` object, which [Per-file
Entries](#per-file-entries) defines.</ins>

> ***Change to the `stats_parsed` item of [Checkpoints][checkpoints]***

 - stats_parsed: The stats can be stored in their [original format](#Per-file-Statistics). This
   field needs to be written when statistics are available and the table property:
   `delta.checkpoint.writeStatsAsStruct` is set to `true`. When this property is set to `false`
   (which is the default), this field should be omitted from the checkpoint. <ins>When the table
   supports Expression Statistics, its `expressionStats` field is typed as [Per-file
   Entries](#per-file-entries) describes.</ins>

> ***Change to the last item of [Writer Requirements for Column
> Mapping][writer-requirements-for-column-mapping]***

 - Assign a physical name for each new column that is added to the schema, and ensure the physical
   field path of the new column is unique across all versions of the table. In addition, column
   identifiers need to be assigned to each column. The maximum id that is assigned to a column<ins>,
   or to an entry key as [Expression Statistics with Adaptive
   Metadata](#expression-statistics-with-adaptive-metadata) describes,</ins> is tracked as the table
   property `delta.columnMapping.maxColumnId`. This is an internal table property that cannot be
   configured by users. This value must increase monotonically as new columns are introduced and
   committed to the table alongside the introduction of the new columns to the schema<ins>, or of
   the new mappings to the `delta.expressionStats` domain</ins>.

## Appendix changes

> ***Add a new row to the [Valid Feature Names in Table Features][valid-feature-names] table.***

Feature | Name | Readers or Writers?
-|-|-
[Expression Statistics](#expression-statistics) | `expressionStats-dev` | Writers only

The feature adds no table property, so the Table Properties table doesn't change.

> ***Regenerate the table of contents with doctoc. It adds entries for the Expression Statistics
> section and its subsections, and for In-Commit Timestamps and Type Widening, which the current
> table of contents lacks.***

[type-widening]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#type-widening
[per-file-statistics]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#per-file-statistics
[domain-metadata]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#domain-metadata
[writer-requirements-for-domain-metadata]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#writer-requirements-for-domain-metadata
[schema-serialization-format]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#schema-serialization-format
[field-path]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#field-path
[checkpoints]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#checkpoints-1
[log-compaction-files]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#log-compaction-files
[valid-feature-names]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#valid-feature-names-in-table-features
[protocol-evolution]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#protocol-evolution
[add-file-and-remove-file]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#add-file-and-remove-file
[collated-string-type]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/collated-string-type.md
[adaptive-metadata]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/iceberg-v4-metadata.md
[iceberg-writer-compat-v1]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/iceberg-writer-compat-v1.md#allowed-supported-list-of-features
[kernel-iceberg-writer-compat-v3]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/kernel/kernel-api/src/main/java/io/delta/kernel/internal/icebergcompat/IcebergWriterCompatV3MetadataValidatorAndUpdater.java#L104-L131
[bcp14]: https://www.rfc-editor.org/info/bcp14
[column-updates]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/column-updates.md
[materialize-partition-columns]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#materialize-partition-columns
[checkpoint-protection]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/checkpoint-protection.md
[column-mapping-usage-tracking]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/column-mapping-usage-tracking.md
[concurrent-identity-columns]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/concurrent-identity-columns.md
[interval-types]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/interval-types.md
[nanosecond-timestamps]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/nanosecond-timestamps.md
[user-defined-types]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/user-defined-types.md
[iceberg-tight-stats]: https://github.com/apache/iceberg/pull/18585
[iceberg-single-value]: https://github.com/apache/iceberg/blob/c285171ea8758378db2fc8e735ebbe59a5cde9b0/format/spec.md#json-single-value-serialization
[file-type]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/file-type.md
[iceberg-expressions]: https://github.com/apache/iceberg/blob/2234c549de60ae6bb9cf6f319dff0f6f94df76e3/format/expressions-spec.md
[iceberg-expressions-json]: https://github.com/apache/iceberg/blob/2234c549de60ae6bb9cf6f319dff0f6f94df76e3/format/expressions-spec.md#appendix-b-json-serialization
[iceberg-functions]: https://github.com/apache/iceberg/blob/2234c549de60ae6bb9cf6f319dff0f6f94df76e3/format/expressions-spec.md#appendix-a-iceberg-functions
[geospatial-types]: https://github.com/delta-io/delta/pull/8240
[partition-value-serialization]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#partition-value-serialization
[column-mapping]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#column-mapping
[writer-requirements-for-column-mapping]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#writer-requirements-for-column-mapping
[iceberg-stats-fields]: https://github.com/apache/iceberg/blob/3ad26de2bd62600724d38d3428f65f4f7cde2586/format/spec.md#stats-only-fields
[deletion-vectors]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#deletion-vectors
[v2-checkpoint]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#v2-checkpoint-table-feature
[metadata-cleanup]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#metadata-cleanup
[vacuum-protocol-check]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#vacuum-protocol-check
[version-checksum-file]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#version-checksum-file
[action-reconciliation]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#action-reconciliation
[row-tracking]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#row-tracking
[change-data-feed]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#add-cdc-file
[append-only-tables]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#append-only-tables
[in-commit-timestamps]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#in-commit-timestamps
[generated-columns]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#generated-columns
[check-constraints]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#check-constraints
[column-invariants]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#column-invariants
[default-columns]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#default-columns
[identity-columns]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#identity-columns
[clustered-table]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#clustered-table
[iceberg-compat-v1]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#iceberg-compatibility-v1
[iceberg-compat-v2]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#iceberg-compatibility-v2
[iceberg-compat-v3]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/protocol_rfcs/iceberg-compat-v3.md
[catalog-managed-tables]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#catalog-managed-tables
[variant-data-type]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#variant-data-type
[timestamp-ntz]: https://github.com/delta-io/delta/blob/25c34856c6847809e95deb79af523e56f5c4604d/PROTOCOL.md#timestamp-without-timezone-timestampntz
[clustering-expressions]: https://github.com/delta-io/delta/pull/6737
[iceberg-native-v4]: https://github.com/delta-io/delta/pull/7374
[parquet-encryption]: https://github.com/delta-io/delta/pull/7470
[invisible-columns]: https://github.com/delta-io/delta/pull/7732
