# User-Defined Types (UDT)
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7559**

This protocol change documents the disposition of Spark's `UserDefinedType` (`udt`) columns. Spark writes `udt` columns into `metaData.schemaString`, but the protocol's schema type system defines only primitive / struct / array / map (plus `variant`). A `udt` field is therefore non-conformant under the current spec, even though such columns already exist in tables written by earlier clients, and a reader that rejects the unknown type fails to read the entire table.

Like [`void`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) and interval types, `udt` is documented here post-facto and is **not** gated by a table feature. A `udt` introduces no new physical representation: it is an engine-specific *annotation* over an existing physical type (its `sqlType`). A reader that does not run the engine's deserialization code and simply reads the `sqlType` reads correct data. Unlike `timestampNtz` / `variant`, which introduced new physical semantics that readers must opt into, there is nothing here for a reader to opt into, and a feature gate could only fragment behavior for columns that already exist unguarded. This mirrors the `void` precedent.

This change consists of one addition:

- A new **User-Defined Types** section under Schema Serialization Format, defining the `udt` schema serialization, its physical representation (always the `sqlType`), the annotation fields as an opaque string-to-string mapping, reader/writer requirements, column-mapping behavior, statistics behavior, and error conditions.

--------

> ***Add a new "User-Defined Types" section after the [Map Type](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#map-type) section within [Schema Serialization Format](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format).***

# User-Defined Types

A user-defined type (UDT) is an engine-specific logical annotation over an ordinary physical type. It carries no new physical representation: the column is stored on disk exactly as its **`sqlType`**. The annotation names an engine-specific class (referenced by `class` and/or `pyClass`) that the engine uses to convert between the stored `sqlType` value and a richer in-memory object (for example, a Spark ML vector). That class, and the conversion it performs, are outside the scope of this protocol; Delta neither loads nor runs it.

A UDT field's `type` in `metaData.schemaString` is a JSON object. A full example is Spark ML's `VectorUDT`, whose `sqlType` is a struct:

```json
{
  "name": "features",
  "type": {
    "type": "udt",
    "class": "org.apache.spark.ml.linalg.VectorUDT",
    "pyClass": "pyspark.ml.linalg.VectorUDT",
    "sqlType": {
      "type": "struct",
      "fields": [
        { "name": "type",    "type": "byte",    "nullable": false, "metadata": {} },
        { "name": "size",    "type": "integer", "nullable": true,  "metadata": {} },
        { "name": "indices", "type": { "type": "array", "elementType": "integer", "containsNull": false }, "nullable": true, "metadata": {} },
        { "name": "values",  "type": { "type": "array", "elementType": "double",  "containsNull": false }, "nullable": true, "metadata": {} }
      ]
    }
  },
  "nullable": true,
  "metadata": {}
}
```

The `udt` object MUST contain:

- **`type`**: the string `"udt"`.
- **`sqlType`**: any valid Delta type (see below). This is the physical, on-disk representation of the column.

Every other member forms the **annotation**: an open, engine-defined set of members identifying the engine-specific type and where its conversion code lives. The protocol does not define these members and never interprets them. Each value MUST be a JSON string or JSON null, and readers and writers MUST preserve every member verbatim. An engine emits only the members meaningful to it; a reader that does not recognize them reads the column as its `sqlType`. A `udt` may carry no annotation members at all: `{type, sqlType}` alone is valid, and is then indistinguishable to a reader from its `sqlType`. Whether a missing engine-specific member (for example Spark's `class`) is an error is the engine's decision, not the protocol's.

**What the annotation is for.** A UDT pairs the stored physical type (`sqlType`) with a reference to engine-specific code that converts between the stored value and the engine's own richer representation, in both directions: reading turns a stored `sqlType` value into the rich object, and writing turns a rich object back into a `sqlType` value. Only the `sqlType` value is ever stored; the rich object exists only in the engine's memory. An engine that recognizes the reference uses its code to convert; one that does not reads the `sqlType` value, which is already correct.

**Spark's members (example).** Spark records where its conversion code lives as:

- `class`: the JVM type implementing the conversion (e.g. `org.apache.spark.ml.linalg.VectorUDT`). Present for JVM-defined UDTs and Python UDTs with a JVM peer.
- `pyClass`: the Python type (e.g. `pyspark.ml.linalg.VectorUDT`), or `null` when there is no Python pairing (the key is still present).
- `serializedClass`: a base64-encoded Python type, used when there is no JVM `class`.

Each is only an identifier Spark uses to find its code; Delta does not interpret any of them. So a Spark `udt` takes one of two shapes, `{type, sqlType, class, pyClass}` or `{type, sqlType, pyClass, serializedClass}` (Python-only). These are an example of the mechanism, not the set of members the protocol requires. For example, `VectorUDT`'s stored `sqlType` is `struct<type: byte, size: int, indices: array<int>, values: array<double>>`, and Spark's code maps that struct to and from a vector object.

**Other engines.** A different engine uses its own annotation members (whatever identifies its type and code), or none at all. It does not set Spark's `class`/`pyClass` (not even to `null`); those are meaningful only to Spark. Any reader that lacks a given engine's code reads the column as its `sqlType`.

### The `sqlType`

`sqlType` is a **Delta schema type**, expressed in the same serialization used everywhere else in `metaData.schemaString`, not a Parquet type. It may be a primitive, or a nested struct / array / map, and may itself contain any Delta type.

A `udt`'s `sqlType` (recursively) MUST consist only of types that are supported by the table's protocol version and enabled table features, exactly as if the column were declared with that type directly. A `udt` does not exempt its `sqlType` from any type's requirements: if the `sqlType` contains a feature-gated type (for example `timestampNtz` or `variant`), that feature MUST be enabled on the table. Equivalently, table-feature detection MUST descend into a `udt`'s `sqlType`, and a writer MUST NOT use a `udt` to introduce a type whose feature is not enabled.

This requirement is also what makes an ungated `udt` safe. Because the `sqlType` is independently conformant with the table's enabled features, a reader that supports those features reads the `sqlType` correctly, and the `udt` annotation adds nothing a reader must opt into. Without it, a `udt` would be a backdoor around the feature gate. Statistics and Parquet rules for the physical `sqlType` apply as normal.

UDT columns are permitted anywhere a type is permitted: as a top-level column, a nested struct field, an array element type, or a map key or value type.

## Physical Representation

The physical Parquet representation of a UDT column is exactly that of its `sqlType`. The column data carries no `udt` marker: a UDT column is physically indistinguishable from a column of its `sqlType`, and the UDT semantics are carried solely by the annotation in `metaData.schemaString`.

Spark additionally embeds a copy of its full schema (including the `udt` annotation) in the Parquet file footer's key-value metadata (under `org.apache.spark.sql.parquet.row.metadata`). This footer copy is Spark-specific and is **not required** by this protocol. A Delta reader obtains the table schema from `metaData.schemaString` and MUST NOT depend on the Parquet footer for UDT information; a writer is not required to produce it.

## Column Mapping

A UDT is a **leaf** for column mapping. When [Column Mapping](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#column-mapping) is enabled, the field whose type is a `udt` receives a `physicalName` and field id like any other column, but column mapping does **not** recurse into the `sqlType`: the fields inside the `sqlType` are not assigned `physicalName`/id metadata and are matched by the `sqlType`'s own intrinsic structure (the field names the UDT defines). This matches how a UDT is handled elsewhere: its internal shape is engine-defined and opaque to the table's logical column namespace.

## Reader Requirements

A reader:

- MUST interpret a `udt` column as its `sqlType` for all physical operations (Parquet read, projection, statistics).
- that does not implement the engine deserialization code identified by `class` / `pyClass` MUST still read the column as its `sqlType`. The stored values are correct; only the reconstruction of the richer engine object is unavailable.
- that preserves and re-serializes the schema MUST retain the annotation (see Writer Requirements).

## Writer Requirements

A writer:

- MUST store a UDT column's data physically as its `sqlType`.
- **MUST preserve `udt` columns.** A writer MUST NOT drop the annotation or downgrade a `udt` column to its bare `sqlType`. It MUST emit `type` and `sqlType`, and MUST retain every annotation member together with its string-or-null value, in `metaData.schemaString`.
- MUST NOT introduce a non-string annotation member.

## Per-file Statistics

A UDT column is not eligible for `minValues` / `maxValues`, and readers must not perform min/max data skipping over one: `UserDefinedType` is not a skipping-eligible type, so no min/max is recorded, and its `sqlType` (even if it contains orderable leaves) is not descended into for skipping. A per-column `nullCount` is recorded for a UDT column, with the UDT treated as a single statistics leaf: the null count is for the column as a whole, and statistics do not descend into the `sqlType`. Per-file `numRecords` is unaffected.

## Partitioning and Clustering

A UDT column must not be used as a partition column or a clustering column. Partition values have no defined serialization for a UDT (the partition-value serialization rules cover primitive types only), and clustering requires per-column `minValues` / `maxValues` statistics, which are not collected for a UDT column.

## Schema Evolution and Type Changes

A UDT column does not participate in type changes. A writer must not change a UDT column to any other type, nor any other type to a UDT, via the [Type Widening](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#type-widening) feature. Changing the annotation (`class` / `pyClass`) while preserving `sqlType` is an engine concern outside this protocol.

## IcebergCompat

Apache Iceberg has no UDT concept. When any of the `icebergCompatV1`, `icebergCompatV2`, or `icebergCompatV3` features is enabled, a writer must reject a schema containing a UDT column.

## No Table Feature

`udt` is not gated by a table feature (see the introduction). Because it is ungated, a client that does not recognize `udt` may fail when parsing `metaData.schemaString`; the recommended behavior is to read the column as its `sqlType` rather than reject the schema.

## Error Conditions

- **Missing `sqlType`.** A `udt` object without a `sqlType` member must be rejected with an error.
- **Non-string annotation member.** A `udt` object whose annotation member (any member other than `type` and `sqlType`) has a value that is neither a JSON string nor JSON null must be rejected. The annotation is defined as a string-to-string mapping; a non-string value is non-conformant.
- **Unknown but string-valued members** must be preserved, not rejected: the annotation set is engine-extensible and Delta does not enumerate it.
