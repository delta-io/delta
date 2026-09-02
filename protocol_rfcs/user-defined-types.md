# User-Defined Types (UDT)
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7559**

This protocol change documents the disposition of Spark's `UserDefinedType` (`udt`) columns. Spark writes `udt` columns into `metaData.schemaString`, but the protocol's schema type system defines only primitive / struct / array / map (plus `variant`). A `udt` field is therefore non-conformant under the current spec, even though such columns already exist in tables written by earlier clients, and a reader that rejects the unknown type fails to read the entire table.

Like [`void`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) and interval types, `udt` is documented here post-facto and is **not** gated by a table feature. A `udt` introduces no new physical representation: it is an engine-specific *annotation* over an existing physical type (its `sqlType`). A reader that does not run the engine's deserialization code and simply reads the `sqlType` reads correct data. Unlike `timestampNtz` / `variant`, which introduced new physical semantics that readers must opt into, there is nothing here for a reader to opt into, and a feature gate could only fragment behavior for columns that already exist unguarded. This mirrors the `void` precedent.

Notes for reviewers (not part of the proposed spec text):

- The annotation members (`class` / `pyClass` / `serializedClass`) are Spark-specific and shown only as an example; the protocol fixes only `type` and `sqlType` and treats the rest as an opaque, engine-defined set. Generated columns and column defaults are deliberately left to the general rule ("a UDT column is subject to the same rules as its `sqlType`"): Delta does not restrict them for a UDT today, and a UDT-typed generated column or default is in any case not expressible, so the spec does not call them out.
- Nested UDTs are forbidden because they do not round-trip: an engine that reconstructs a UDT from its declared type ignores the serialized inner `sqlType`, silently dropping the nested annotation.
- On adoption, the corresponding `PROTOCOL.md` sections (statistics, type widening, column mapping, IcebergCompat) should be updated to reference UDT, rather than only this standalone section.

--------

> ***Add a new "User-Defined Types" section after the [Map Type](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#map-type) section within [Schema Serialization Format](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format).***

# User-Defined Types

A user-defined type (UDT) is an engine-specific logical annotation over an ordinary physical type. It introduces no new physical representation: the column is stored on disk exactly as its `sqlType`. The annotation identifies engine-specific code that converts between the stored `sqlType` value and a richer in-memory object (for example, a Spark ML vector); that code is outside the scope of this protocol, and Delta neither loads nor runs it. A `udt` is not gated by a table feature; a client that does not recognize `udt` should read the column as its `sqlType` rather than reject the schema.

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

- `type`: the string `"udt"`.
- `sqlType`: any valid Delta type (see [The `sqlType`](#the-sqltype)). This is the physical, on-disk representation of the column.

Every other member forms the **annotation**: an open, engine-defined set that identifies the engine-specific type and where its conversion code lives. Delta does not define or interpret these members. Each value MUST be a JSON string or JSON null, and readers and writers MUST preserve every member verbatim. An engine emits only the members meaningful to it, and MAY emit none: `{type, sqlType}` alone is valid, and is then indistinguishable to a reader from its `sqlType`. A reader that does not recognize the members reads the column as its `sqlType`. Whether a missing engine-specific member is an error is the engine's decision, not the protocol's.

**What the annotation is for.** A UDT pairs the stored physical type (`sqlType`) with a reference to engine-specific code that converts between the stored value and the engine's own richer representation, in both directions: reading turns a stored `sqlType` value into the rich object, and writing turns a rich object back into a `sqlType` value. Only the `sqlType` value is ever stored. An engine that recognizes the reference uses its code to convert; one that does not reads the `sqlType` value, which is already correct.

**Spark's members (example).** Spark records where its conversion code lives as:

- `class`: the JVM type implementing the conversion (e.g. `org.apache.spark.ml.linalg.VectorUDT`). Present for JVM-defined UDTs and Python UDTs with a JVM peer.
- `pyClass`: the Python type (e.g. `pyspark.ml.linalg.VectorUDT`), or `null` when there is no Python pairing (the key is still present).
- `serializedClass`: a base64-encoded Python type, used when there is no JVM `class`.

Each is only an identifier Spark uses to find its code; Delta does not interpret any of them. A Spark `udt` therefore takes one of two shapes, `{type, sqlType, class, pyClass}` or `{type, sqlType, pyClass, serializedClass}` (Python-only). These are an example of the mechanism, not the set of members the protocol requires. A different engine uses its own annotation members, or none, and does not set Spark's `class` / `pyClass`.

The Parquet column data carries no `udt` marker: a UDT column is physically indistinguishable from a column of its `sqlType`, and the UDT semantics are carried solely by the annotation in `metaData.schemaString`. (Spark also copies its full schema, annotation included, into the Parquet file footer's key-value metadata under `org.apache.spark.sql.parquet.row.metadata`; that copy is Spark-specific and not required. A reader obtains the schema from `metaData.schemaString` and MUST NOT depend on the Parquet footer for UDT information.)

## The `sqlType`

`sqlType` is a Delta type, expressed in the same serialization used everywhere else in `metaData.schemaString`, not a Parquet type. It may be a primitive or a nested struct / array / map, and may itself contain any Delta type, subject to two constraints:

- **Feature conformance.** A `udt`'s `sqlType` (recursively) MUST consist only of types supported by the table's protocol version and enabled table features, exactly as if the column were declared with that type directly. If the `sqlType` contains a feature-gated type (for example `timestampNtz` or `variant`), that feature MUST be enabled on the table, and table-feature detection MUST descend into the `sqlType`. A `udt` does not exempt its `sqlType` from any type's requirements.
- **No nesting.** A `udt`'s `sqlType` MUST NOT be, or contain, another `udt`.

A UDT column is permitted anywhere a type is permitted: as a top-level column, a nested struct field, an array element type, or a map key or value type.

## Reader and writer requirements

A reader:

- MUST interpret a `udt` column as its `sqlType` for all physical operations (Parquet read, projection, statistics).
- that does not implement the engine code identified by the annotation MUST still read the column as its `sqlType`; the stored values are correct, and only reconstruction of the richer object is unavailable.
- MUST reject a `udt` object that has no `sqlType`, or whose annotation contains a member (other than `type` and `sqlType`) whose value is neither a JSON string nor JSON null.
- that re-serializes the schema MUST retain the annotation verbatim.

A writer:

- MUST store a UDT column's data physically as its `sqlType`.
- MUST preserve `udt` columns: it MUST NOT drop the annotation or downgrade a column to its bare `sqlType`, and MUST retain `type`, `sqlType`, and every annotation member with its string-or-null value.
- MUST NOT introduce a non-string annotation member.

## Constraints and interactions with other features

Except where stated below, a UDT column is subject to the same protocol rules as a column of its `sqlType`.

- **Column mapping.** A UDT is a leaf: the enclosing field receives a `physicalName` and field id, and column mapping does not recurse into the `sqlType`, whose fields are matched by their intrinsic UDT-defined structure.
- **Statistics and data skipping.** A UDT column is not eligible for `minValues` / `maxValues`, and readers MUST NOT perform min/max data skipping over one. A per-column `nullCount` is recorded, with the UDT treated as a single statistics leaf (statistics do not descend into the `sqlType`). Per-file `numRecords` is unaffected.
- **Partitioning and clustering.** A UDT column MUST NOT be a partition column or a clustering column: partition values have no serialization for a UDT, and clustering requires the `minValues` / `maxValues` a UDT does not have.
- **Identity columns.** A UDT column MUST NOT be an identity column.
- **Type widening.** A UDT does not participate in type changes: a writer MUST NOT widen a UDT to another type, nor another type to a UDT.
- **IcebergCompat.** Iceberg has no UDT concept; when any of `icebergCompatV1`, `icebergCompatV2`, or `icebergCompatV3` is enabled, a writer MUST reject a schema containing a UDT column.
