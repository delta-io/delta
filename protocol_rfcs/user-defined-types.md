# User-Defined Types (UDT)
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/7559**

This protocol change documents how Spark currently defines `UserDefinedType` (`udt`) columns and proposes adding them to the Delta protocol so they are available to all connectors. Spark writes `udt` columns into `metaData.schemaString`, but the protocol's schema type system defines only primitive / struct / array / map (plus `variant`), so a `udt` field is non-conformant under the current spec even though such columns already exist in tables written by earlier clients, and a reader that rejects the unknown type fails to read the entire table.

Like [`void`](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types) and interval types, `udt` is documented here post-facto and is **not** gated by a table feature. A `udt` introduces no new physical representation: it is an engine-specific *annotation* over an existing physical type (its `sqlType`). A reader that does not run the engine's deserialization code and simply reads the `sqlType` reads correct data. Unlike `timestampNtz` / `variant`, which introduced new physical semantics that readers must opt into, there is nothing here for a reader to opt into, and a feature gate could only fragment behavior for columns that already exist unguarded. This mirrors the `void` precedent.

Notes for reviewers (not part of the proposed spec text):

- The annotation members (`class` / `pyClass` / `serializedClass`) are Spark-specific and shown only as an example; the protocol fixes only `type` and `sqlType` and treats the rest as an opaque, engine-defined set.
- Generated columns and column defaults are forbidden for UDT columns (see Constraints). This is a new restriction: Delta Spark does not reject them today (it accepts a UDT generated column that copies another UDT column), so enforcing it needs a writer-side check.
- Nested UDTs are forbidden because they do not round-trip: an engine that reconstructs a UDT from its declared type ignores the serialized inner `sqlType`, silently dropping the nested annotation.
- On adoption, the corresponding `PROTOCOL.md` sections (statistics, type widening, column mapping, IcebergCompat) should be updated to reference UDT, rather than only this standalone section.

--------

> ***Add a new "User-Defined Types" section after the [Map Type](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#map-type) section within [Schema Serialization Format](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format).***

# User-Defined Types

A user-defined type (UDT) is an engine-specific logical annotation over an ordinary physical type. It introduces no new physical representation: a UDT's *physical* type is exactly its `sqlType`. The annotation identifies engine-specific code that converts between the stored `sqlType` value and a richer in-memory object (for example, a Spark ML vector); that code is outside the scope of this protocol, and the protocol specifies neither how to load nor to run it. A `udt` is not gated by a table feature; a client that does not recognize `udt` should read the column as its `sqlType` rather than reject the schema.

A UDT field's `type` in `metaData.schemaString` is a JSON object. At its simplest, a `udt` over a `long` physical type, shown as it appears within a table's `schemaString`:

```json
{
  "type": "struct",
  "fields": [
    { "name": "id", "type": { "type": "udt", "class": "com.example.IdUDT", "sqlType": "long" }, "nullable": true, "metadata": {} }
  ]
}
```

A fuller field-level example is Spark ML's `VectorUDT`, whose `sqlType` is a struct:

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
- `sqlType`: any valid Delta type other than `udt`. This is the physical, on-disk representation of the column.

`sqlType` is expressed in the same serialization used everywhere else in `metaData.schemaString`, not a Parquet type. It may be a primitive or a nested struct / array / map, subject to two constraints:

- **Feature conformance.** A `udt`'s `sqlType` (recursively) MUST consist only of types supported by the table's protocol version and enabled table features, exactly as if the column were declared with that type directly. If the `sqlType` contains a feature-gated type (for example `timestampNtz` or `variant`), that feature MUST be enabled on the table, and table-feature detection MUST descend into the `sqlType`. A `udt` does not exempt its `sqlType` from any type's requirements.
- **No nesting.** A `udt`'s `sqlType` MUST NOT contain another `udt`.

A UDT is permitted anywhere its `sqlType` is permitted: as a top-level column, a nested struct field, an array element type, or a map key or value type. As a map key, a UDT is allowed only when its `sqlType` is itself a valid map-key type.

Every other member forms the **annotation**: an open, engine-defined set that identifies the engine-specific type and where its conversion code lives. Delta does not define or interpret these members. Each value MUST be a JSON string or JSON null. An engine emits only the members meaningful to it, and MAY emit none: `{type, sqlType}` alone is valid, and is then indistinguishable to a reader from its `sqlType`. A reader that does not recognize the members reads the column as its `sqlType`. Whether a missing engine-specific member is an error is the engine's decision, not the protocol's.

**Spark's members (example).** Spark records where its conversion code lives as:

- `class`: the JVM type implementing the conversion (e.g. `org.apache.spark.ml.linalg.VectorUDT`). Present for JVM-defined UDTs and Python UDTs with a JVM peer.
- `pyClass`: the Python type (e.g. `pyspark.ml.linalg.VectorUDT`), or `null` when there is no Python pairing (the key is still present).
- `serializedClass`: a base64-encoded Python type, used when there is no JVM `class`.

Each is only an identifier Spark uses to find its code; Delta does not interpret any of them. A Spark `udt` therefore takes one of two shapes, `{type, sqlType, class, pyClass}` or `{type, sqlType, pyClass, serializedClass}` (Python-only). These are an example of the mechanism, not the set of members the protocol requires. A different engine uses its own annotation members, or none, and does not set Spark's `class` / `pyClass`.

The Parquet column data carries no `udt` marker: a UDT column is physically indistinguishable from a column of its `sqlType`, and the UDT semantics are carried solely by the annotation in `metaData.schemaString`. (Spark also copies its full schema, annotation included, into the Parquet file footer's key-value metadata under `org.apache.spark.sql.parquet.row.metadata`; that copy is Spark-specific and not required. A reader obtains the schema from `metaData.schemaString` and MUST NOT depend on the Parquet footer for UDT information.)

## Reader and writer requirements

A reader has no UDT-specific requirements: a UDT's physical type is its `sqlType`, so a client reads the column as its `sqlType` for all physical operations (Parquet read, projection, expression evaluation) whether or not it recognizes the annotation. A `udt` missing its `sqlType`, or carrying an annotation member whose value is neither a JSON string nor JSON null, is not a valid `udt`.

A writer:

- MUST store a UDT column's data physically as its `sqlType`.
- MUST preserve `udt` columns: it MUST NOT drop the annotation or downgrade a column to its bare `sqlType`, MUST retain `type`, `sqlType`, and every annotation member, and MUST NOT introduce an annotation member whose value is neither a JSON string nor JSON null.

## Constraints and interactions with other features

Except where stated below, a UDT column is subject to the same protocol rules as a column of its `sqlType`.

- **Column mapping.** A UDT is a leaf: the enclosing field receives a `physicalName` and field id, but column mapping does not descend into the `sqlType`. The `sqlType`'s internal fields carry no `physicalName` or field id and are matched by the names defined in the `sqlType` itself, exactly as the same type would be in a table without column mapping. This holds in both `id` and `name` modes.
- **Statistics and data skipping.** A UDT column is not eligible for `minValues` / `maxValues`, and readers MUST NOT perform min/max data skipping over one. A per-column `nullCount` is recorded, with the UDT treated as a single statistics leaf (statistics do not descend into the `sqlType`). Per-file `numRecords` is unaffected.
- **Partitioning and clustering.** A UDT column MUST NOT be a partition column or a clustering column: partition values have no serialization for a UDT, and clustering requires the `minValues` / `maxValues` a UDT does not have.
- **Identity columns.** A UDT column MUST NOT be an identity column.
- **Generated columns and column defaults.** A UDT column MUST NOT be a generated column, and MUST NOT carry a column default.
- **Type widening.** A UDT does not participate in type changes: a writer MUST NOT widen a UDT to another type, nor another type to a UDT, nor widen any type inside a UDT's `sqlType`.
- **IcebergCompat.** Iceberg has no UDT concept; when any of `icebergCompatV1`, `icebergCompatV2`, or `icebergCompatV3` is enabled, a writer MUST reject a schema containing a UDT column.
