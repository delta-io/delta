# IcebergWriterCompatV1
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/4284

This protocol change introduces a compatibility flag, which ensures that a delta table can be safely
read and written as an Apache Iceberg™ format table, similar to
[IcebergCompatV1](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v1)
and
[IcebergCompatV2](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#iceberg-compatibility-v2).

--------

# IcebergWriterCompatV1
> ***New Section after [Iceberg Compatibility V2](#iceberg-compatibility-v2)***

This table feature (`icebergWriterCompatV1`) ensures that Delta tables can be converted to Apache
Iceberg™ format, though this table feature does not implement or specify that conversion.

To support this feature:
- Since this table feature depends on Column Mapping, the table must be on Reader Version = 2, or it must be on Reader Version >= 3 and the feature `columnMapping` must exist in the `protocol`'s `readerFeatures`.
- The table must be on Writer Version 7.
- The feature `icebergCompatV2` must exist in the table protocol's `writerFeatures`.
- The feature `icebergWriterCompatV1` must exist in the table protocol's `writerFeatures`.

This table feature is enabled when the table property `delta.enableIcebergWriterCompatV1` is set to `true`.

## Writer Requirements for IcebergWriterCompatV1
For `IcebergWriterCompatV1` writers must ensure:

- The table is using [Column Mapping](#column-mapping) and that it is set to `id` mode.
  - Note this is a tightening of the `IcebergCompatV2` requirement which supports `name` and `id` mode.

- Each field _must_ have a column mapping physical name that is exactly `col-[column id]`. That is the `delta.columnMapping.physicalName` in the column metadata _must_ be equal to `col-[delta.columnMapping.id]`. The following is an example compliant schema definition:

  ```json
{
  "type": "struct",
  "fields": [
    {
      "name": "a",
      "type": "integer",
      "nullable": false,
      "metadata": {
        "delta.columnMapping.id": 1,
        "delta.columnMapping.physicalName": "col-1"
      }
    },
    {
      "name": "b",
      "type": "string",
      "nullable": false,
      "metadata": {
        "delta.columnMapping.id": 2,
        "delta.columnMapping.physicalName": "col-2"
      }
    }
  ]
}
  ```

- The table does not contain any columns with the type `byte` or `short`
  - Note that these types _are_ allowed by `IcebergCompatV2`
  - Therefore the list of allowed types for a table with `IcebergWriterCompatV1` enabled is: [`integer`, `long`, `float`, `double`, `decimal`, `string`, `binary`, `boolean`, `timestamp`, `timestampNTZ`, `date`, `array`, `map`, `struct`].

- [Iceberg Compatibility V2](#iceberg-compatibility-v2) is **enabled** on the table.
  - This means _all_ the conditions that [Iceberg Compatibility V2](#iceberg-compatibility-v2) imposes are met.

- Any enabled features are in the [allowlist](#allowed-supported-list-of-features)

- All [Disallowed features](#disallowed-features) are not supported and/or inactive (see below)

### Disallowed Features
For this section, we use the specific meanings of "supported" and "active" from [Supported Features](#supported-features). All the following features must not be used in the table. For legacy features (any feature introduced before writer version 7), the feature _can_ be "supported", but must _not_ be "active".

| Feature                                                                                           | Legacy | Can be ["supported"](#supported-features)? | Not Active Check                                                                                                                                                                                                                                         |
|---------------------------------------------------------------------------------------------------|--------|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [column invariants](#column-invariants)                                                           | Yes    | Yes, if not active                         | No column includes `delta.invariants` in its [Metadata]                                                                                                                                                                                                  |
| [Change Data Feed](#add-cdc-file)                                                                 | Yes    | Yes, if not active                         | The `delta.enableChangeDataFeed` configuration flag in the [Metadata] of the table does not exist (or is `disabled`?)                                                                                                                                    |
| [CHECK Constraints](#check-constraints)                                                           | Yes    | Yes, if not active                         | No keys in the `configuration` field of [Metadata] start with `delta.constraints.`.                                                                                                                                                                      |
| [Identity Columns](#identity-columns)                                                             | Yes    | Yes, if not active                         | No columns exist in the schema with any of the properties specified in [Identity Columns](#identity-columns) in the column metadata: `delta.identity.start`, `delta.identity.step`, `delta.identity.highWaterMark`, `delta.identity.allowExplicitInsert` |
| [Generated Columns](#default-columns)                                                             | Yes    | Yes, if not active                         | No column metadata contains the key `delta.generationExpression`                                                                                                                                                                                         |
| [Default Columns](#default-columns)                                                               | No     | No                                         | N/A                                                                                                                                                                                                                                                      |
| [Row Tracking](#row-tracking)                                                                     | No     | No                                         | N/A                                                                                                                                                                                                                                                      |
| [Collations](https://github.com/delta-io/delta/blob/master/protocol_rfcs/collated-string-type.md) | No     | No                                         | N/A                                                                                                                                                                                                                                                      |
| [Variant Types](#variant-data-type)                                                               | No     | No                                         | N/A                                                                                                                                                                                                                                                      |
### Allowed Supported list of features
To ensure that future features do not break tables with `IcebergWriterCompatV1` enabled, all enabled features must also be checked against an allowlist. Any enabled table features _must_ be in the list: [`appendOnly`, `columnMapping`, `icebergWriterCompatV1`, `icebergCompatV2`, `domainMetadata`, `vacuumProtocolCheck`, `v2Checkpoint`, `inCommitTimestamp`, `clustering`, `timestampNtz`, `typeWidening`]

Additionally, the following features are allowed to be "supported", but must not be "active" (see [Disallowed Features](#disallowed-features)): [`invariants`, `changeDataFeed`, `checkConstraints`, `identityColumns`, `generatedColumns`]. These features, if supported, must be verified to be "inactive" via the checks specified above.

We allow these legacy features to be "supported" because protocol updates can cause features to be carried over even though they are not in use. For example, if a table is on writer version 2, and then is updated to version 7, `invariants` can appear in the `writerFeatures` list because it was implicitly supported at version 2, even if it was not in use.

[Metadata]: #change-metadata

