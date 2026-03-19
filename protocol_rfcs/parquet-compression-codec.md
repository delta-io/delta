# Parquet Compression Codec
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6323

Delta Lake tables store data in Parquet files, and Parquet supports multiple compression codecs. Currently, the Delta protocol does not formally document how compression is specified or which codecs are supported. This RFC introduces the `delta.parquet.compression.codec` table property to persistently configure the compression codec used for new data files, and defines the semantics of tables that contain data files with mixed compression codecs.

--------

> ***New Section after [Append-only Tables](#append-only-tables)***

## Parquet Compression Codec

Delta tables store data in Parquet files. Parquet supports multiple compression codecs, and different data files within the same table may use different compression codecs. This is valid and readers must be able to read all data files regardless of which codec was used to write them.

The table property `delta.parquet.compression.codec` controls the compression codec that writers use when writing new Parquet data files. When this property is not set, writers may choose any supported codec. Historically, `snappy` has been
used as a default.

Supported values for `delta.parquet.compression.codec` are:

Value | Description
-|-
`uncompressed` | No compression is applied
`snappy` | Snappy compression (recommended default)
`gzip` | GZIP compression
`lz4` | (Deprecated) LZ4 compression (using the Hadoop LZ4 framing). For backwards compability only. 
`zstd` | Zstandard compression

### Writer Requirements for Parquet Compression Codec

When the table property `delta.parquet.compression.codec` is present in the table metadata:
- Writers SHOULD apply the specified compression codec when writing new Parquet data and checkpoint files to the table.
- If a writer does not support the specified codec, it MAY abort the write and surface an appropriate error.
- Writers MAY override the table-level codec for a specific write operation (e.g. via a per-operation write option). The override applies only to that write; it does not change the table property.

When the table property `delta.parquet.compression.codec` is absent:
- Writers SHOULD use `snappy` compression.
- Writers MAY use any other supported codec.

### Reader Requirements for Parquet Compression Codec

Readers MUST be able to read all data files in a table regardless of which compression codec was used to write them. In particular:
- A table may contain data files written with different compression codecs (a "mixed-codec" table). This is a normal and expected state, not an error condition.
- Readers MUST NOT reject or fail when encountering a data file whose codec differs from the `delta.parquet.compression.codec` table property.

### Example

The following `metaData` action configures a table to use Zstandard compression for new writes:

```json
{
  "metaData": {
    "id": "af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
    "format": {"provider": "parquet", "options": {}},
    "schemaString": "...",
    "partitionColumns": [],
    "configuration": {
      "delta.parquet.compression.codec": "zstd"
    }
  }
}
```

A table with the above property may already contain data files written with a different codec (e.g. snappy) from previous writes. Both old and new data files may coexist in the table and must be readable together.
