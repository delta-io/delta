# Parquet Compression Codec
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6323

Delta Lake tables store data in parquet files, and parquet supports multiple compression codecs. Currently, the Delta protocol does not formally document how compression is specified or which codecs are supported. This RFC introduces the `delta.parquet.compression.codec` table property to persistently configure the compression codec used for new parquet files.

--------

> ***New top-level Section just before [Appendix](#appendix)***

# Table Properties

Delta Lake tables support a set of properties stored in the `configuration` field of the `metaData` action that control various aspects of table behavior.

## Overview

Property | Description
-|-
[`delta.parquet.compression.codec`](#deltaparquetcompressioncodec) | Compression codec for new Parquet data and checkpoint files

## Property Details

### delta.parquet.compression.codec

Specifies the compression codec writers SHOULD use when writing new Parquet data and checkpoint files. Changing this property does not affect existing files; a table may contain files written with different codecs, which is a normal and expected state.

Supported values (matched case-insensitively):

Value | Description
-|-
`uncompressed` or `none` | No compression
`snappy` | Snappy compression (recommended default)
`gzip` | GZIP compression
`lz4` | (Deprecated) LZ4 compression (Hadoop framing). For backwards compatibility only.
`zstd` | Zstandard compression

When the property is absent, writers SHOULD default to `zstd`. If a writer does not support the specified codec, it SHOULD abort with an appropriate error or fall back to a default codec.

Readers SHOULD be able to read parquet files compressed with any of the supported codecs, regardless of the current table property value.
