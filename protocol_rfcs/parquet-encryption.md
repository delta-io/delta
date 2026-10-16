# Parquet Encryption
**Associated Github issue for discussions: https://github.com/delta-io/delta/issues/6195**

This RFC proposes integrating Parquet modular encryption into Delta Lake,
to enable column-level access control, and control access to data separately
from access to the underlying storage.

--------

> ***New row appended to both the `add` and `remove` action schemas in [Add File and Remove File](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#add-file-and-remove-file)***

Field Name | Data Type | Description | optional/required
-|-|-|-
encryptionKeyMetadata | [Key Metadata Struct](#encryption-key-metadata) | Contains metadata that can be used to derive the footer and column encryption keys required to decrypt data in the file. Must be set if the file uses Parquet modular encryption | optional

> ***New section added to the [Appendix](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#appendix)***

## Encryption Key Metadata

The schema of the `encryptionKeyMetadata` field in `add` and `remove` actions is as follows:

Field Name | Data Type | Description | optional/required
-|-|-|-
footerKey | `String` | Metadata that can be used to derive the footer encryption key required to decrypt data in the file, or verify its integrity for files with a plaintext footer | required
columnKeys | `Map[String, String]` | A map from column names to metadata that can be used to derive per-column encryption keys. For tables that use column mapping, column names are the physical column names. This field is not set for files that use uniform encryption. | optional

The format of the key metadata itself is not prescribed by the Delta protocol and may be implementation specific.

> ***New section added before [Additional Requirements for Writers](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#additional-requirements-for-writers)***

# Parquet Encryption

The Parquet encryption feature allows tables to store data that is encrypted using [Parquet modular encryption](https://github.com/apache/parquet-format/blob/master/Encryption.md).

## Enablement

The `parquetEncryption` table feature is supported and active when:
- The table is on Reader Version 3 and Writer Version 7.
- The feature `parquetEncryption` exists in the table `protocol`'s `readerFeatures` and `writerFeatures`.

## Table Properties for Parquet Encryption

The following table properties are used to configure Parquet encryption:

### `delta.encryption.kms_id`

Identifier of the type of Key Management System (KMS) to use for encryption.

### `delta.encryption.kms_configuration`

Arbitrary configuration string, with a format specific to the type of KMS used. This may be a JSON formatted object for example.

### `delta.encryption.footer_key`

The master key ID for footer encryption. When plaintext footers are enabled, this is used to sign the footer.

### `delta.encryption.plaintext_footer`

Boolean to control whether footers are written unencrypted. Defaults to false.

### `delta.encryption.column_keys`

A list of columns to encrypt, with master key IDs.
Formatted as: `<masterKeyId>:<columnName>;<masterKeyId>:<columnName>,<columnName>`.
A semi-colon (`;`) separates key entries and a comma (`,`) separates columns sharing the same master key.

If this is empty or unspecified and a footer key is specified, uniform encryption is used, where all columns are encrypted with the footer key.

Column names can be dot-separated to specify encryption of nested columns, using the [field path](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#field-path) scheme.
If an encryption key is configured for a column that contains nested fields, all of its nested fields are encrypted with the specified key.
When the column mapping feature is enabled, the column names must be physical column names so that they are stable.

## Writer Requirements for Parquet Encryption

Writers must ensure the `parquetEncryption` table feature is present in the table protocol's `writerFeatures` and `readerFeatures` when enabling encryption (setting any of the `delta.encryption.*` table properties).

Writers must validate the encryption table properties, applying the following rules:
- If column keys are configured, a footer key must also be specified.
- Column names specified must all correspond to valid column names in the table.
- A column must not appear multiple times in the `delta.encryption.column_keys` property.
- An encryption key must not be set for a nested column if one is also set for one of its ancestors.
- An encryption key cannot be configured for a partition column.

When the `delta.encryption.footer_key` table property is set, writers must use the configured table properties
to control encryption of the Parquet file,
in accordance with the [Parquet modular encryption](https://github.com/apache/parquet-format/blob/master/Encryption.md) specification.
The footer must be encrypted with the specified master key, optionally using an envelope encryption scheme where
the master key is not used directly but used to encrypt a data encryption key or intermediate key-encryption key.
If `delta.encryption.plaintext_footer` is true, the footer is stored in plaintext and signed with the specified key.

If `delta.encryption.column_keys` is specified, only the configured columns are encrypted, using their corresponding
master encryption keys.

Any `add` actions generated for an encrypted file must have the [`encryptionKeyMetadata` field](#encryption-key-metadata) set,
and the `footerKey` field must be non-empty.
If per-column encryption keys are configured, the `columnKeys` field in the `encryptionKeyMetadata` must also be set and
contain non-empty strings for each encrypted column.
When generating a `remove` action for an encrypted file, the `encryptionKeyMetadata` must be propagated from the
original `add` action.

Writers may additionally choose to store key metadata embedded in the Parquet file,
for example in the [`key_metadata`](https://github.com/apache/parquet-format/blob/2076361bb64e2de9ca6a8d06eda025a6fa4e9df6/src/main/thrift/parquet.thrift#L984)
field of the `EncryptionWithColumnKey` Thrift struct for column keys.

Note that it is valid for a table to have the `parquetEncryption` feature enabled, but no footer key configured,
for example if encryption was previously used and then disabled. In this case, data files are written unencrypted
and no `encryptionKeyMetadata` is written.

### Statistics for Encrypted Columns

The statistics associated with an encrypted column, such as its minimum and maximum value, may reveal
sensitive information.
Because metadata files containing per-file statistics, such as Delta log files and checkpoint files, are not encrypted,
writers must not store any per-file statistics for columns that are encrypted.
When `delta.encryption.column_keys` is not set and uniform encryption is used,
column statistics must not be written to Delta metadata files.

Note that column statistics may be stored within the Parquet file metadata, allowing row-group and page skipping.
The Parquet encryption specification ensures that they are encrypted appropriately.

### Property Updates

Writers should allow any Parquet encryption properties to be changed.
Encryption property changes have no effect on any previously written data files,
but only affect future writes.

The exceptions to this are the `delta.encryption.kms_id` and `delta.encryption.kms_configuration` properties.
These are used by readers, so it is a user's responsibility to ensure that any changes to these properties
do not break the ability to decrypt any previously written data.

Writers that allow enabling encryption on a previously unencrypted table,
or configuring an encryption key for a previously unencrypted column,
should provide a way for users to ensure any previously written
and unencrypted data is purged from the table and rewritten in encrypted form.

### Schema Changes

Writers must ensure that the table encryption properties remain compatible with any schema changes.
One change that could cause drift is removing a column from a table. In this scenario,
the `delta.encryption.column_keys` property must be updated to remove any references to the
removed column.
When column mapping is enabled, a column rename does not
require updates to the encryption properties because the column
encryption keys must be configured in terms of the physical column names, which do not change.

## Reader Requirements for Parquet Encryption

When reading a data file that has `encryptionKeyMetadata` set in its corresponding `add` action,
a reader must use the configured KMS and `encryptionKeyMetadata`, or encryption metadata embedded in the Parquet file,
to determine decryption keys for reading the data file,
or raise an error if it is unable to do so.

Readers must not use the `delta.encryption.footer_key` or `delta.encryption.column_keys`
table properties to derive decryption keys, because these may change over time and their
current values may not correspond to the settings used to write a data file.

> ***New paragraph added to [Table Properties](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-properties)***

For table properties specific to the Parquet Encryption feature, see [Table Properties for Parquet Encryption](#table-properties-for-parquet-encryption).

> ***New row added to [Valid Feature Names in Table Features](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#valid-feature-names-in-table-features)***

Feature | Name | Readers or Writers?
-|-|-
[Parquet Encryption](#parquet-encryption) | `parquetEncryption` | Readers and writers
