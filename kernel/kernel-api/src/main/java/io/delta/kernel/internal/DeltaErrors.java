/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.internal;

import static java.lang.String.format;

import io.delta.kernel.commit.CommitFailedException;
import io.delta.kernel.exceptions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.util.SchemaIterable;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TypeChange;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Contains methods to create user-facing Delta exceptions. */
public final class DeltaErrors {
  private DeltaErrors() {}

  public static KernelException missingCheckpoint(String tablePath, long checkpointVersion) {
    return new InvalidTableException(
        tablePath, String.format("Missing checkpoint at version %s", checkpointVersion));
  }

  public static KernelException versionBeforeFirstAvailableCommit(
      String tablePath, long versionToLoad, long earliestVersion) {
    String message =
        String.format(
            "%s: Cannot load table version %s as the transaction log has been truncated due to "
                + "manual deletion or the log/checkpoint retention policy. The earliest available "
                + "version is %s.",
            tablePath, versionToLoad, earliestVersion);
    return new KernelException(message);
  }

  public static KernelException versionToLoadAfterLatestCommit(
      String tablePath, long versionToLoad, long latestVersion) {
    String message =
        String.format(
            "%s: Cannot load table version %s as it does not exist. "
                + "The latest available version is %s.",
            tablePath, versionToLoad, latestVersion);
    return new KernelException(message);
  }

  public static KernelException timestampBeforeFirstAvailableCommit(
      String tablePath,
      long providedTimestamp,
      long earliestCommitTimestamp,
      long earliestCommitVersion) {
    String message =
        String.format(
            "%s: The provided timestamp %s ms (%s) is before the earliest available version %s. "
                + "Please use a timestamp greater than or equal to %s ms (%s).",
            tablePath,
            providedTimestamp,
            formatTimestamp(providedTimestamp),
            earliestCommitVersion,
            earliestCommitTimestamp,
            formatTimestamp(earliestCommitTimestamp));
    return new KernelException(message);
  }

  public static KernelException timestampAfterLatestCommit(
      String tablePath,
      long providedTimestamp,
      long latestCommitTimestamp,
      long latestCommitVersion) {
    String message =
        String.format(
            "%s: The provided timestamp %s ms (%s) is after the latest available version %s. "
                + "Please use a timestamp less than or equal to %s ms (%s).",
            tablePath,
            providedTimestamp,
            formatTimestamp(providedTimestamp),
            latestCommitVersion,
            latestCommitTimestamp,
            formatTimestamp(latestCommitTimestamp));
    return new KernelException(message);
  }

  public static CommitRangeNotFoundException noCommitFilesFoundForVersionRange(
      String tablePath, long startVersion, Optional<Long> endVersionOpt) {
    return new CommitRangeNotFoundException(tablePath, startVersion, endVersionOpt);
  }

  public static KernelException startVersionNotFound(
      String tablePath, long startVersionRequested, Optional<Long> earliestAvailableVersion) {
    String message =
        String.format(
            "%s: Requested table changes beginning with startVersion=%s but no log file found for "
                + "version %s.",
            tablePath, startVersionRequested, startVersionRequested);
    if (earliestAvailableVersion.isPresent()) {
      message =
          message
              + String.format(" Earliest available version is %s", earliestAvailableVersion.get());
    }
    return new KernelException(message);
  }

  public static KernelException endVersionNotFound(
      String tablePath, long endVersionRequested, long latestAvailableVersion) {
    String message =
        String.format(
            "%s: Requested table changes ending with endVersion=%d but no log file found for "
                + "version %d. Latest available version is %d",
            tablePath, endVersionRequested, endVersionRequested, latestAvailableVersion);
    return new KernelException(message);
  }

  public static KernelException invalidVersionRange(long startVersion, long endVersion) {
    String message =
        String.format(
            "Invalid version range: requested table changes for version range [%s, %s]. "
                + "Requires startVersion >= 0 and endVersion >= startVersion.",
            startVersion, endVersion);
    return new KernelException(message);
  }

  public static KernelException invalidResolvedVersionRange(
      String tablePath, long startVersion, long endVersion) {
    String message =
        String.format(
            "%s: Invalid resolved version range: after timestamp resolution, "
                + "startVersion=%d > endVersion=%d. "
                + "Please adjust the provided timestamp boundaries.",
            tablePath, startVersion, endVersion);
    return new KernelException(message);
  }

  /* ------------------------ PROTOCOL EXCEPTIONS ----------------------------- */
  public static UnsupportedProtocolVersionException unsupportedReaderProtocol(
      String tablePath, int tableReaderVersion) {
    return new UnsupportedProtocolVersionException(
        tablePath,
        tableReaderVersion,
        UnsupportedProtocolVersionException.ProtocolVersionType.READER);
  }

  public static UnsupportedProtocolVersionException unsupportedWriterProtocol(
      String tablePath, int tableWriterVersion) {
    return new UnsupportedProtocolVersionException(
        tablePath,
        tableWriterVersion,
        UnsupportedProtocolVersionException.ProtocolVersionType.WRITER);
  }

  public static UnsupportedTableFeatureException unsupportedTableFeature(String feature) {
    String message =
        String.format(
            "Unsupported Delta table feature: table requires feature \"%s\" "
                + "which is unsupported by this version of Delta Kernel.",
            feature);
    return new UnsupportedTableFeatureException(null, feature, message);
  }

  public static UnsupportedTableFeatureException unsupportedReaderFeatures(
      String tablePath, Set<String> readerFeatures) {
    String message =
        String.format(
            "Unsupported Delta reader features: table `%s` requires reader table features [%s] "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, String.join(", ", readerFeatures));
    return new UnsupportedTableFeatureException(tablePath, readerFeatures, message);
  }

  public static UnsupportedTableFeatureException unsupportedWriterFeatures(
      String tablePath, Set<String> writerFeatures) {
    String message =
        String.format(
            "Unsupported Delta writer features: table `%s` requires writer table features [%s] "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, String.join(", ", writerFeatures));
    return new UnsupportedTableFeatureException(tablePath, writerFeatures, message);
  }

  public static KernelException columnInvariantsNotSupported() {
    String message =
        "This version of Delta Kernel does not support writing to tables with "
            + "column invariants present.";
    return new KernelException(message);
  }

  public static KernelException checkpointOnUnpublishedCommits(
      String tablePath, long version, long maxPublishedVersion) {
    String message =
        String.format(
            "Unable to create checkpoint: Snapshot at at path"
                + " `%s` with version %d has unpublished commits. "
                + "Max known published version is %d",
            tablePath, version, maxPublishedVersion);
    return new KernelException(message);
  }

  public static KernelException unsupportedDataType(DataType dataType) {
    return new KernelException("Kernel doesn't support writing data of type: " + dataType);
  }

  public static KernelException unsupportedStatsDataType(DataType dataType) {
    return new KernelException("Kernel doesn't support writing stats data of type: " + dataType);
  }

  public static KernelException unsupportedPartitionDataType(String colName, DataType dataType) {
    String msgT = "Kernel doesn't support writing data with partition column (%s) of type: %s";
    return new KernelException(format(msgT, colName, dataType));
  }

  public static KernelException duplicateColumnsInSchema(
      StructType schema, List<String> duplicateColumns) {
    String msg =
        format(
            "Schema contains duplicate columns: %s.\nSchema: %s",
            String.join(", ", duplicateColumns), schema);
    return new KernelException(msg);
  }

  public static KernelException conflictWithReservedInternalColumnName(String columnName) {
    return new KernelException(
        format("Cannot use column name '%s' because it is reserved for internal use", columnName));
  }

  public static KernelException invalidColumnName(String columnName, String unsupportedChars) {
    return new KernelException(
        format(
            "Column name '%s' contains one of the unsupported (%s) characters.",
            columnName, unsupportedChars));
  }

  public static KernelException requiresSchemaForNewTable(String tablePath) {
    return new TableNotFoundException(
        tablePath, "Must provide a new schema to write to a new table.");
  }

  public static KernelException requireSchemaForReplaceTable() {
    return new KernelException("Must provide a new schema for REPLACE TABLE");
  }

  public static KernelException tableAlreadyExists(String tablePath, String message) {
    return new TableAlreadyExistsException(tablePath, message);
  }

  public static KernelException dataSchemaMismatch(
      String tablePath, StructType tableSchema, StructType dataSchema) {
    String msgT =
        "The schema of the data to be written to the table doesn't match "
            + "the table schema. \nTable: %s\nTable schema: %s, \nData schema: %s";
    return new KernelException(format(msgT, tablePath, tableSchema, dataSchema));
  }

  public static KernelException statsTypeMismatch(
      String fieldName, DataType expected, DataType actual) {
    String msgFormat =
        "Type mismatch for field '%s' when writing statistics: expected %s, but found %s";
    return new KernelException(format(msgFormat, fieldName, expected, actual));
  }

  public static KernelException columnNotFoundInSchema(Column column, StructType tableSchema) {
    return new KernelException(
        format("Column '%s' was not found in the table schema: %s", column, tableSchema));
  }

  public static KernelException overlappingTablePropertiesSetAndUnset(Set<String> violatingKeys) {
    return new KernelException(
        format(
            "Cannot set and unset the same table property in the same transaction. "
                + "Properties set and unset: %s",
            violatingKeys));
  }

  /// Start: icebergCompat exceptions
  public static KernelException icebergCompatMissingNumRecordsStats(
      String compatVersion, DataFileStatus dataFileStatus) {
    throw new KernelException(
        format(
            "%s compatibility requires 'numRecords' statistic.\n DataFileStatus: %s",
            compatVersion, dataFileStatus));
  }

  public static KernelException icebergCompatIncompatibleVersionEnabled(
      String compatVersion, String incompatibleIcebergCompatVersion) {
    throw new KernelException(
        format(
            "%s: Only one IcebergCompat version can be enabled. Incompatible version enabled: %s",
            compatVersion, incompatibleIcebergCompatVersion));
  }

  public static KernelException icebergCompatUnsupportedTypeColumns(
      String compatVersion, List<DataType> dataTypes) {
    throw new KernelException(
        format("%s does not support the data types: %s.", compatVersion, dataTypes));
  }

  public static KernelException icebergCompatUnsupportedTypeWidening(
      String compatVersion, TypeChange typeChange) {
    throw new KernelException(
        format(
            "%s does not support type widening present in table: %s.", compatVersion, typeChange));
  }

  public static KernelException icebergCompatUnsupportedTypePartitionColumn(
      String compatVersion, DataType dataType) {
    throw new KernelException(
        format(
            "%s does not support the data type '%s' for a partition column.",
            compatVersion, dataType));
  }

  public static KernelException icebergCompatRequiresLiteralDefaultValue(
      String compatVersion, DataType dataType, String value) {
    throw new KernelException(
        format(
            "%s requires the default value to be literal with correct data types for "
                + "a column. '%s: %s' is invalid.",
            compatVersion, dataType, value));
  }

  public static KernelException icebergCompatIncompatibleTableFeatures(
      String compatVersion, Set<TableFeature> incompatibleFeatures) {
    throw new KernelException(
        format(
            "Table features %s are incompatible with %s.",
            incompatibleFeatures.stream()
                .map(TableFeature::featureName)
                .collect(Collectors.toList()),
            compatVersion));
  }

  public static KernelException icebergCompatRequiredFeatureMissing(
      String compatVersion, String feature) {
    throw new KernelException(
        format("%s: requires the feature '%s' to be enabled.", compatVersion, feature));
  }

  public static KernelException enablingIcebergCompatFeatureOnExistingTable(String key) {
    return new KernelException(
        String.format(
            "Cannot enable %s on an existing table. "
                + "Enablement is only supported upon table creation.",
            key));
  }

  public static KernelException icebergWriterCompatInvalidPhysicalName(List<String> invalidFields) {
    return new KernelException(
        String.format(
            "IcebergWriterCompatV1 requires column mapping field physical names be equal to "
                + "'col-[fieldId]', but this is not true for the following fields %s",
            invalidFields));
  }

  public static KernelException disablingIcebergCompatFeatureOnExistingTable(String key) {
    return new KernelException(
        String.format("Disabling %s on an existing table is not allowed.", key));
  }

  // End: icebergCompat exceptions

  // Start: Column Defaults Exceptions

  // TODO migrate this to InvalidTableException when table info is available at the call site
  public static KernelException defaultValueRequiresTableFeature() {
    return new KernelException(
        "Found column defaults in the schema but the table does not support the "
            + "columnDefaults table feature.");
  }

  public static KernelException defaultValueRequireIcebergV3() {
    return new KernelException(
        "In Delta Kernel, default values table feature requires "
            + "IcebergCompatV3 to be enabled.");
  }

  public static KernelException unsupportedDataTypeForDefaultValue(
      String fieldName, String fieldType) {
    return new KernelException(
        String.format(
            "Kernel does not support default value for " + "data type %s: %s",
            fieldType, fieldName));
  }

  public static KernelException nonLiteralDefaultValue(String value) {
    return new KernelException(
        String.format(
            "currently only literal values are supported for default values in Kernel."
                + " %s is an invalid default value",
            value));
  }

  // End: Column Defaults Exceptions

  public static KernelException partitionColumnMissingInData(
      String tablePath, String partitionColumn) {
    String msgT = "Missing partition column '%s' in the data to be written to the table '%s'.";
    return new KernelException(format(msgT, partitionColumn, tablePath));
  }

  public static KernelException enablingClusteringOnPartitionedTableNotAllowed(
      String tablePath, Set<String> partitionColNames, List<Column> clusteringCols) {
    return new KernelException(
        String.format(
            "Cannot enable clustering on a partitioned table '%s'. "
                + "Existing partition columns: '%s', Clustering columns: '%s'.",
            tablePath, partitionColNames, clusteringCols));
  }

  public static RuntimeException nonRetryableCommitException(
      int attempt, long commitAsVersion, CommitFailedException cause) {
    throw new RuntimeException(
        String.format(
            "Commit attempt %d for version %d failed with a non-retryable exception.",
            attempt, commitAsVersion),
        cause);
  }

  public static KernelException concurrentTransaction(
      String appId, long txnVersion, long lastUpdated) {
    return new ConcurrentTransactionException(appId, txnVersion, lastUpdated);
  }

  public static KernelException metadataChangedException() {
    return new MetadataChangedException();
  }

  public static KernelException protocolChangedException(long attemptVersion) {
    return new ProtocolChangedException(attemptVersion);
  }

  public static KernelException voidTypeEncountered() {
    return new KernelException(
        "Failed to parse the schema. Encountered unsupported Delta data type: VOID");
  }

  public static KernelException cannotModifyTableProperty(String key) {
    String msg =
        format("The Delta table property '%s' is an internal property and cannot be updated.", key);
    return new KernelException(msg);
  }

  public static KernelException unknownConfigurationException(String confKey) {
    return new UnknownConfigurationException(confKey);
  }

  public static KernelException invalidConfigurationValueException(
      String key, String value, String helpMessage) {
    return new InvalidConfigurationValueException(key, value, helpMessage);
  }

  public static KernelException domainMetadataUnsupported() {
    String message =
        "Cannot commit DomainMetadata action(s) because the feature 'domainMetadata' "
            + "is not supported on this table.";
    return new KernelException(message);
  }

  public static ConcurrentWriteException concurrentDomainMetadataAction(
      DomainMetadata domainMetadataAttempt, DomainMetadata winningDomainMetadata) {
    String message =
        String.format(
            "A concurrent writer added a domainMetadata action for the same domain: %s. "
                + "No domain-specific conflict resolution is available for this domain. "
                + "Attempted domainMetadata: %s. Winning domainMetadata: %s",
            domainMetadataAttempt.getDomain(), domainMetadataAttempt, winningDomainMetadata);
    return new ConcurrentWriteException(message);
  }

  public static KernelException missingNumRecordsStatsForRowTracking() {
    return new KernelException(
        "Cannot write to a rowTracking-supported table without 'numRecords' statistics. "
            + "Connectors are expected to populate the number of records statistics when "
            + "writing to a Delta table with 'rowTracking' table feature supported.");
  }

  public static KernelException rowTrackingSupportedWithDomainMetadataUnsupported() {
    return new KernelException(
        "Feature 'rowTracking' is supported and depends on feature 'domainMetadata',"
            + " but 'domainMetadata' is unsupported");
  }

  public static KernelException rowTrackingRequiredForRowIdHighWatermark(
      String tablePath, String rowIdHighWatermark) {
    return new KernelException(
        String.format(
            "Cannot assign a row id high water mark (`%s`) to a table `%s` that does not support "
                + "`rowTracking` table feature. Please enable the `rowTracking` table feature.",
            rowIdHighWatermark, tablePath));
  }

  public static KernelException cannotToggleRowTrackingOnExistingTable() {
    return new KernelException("Row tracking support cannot be changed once the table is created.");
  }

  public static KernelException missingRowTrackingColumnRequested(String columnName) {
    return new KernelException(
        String.format(
            "Row tracking is not enabled, but row tracking column '%s' was requested.",
            columnName));
  }

  public static KernelException cannotModifyAppendOnlyTable(String tablePath) {
    return new KernelException(
        String.format(
            "Cannot modify append-only table. Table `%s` has configuration %s=true.",
            tablePath, TableConfig.APPEND_ONLY_ENABLED.getKey()));
  }

  public static KernelException rowTrackingMetadataMissingInFile(String entry, String filePath) {
    return new KernelException(
        String.format("Required metadata key %s is not present in scan file %s.", entry, filePath));
  }

  public static InvalidTableException tableWithIctMissingCommitInfo(String dataPath, long version) {
    return new InvalidTableException(
        dataPath,
        String.format(
            "This table has the feature inCommitTimestamp enabled which requires the presence of "
                + "the CommitInfo action in every commit. However, the CommitInfo action is "
                + "missing from commit version %d.",
            version));
  }

  public static InvalidTableException tableWithIctMissingIct(String dataPath, long version) {
    return new InvalidTableException(
        dataPath,
        String.format(
            "This table has the feature inCommitTimestamp enabled which requires the presence of "
                + "inCommitTimestamp in the CommitInfo action. However, this field has not been "
                + "set in commit version %d.",
            version));
  }

  public static KernelException metadataMissingRequiredCatalogTableProperty(
      String committerClassName,
      Map<String, String> missingOrViolatingProperties,
      Map<String, String> requiredCatalogTableProperties) {
    final String details =
        missingOrViolatingProperties.entrySet().stream()
            .map(
                entry ->
                    String.format(
                        "%s (current: '%s', required: '%s')",
                        entry.getKey(),
                        entry.getValue(),
                        requiredCatalogTableProperties.get(entry.getKey())))
            .collect(Collectors.joining(", "));
    return new KernelException(
        String.format(
            "[%s] Metadata is missing or has incorrect values for required catalog properties: %s.",
            committerClassName, details));
  }

  public static KernelException invalidFieldMove(
      int columnId,
      Optional<SchemaIterable.ParentStructFieldInfo> currentParent,
      Optional<SchemaIterable.ParentStructFieldInfo> newParent) {
    return new KernelException(
        String.format(
            "Cannot move fields between different levels of nesting: "
                + "field with fieldId=%s is nested under %s in the current schema and under %s in "
                + "the new schema",
            columnId, formatParentField(currentParent), formatParentField(newParent)));
  }

  /* ------------------------ HELPER METHODS ----------------------------- */

  private static String formatParentField(Optional<SchemaIterable.ParentStructFieldInfo> parent) {
    if (!parent.isPresent()) {
      return "ROOT";
    }
    StructField parentField = parent.get().getParentField();
    String pathToParentField = parent.get().getPathFromParent();
    if (pathToParentField.isEmpty()) {
      // Example: "StructField(name=c1, ...)"
      return parentField.toString();
    } else {
      // Example: "StructField(name=c1, ...) at path=key.element"
      return parentField.toString() + " at path=" + pathToParentField;
    }
  }

  private static String formatTimestamp(long millisSinceEpochUTC) {
    return new Timestamp(millisSinceEpochUTC).toInstant().toString();
  }

  // We use the `Supplier` interface to avoid silently wrapping any checked exceptions
  public static <T> T wrapEngineException(Supplier<T> f, String msgString, Object... args) {
    try {
      return f.get();
    } catch (KernelException e) {
      // Let any KernelExceptions fall through (even though these generally shouldn't
      // originate from the engine implementation there are some edge cases such as
      // deserializeStructType)
      throw e;
    } catch (RuntimeException e) {
      throw new KernelEngineException(String.format(msgString, args), e);
    }
  }

  // Functional interface for a fx that throws an `IOException` (but no other checked exceptions)
  public interface SupplierWithIOException<T> {
    T get() throws IOException;
  }

  public static <T> T wrapEngineExceptionThrowsIO(
      SupplierWithIOException<T> f, String msgString, Object... args) throws IOException {
    try {
      return f.get();
    } catch (KernelException e) {
      // Let any KernelExceptions fall through (even though these generally shouldn't
      // originate from the engine implementation there are some edge cases such as
      // deserializeStructType)
      throw e;
    } catch (RuntimeException e) {
      throw new KernelEngineException(String.format(msgString, args), e);
    }
  }
}
