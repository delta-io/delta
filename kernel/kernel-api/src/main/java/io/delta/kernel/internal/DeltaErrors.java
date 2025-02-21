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

import io.delta.kernel.exceptions.*;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

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

  public static KernelException noCommitFilesFoundForVersionRange(
      String tablePath, long startVersion, long endVersion) {
    String message =
        String.format(
            "%s: Requested table changes between [%s, %s] but no log files found in the requested"
                + " version range.",
            tablePath, startVersion, endVersion);
    return new KernelException(message);
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

  /* ------------------------ PROTOCOL EXCEPTIONS ----------------------------- */
  public static KernelException unsupportedTableFeature(String feature) {
    String message =
        String.format(
            "Unsupported Delta table feature: table requires feature \"%s\" "
                + "which is unsupported by this version of Delta Kernel.",
            feature);
    return new KernelException(message);
  }

  public static KernelException unsupportedReaderProtocol(
      String tablePath, int tableReaderVersion) {
    String message =
        String.format(
            "Unsupported Delta protocol reader version: table `%s` requires reader version %s "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, tableReaderVersion);
    return new KernelException(message);
  }

  public static KernelException unsupportedReaderFeature(
      String tablePath, Set<String> unsupportedFeatures) {
    String message =
        String.format(
            "Unsupported Delta reader features: table `%s` requires reader table features [%s] "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, String.join(", ", unsupportedFeatures));
    return new KernelException(message);
  }

  public static KernelException unsupportedWriterProtocol(
      String tablePath, int tableWriterVersion) {
    String message =
        String.format(
            "Unsupported Delta protocol writer version: table `%s` requires writer version %s "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, tableWriterVersion);
    return new KernelException(message);
  }

  public static KernelException unsupportedWriterFeature(String tablePath, String writerFeature) {
    String message =
        String.format(
            "Unsupported Delta writer feature: table `%s` requires writer table feature \"%s\" "
                + "which is unsupported by this version of Delta Kernel.",
            tablePath, writerFeature);
    return new KernelException(message);
  }

  public static KernelException columnInvariantsNotSupported() {
    String message =
        "This version of Delta Kernel does not support writing to tables with "
            + "column invariants present.";
    return new KernelException(message);
  }

  public static KernelException unsupportedDataType(DataType dataType) {
    return new KernelException("Kernel doesn't support writing data of type: " + dataType);
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

  public static KernelException missingNumRecordsStatsForIcebergCompatV2(
      DataFileStatus dataFileStatus) {
    throw new KernelException(
        format(
            "Iceberg V2 compatibility requires statistics.\n DataFileStatus: %s", dataFileStatus));
  }

  public static KernelException partitionColumnMissingInData(
      String tablePath, String partitionColumn) {
    String msgT = "Missing partition column '%s' in the data to be written to the table '%s'.";
    return new KernelException(format(msgT, partitionColumn, tablePath));
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

  /* ------------------------ HELPER METHODS ----------------------------- */
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
