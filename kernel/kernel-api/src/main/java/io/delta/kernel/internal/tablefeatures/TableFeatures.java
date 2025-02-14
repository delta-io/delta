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

package io.delta.kernel.internal.tablefeatures;

import static io.delta.kernel.internal.DeltaErrors.*;
import static io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode.NONE;
import static io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ;
import static io.delta.kernel.types.VariantType.VARIANT;

import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.CaseInsensitiveMap;
import io.delta.kernel.internal.util.SchemaUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;

/** Contains utility methods related to the Delta table feature support in protocol. */
public class TableFeatures {

  /////////////////////////////////////////////////////////////////////////////////
  /// START: Define the {@link TableFeature}s                                   ///
  /// If feature instance variable ends with                                    ///
  ///  1) `_W_FEATURE` it is a writer only feature.                             ///
  ///  2) `_RW_FEATURE` it is a reader-writer feature.                          ///
  /////////////////////////////////////////////////////////////////////////////////
  public static final TableFeature APPEND_ONLY_W_FEATURE = new AppendOnlyFeature();

  private static class AppendOnlyFeature extends TableFeature.LegacyWriterFeature {
    AppendOnlyFeature() {
      super(/* featureName = */ "appendOnly", /* minWriterVersion = */ 2);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.APPEND_ONLY_ENABLED.fromMetadata(metadata);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return true;
    }
  }

  public static final TableFeature INVARIANTS_W_FEATURE = new InvariantsFeature();

  private static class InvariantsFeature extends TableFeature.LegacyWriterFeature {
    InvariantsFeature() {
      super(/* featureName = */ "invariants", /* minWriterVersion = */ 2);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasInvariants(metadata.getSchema());
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // If there is no invariant, then the table is supported
      return !hasInvariants(metadata.getSchema());
    }
  }

  public static final TableFeature CONSTRAINTS_W_FEATURE = new ConstraintsFeature();

  private static class ConstraintsFeature extends TableFeature.LegacyWriterFeature {
    ConstraintsFeature() {
      super("checkConstraints", /* minWriterVersion = */ 3);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // Kernel doesn't support table with constraints.
      return !hasCheckConstraints(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasCheckConstraints(metadata);
    }
  }

  public static final TableFeature CHANGE_DATA_FEED_W_FEATURE = new ChangeDataFeedFeature();

  private static class ChangeDataFeedFeature extends TableFeature.LegacyWriterFeature {
    ChangeDataFeedFeature() {
      super("changeDataFeed", /* minWriterVersion = */ 4);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.CHANGE_DATA_FEED_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature COLUMN_MAPPING_RW_FEATURE = new ColumnMappingFeature();

  private static class ColumnMappingFeature extends TableFeature.LegacyReaderWriterFeature {
    ColumnMappingFeature() {
      super("columnMapping", /*minReaderVersion = */ 2, /* minWriterVersion = */ 5);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.COLUMN_MAPPING_MODE.fromMetadata(metadata) != NONE;
    }
  }

  public static final TableFeature GENERATED_COLUMNS_W_FEATURE = new GeneratedColumnsFeature();

  private static class GeneratedColumnsFeature extends TableFeature.LegacyWriterFeature {
    GeneratedColumnsFeature() {
      super("generatedColumns", /* minWriterVersion = */ 4);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      // Kernel can write as long as there are no generated columns defined
      return !hasGeneratedColumns(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasGeneratedColumns(metadata);
    }
  }

  public static final TableFeature IDENTITY_COLUMNS_W_FEATURE = new IdentityColumnsFeature();

  private static class IdentityColumnsFeature extends TableFeature.LegacyWriterFeature {
    IdentityColumnsFeature() {
      super("identityColumns", /* minWriterVersion = */ 6);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return !hasIdentityColumns(metadata);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasIdentityColumns(metadata);
    }
  }

  public static final TableFeature VARIANT_RW_FEATURE = new VariantTypeTableFeature("variantType");
  public static final TableFeature VARIANT_RW_PREVIEW_FEATURE =
      new VariantTypeTableFeature("variantType-preview");

  private static class VariantTypeTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    VariantTypeTableFeature(String featureName) {
      super(
          /* featureName = */ featureName, /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasTypeColumn(metadata.getSchema(), VARIANT);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }
  }

  public static final TableFeature DOMAIN_METADATA_W_FEATURE = new DomainMetadataFeature();

  private static class DomainMetadataFeature extends TableFeature.WriterFeature {
    DomainMetadataFeature() {
      super("domainMetadata", /* minWriterVersion = */ 7);
    }
  }

  public static final TableFeature ROW_TRACKING_W_FEATURE = new RowTrackingFeature();

  private static class RowTrackingFeature extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    RowTrackingFeature() {
      super("rowTracking", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata);
    }

    @Override
    public Set<TableFeature> requiredFeatures() {
      return Collections.singleton(DOMAIN_METADATA_W_FEATURE);
    }
  }

  public static final TableFeature DELETION_VECTORS_RW_FEATURE = new DeletionVectorsTableFeature();

  /**
   * Kernel currently only support blind appends. So we don't need to do anything special for
   * writing into a table with deletion vectors enabled (i.e a table feature with DV enabled is both
   * readable and writable.
   */
  private static class DeletionVectorsTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    DeletionVectorsTableFeature() {
      super("deletionVectors", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.DELETION_VECTORS_CREATION_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature ICEBERG_COMPAT_V2_W_FEATURE = new IcebergCompatV2TableFeature();

  private static class IcebergCompatV2TableFeature extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    IcebergCompatV2TableFeature() {
      super("icebergCompatV2", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata);
    }

    public @Override Set<TableFeature> requiredFeatures() {
      return Collections.singleton(COLUMN_MAPPING_RW_FEATURE);
    }
  }

  public static final TableFeature TYPE_WIDENING_RW_FEATURE =
      new TypeWideningTableFeature("typeWidening");
  public static final TableFeature TYPE_WIDENING_PREVIEW_TABLE_FEATURE =
      new TypeWideningTableFeature("typeWidening-preview");

  private static class TypeWideningTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    TypeWideningTableFeature(String featureName) {
      super(featureName, /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.TYPE_WIDENING_ENABLED.fromMetadata(metadata);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to support it.
    }
  }

  public static final TableFeature IN_COMMIT_TIMESTAMP_W_FEATURE =
      new InCommitTimestampTableFeature();

  private static class InCommitTimestampTableFeature extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    InCommitTimestampTableFeature() {
      super("inCommitTimestamp", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata);
    }
  }

  public static final TableFeature TIMESTAMP_NTZ_RW_FEATURE = new TimestampNtzTableFeature();

  private static class TimestampNtzTableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    TimestampNtzTableFeature() {
      super("timestampNtz", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean hasKernelWriteSupport(Metadata metadata) {
      return false; // TODO: yet to be implemented in Kernel
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return hasTypeColumn(metadata.getSchema(), TIMESTAMP_NTZ);
    }
  }

  public static final TableFeature CHECKPOINT_V2_RW_FEATURE = new CheckpointV2TableFeature();

  /**
   * In order to commit, there is no extra work required when v2 checkpoint is enabled. This affects
   * the checkpoint format only. When v2 is enabled, writing classic checkpoints is still allowed.
   */
  private static class CheckpointV2TableFeature extends TableFeature.ReaderWriterFeature
      implements FeatureAutoEnabledByMetadata {
    CheckpointV2TableFeature() {
      super("v2Checkpoint", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      // TODO: define an enum for checkpoint policy when we start supporting writing v2 checkpoints
      return "v2".equals(TableConfig.CHECKPOINT_POLICY.fromMetadata(metadata));
    }
  }

  public static final TableFeature VACUUM_PROTOCOL_CHECK_RW_FEATURE =
      new VacuumProtocolCheckTableFeature();

  private static class VacuumProtocolCheckTableFeature extends TableFeature.ReaderWriterFeature {
    VacuumProtocolCheckTableFeature() {
      super("vacuumProtocolCheck", /* minReaderVersion = */ 3, /* minWriterVersion = */ 7);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// END: Define the {@link TableFeature}s                                     ///
  /////////////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////////////
  /// Public static variables and methods                                       ///
  /////////////////////////////////////////////////////////////////////////////////
  /** Min reader version that supports reader features. */
  public static final int TABLE_FEATURES_MIN_READER_VERSION = 3;

  /** Min reader version that supports writer features. */
  public static final int TABLE_FEATURES_MIN_WRITER_VERSION = 7;

  public static final List<TableFeature> TABLE_FEATURES =
      Collections.unmodifiableList(
          Arrays.asList(
              APPEND_ONLY_W_FEATURE,
              CHECKPOINT_V2_RW_FEATURE,
              CHANGE_DATA_FEED_W_FEATURE,
              COLUMN_MAPPING_RW_FEATURE,
              CONSTRAINTS_W_FEATURE,
              DELETION_VECTORS_RW_FEATURE,
              GENERATED_COLUMNS_W_FEATURE,
              DOMAIN_METADATA_W_FEATURE,
              ICEBERG_COMPAT_V2_W_FEATURE,
              IDENTITY_COLUMNS_W_FEATURE,
              IN_COMMIT_TIMESTAMP_W_FEATURE,
              INVARIANTS_W_FEATURE,
              ROW_TRACKING_W_FEATURE,
              TIMESTAMP_NTZ_RW_FEATURE,
              TYPE_WIDENING_PREVIEW_TABLE_FEATURE,
              TYPE_WIDENING_RW_FEATURE,
              VACUUM_PROTOCOL_CHECK_RW_FEATURE,
              VARIANT_RW_FEATURE,
              VARIANT_RW_PREVIEW_FEATURE));

  public static final Map<String, TableFeature> TABLE_FEATURE_MAP =
      Collections.unmodifiableMap(
          new CaseInsensitiveMap<TableFeature>() {
            {
              for (TableFeature feature : TABLE_FEATURES) {
                put(feature.featureName(), feature);
              }
            }
          });

  /** Get the table feature by name. Case-insensitive lookup. If not found, throws error. */
  public static TableFeature getTableFeature(String featureName) {
    TableFeature tableFeature = TABLE_FEATURE_MAP.get(featureName);
    if (tableFeature == null) {
      throw DeltaErrors.unsupportedTableFeature(featureName);
    }
    return tableFeature;
  }

  /** Does reader version has support explicitly specifying reader feature set in protocol? */
  public static boolean supportsReaderFeatures(int minReaderVersion) {
    return minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION;
  }

  /** Does writer version has support explicitly specifying writer feature set in protocol? */
  public static boolean supportsWriterFeatures(int minWriterVersion) {
    return minWriterVersion >= TABLE_FEATURES_MIN_WRITER_VERSION;
  }

  /** Returns the minimum reader/writer versions required to support all provided features. */
  public static Tuple2<Integer, Integer> minimumRequiredVersions(Set<TableFeature> features) {
    int minReaderVersion =
        features.stream().mapToInt(TableFeature::minReaderVersion).max().orElse(0);

    int minWriterVersion =
        features.stream().mapToInt(TableFeature::minWriterVersion).max().orElse(0);

    return new Tuple2<>(Math.max(minReaderVersion, 1), Math.max(minWriterVersion, 2));
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Everything below will be removed once the Kernel upgrades to use the     ///
  /// above interfaces.                                                         ///
  /////////////////////////////////////////////////////////////////////////////////
  private static final Set<String> SUPPORTED_WRITER_FEATURES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("appendOnly");
              add("inCommitTimestamp");
              add("columnMapping");
              add("typeWidening-preview");
              add("typeWidening");
              add(DOMAIN_METADATA_FEATURE_NAME);
              add(ROW_TRACKING_FEATURE_NAME);
            }
          });

  private static final Set<String> SUPPORTED_READER_FEATURES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("columnMapping");
              add("deletionVectors");
              add("timestampNtz");
              add("typeWidening-preview");
              add("typeWidening");
              add("vacuumProtocolCheck");
              add("variantType");
              add("variantType-preview");
              add("v2Checkpoint");
            }
          });

  public static final String DOMAIN_METADATA_FEATURE_NAME = "domainMetadata";

  public static final String ROW_TRACKING_FEATURE_NAME = "rowTracking";

  public static final String INVARIANTS_FEATURE_NAME = "invariants";

  ////////////////////
  // Helper Methods //
  ////////////////////

  public static void validateReadSupportedTable(Protocol protocol, String tablePath) {
    switch (protocol.getMinReaderVersion()) {
      case 1:
        break;
      case 2:
        break;
      case 3:
        Set<String> readerFeatures = protocol.getReaderFeatures();
        if (!SUPPORTED_READER_FEATURES.containsAll(readerFeatures)) {
          Set<String> unsupportedFeatures = new HashSet<>(readerFeatures);
          unsupportedFeatures.removeAll(SUPPORTED_READER_FEATURES);
          throw DeltaErrors.unsupportedReaderFeature(tablePath, unsupportedFeatures);
        }
        break;
      default:
        throw DeltaErrors.unsupportedReaderProtocol(tablePath, protocol.getMinReaderVersion());
    }
  }

  /**
   * Utility method to validate whether the given table is supported for writing from Kernel.
   * Currently, the support is as follows:
   *
   * <ul>
   *   <li>protocol writer version 1.
   *   <li>protocol writer version 2 only with appendOnly feature enabled.
   *   <li>protocol writer version 7 with {@code appendOnly}, {@code inCommitTimestamp}, {@code
   *       columnMapping}, {@code typeWidening}, {@code domainMetadata}, {@code rowTracking} feature
   *       enabled.
   * </ul>
   *
   * @param protocol Table protocol
   * @param metadata Table metadata
   */
  public static void validateWriteSupportedTable(
      Protocol protocol, Metadata metadata, String tablePath) {
    int minWriterVersion = protocol.getMinWriterVersion();
    switch (minWriterVersion) {
      case 1:
        break;
      case 2:
        // Append-only and column invariants are the writer features added in version 2
        // Append-only is supported, but not the invariants
        validateNoInvariants(metadata.getSchema());
        break;
      case 3:
        // Check constraints are added in version 3
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 4:
        // CDF and generated columns are writer features added in version 4
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 5:
        // Column mapping is the only one writer feature added in version 5
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 6:
        // Identity is the only one writer feature added in version 6
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
      case 7:
        for (String writerFeature : protocol.getWriterFeatures()) {
          if (writerFeature.equals(INVARIANTS_FEATURE_NAME)) {
            // For version 7, we allow 'invariants' to be present in the protocol's writerFeatures
            // to unblock certain use cases, provided that no invariants are defined in the schema.
            validateNoInvariants(metadata.getSchema());
          } else if (!SUPPORTED_WRITER_FEATURES.contains(writerFeature)) {
            throw unsupportedWriterFeature(tablePath, writerFeature);
          }
        }

        // Eventually we may have a way to declare and enforce dependencies between features.
        // By putting this check for row tracking here, it makes it easier to spot that row
        // tracking defines such a dependency that can be implicitly checked.
        if (isRowTrackingSupported(protocol) && !isDomainMetadataSupported(protocol)) {
          throw DeltaErrors.rowTrackingSupportedWithDomainMetadataUnsupported();
        }
        break;
      default:
        throw unsupportedWriterProtocol(tablePath, minWriterVersion);
    }
  }

  /**
   * Given the automatically enabled features from Delta table metadata, returns the minimum
   * required reader and writer version that satisfies all enabled table features in the metadata.
   *
   * @param enabledFeatures the automatically enabled features from the Delta table metadata
   * @return the minimum required reader and writer version that satisfies all enabled table
   */
  public static Tuple2<Integer, Integer> minProtocolVersionFromAutomaticallyEnabledFeatures(
      Set<String> enabledFeatures) {

    int readerVersion = 0;
    int writerVersion = 0;

    for (String feature : enabledFeatures) {
      readerVersion = Math.max(readerVersion, getMinReaderVersion(feature));
      writerVersion = Math.max(writerVersion, getMinWriterVersion(feature));
    }

    return new Tuple2<>(readerVersion, writerVersion);
  }

  /**
   * Extract the writer features that should be enabled automatically based on the metadata which
   * are not already enabled. For example, the {@code inCommitTimestamp} feature should be enabled
   * when the delta property name (delta.enableInCommitTimestamps) is set to true in the metadata if
   * it is not already enabled.
   *
   * @param metadata the metadata of the table
   * @param protocol the protocol of the table
   * @return the writer features that should be enabled automatically
   */
  public static Set<String> extractAutomaticallyEnabledWriterFeatures(
      Metadata metadata, Protocol protocol) {
    return TableFeatures.SUPPORTED_WRITER_FEATURES.stream()
        .filter(f -> metadataRequiresWriterFeatureToBeEnabled(metadata, f))
        .filter(
            f -> protocol.getWriterFeatures() == null || !protocol.getWriterFeatures().contains(f))
        .collect(Collectors.toSet());
  }

  /**
   * Checks if the table protocol supports the "domainMetadata" writer feature.
   *
   * @param protocol the protocol to check
   * @return true if the "domainMetadata" feature is supported, false otherwise
   */
  public static boolean isDomainMetadataSupported(Protocol protocol) {
    return isWriterFeatureSupported(protocol, DOMAIN_METADATA_FEATURE_NAME);
  }

  /**
   * Check if the table protocol supports the "rowTracking" writer feature.
   *
   * @param protocol the protocol to check
   * @return true if the protocol supports row tracking, false otherwise
   */
  public static boolean isRowTrackingSupported(Protocol protocol) {
    return isWriterFeatureSupported(protocol, ROW_TRACKING_FEATURE_NAME);
  }

  /**
   * Get the minimum reader version required for a feature.
   *
   * @param feature the feature
   * @return the minimum reader version required for the feature
   */
  private static int getMinReaderVersion(String feature) {
    switch (feature) {
      case "inCommitTimestamp":
        return 3;
      default:
        return 1;
    }
  }

  /**
   * Get the minimum writer version required for a feature.
   *
   * @param feature the feature
   * @return the minimum writer version required for the feature
   */
  private static int getMinWriterVersion(String feature) {
    switch (feature) {
      case "inCommitTimestamp":
        return 7;
      default:
        return 2;
    }
  }

  /**
   * Determine whether a writer feature must be supported and enabled to satisfy the metadata
   * requirements.
   *
   * @param metadata the table metadata
   * @param feature the writer feature to check
   * @return whether the writer feature must be enabled
   */
  private static boolean metadataRequiresWriterFeatureToBeEnabled(
      Metadata metadata, String feature) {
    switch (feature) {
      case "inCommitTimestamp":
        return TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata);
      default:
        return false;
    }
  }

  private static boolean isWriterFeatureSupported(Protocol protocol, String featureName) {
    Set<String> writerFeatures = protocol.getWriterFeatures();
    if (writerFeatures == null) {
      return false;
    }
    return writerFeatures.contains(featureName)
        && protocol.getMinWriterVersion() >= TABLE_FEATURES_MIN_WRITER_VERSION;
  }

  private static void validateNoInvariants(StructType tableSchema) {
    if (hasInvariants(tableSchema)) {
      throw DeltaErrors.columnInvariantsNotSupported();
    }
  }

  private static boolean hasInvariants(StructType tableSchema) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            /* recurseIntoMapOrArrayElements = */ false, // invariants are not allowed in maps or
            // arrays
            // arrays
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getMetadata().contains("delta.invariants"))
        .isEmpty();
  }

  private static boolean hasCheckConstraints(Metadata metadata) {
    return metadata.getConfiguration().entrySet().stream()
        .findAny()
        .map(entry -> entry.getKey().startsWith("delta.constraints."))
        .orElse(false);
  }

  /**
   * Check if the table schema has a column of type. Caution: works only for the primitive types.
   */
  private static boolean hasTypeColumn(StructType tableSchema, DataType type) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            /* recurseIntoMapOrArrayElements = */ true,
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getDataType().equals(type))
        .isEmpty();
  }

  private static boolean hasIdentityColumns(Metadata metadata) {
    return !SchemaUtils.filterRecursively(
            metadata.getSchema(),
            /* recurseIntoMapOrArrayElements = */ false, // don't expected identity columns in
            // nested columns
            /* stopOnFirstMatch */ true,
            /* filter */ field -> {
              FieldMetadata fieldMetadata = field.getMetadata();

              // Check if the metadata contains the required keys
              boolean hasStart = fieldMetadata.contains("delta.identity.start");
              boolean hasStep = fieldMetadata.contains("delta.identity.step");
              boolean hasInsert = fieldMetadata.contains("delta.identity.allowExplicitInsert");

              // Verify that all or none of the three fields are present
              if (!((hasStart == hasStep) && (hasStart == hasInsert))) {
                throw new KernelException(
                    String.format(
                        "Inconsistent IDENTITY metadata for column %s detected: %s, %s, %s",
                        field.getName(), hasStart, hasStep, hasInsert));
              }

              // Return true only if all three fields are present
              return hasStart && hasStep && hasInsert;
            })
        .isEmpty();
  }

  private static boolean hasGeneratedColumns(Metadata metadata) {
    return !SchemaUtils.filterRecursively(
            metadata.getSchema(),
            /* recurseIntoMapOrArrayElements = */ false, // don't expected generated columns in
            // nested columns
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getMetadata().contains("delta.generationExpression"))
        .isEmpty();
  }
}
