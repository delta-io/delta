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
import static java.util.stream.Collectors.toSet;

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
import java.util.stream.Stream;

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
      // writable if change data feed is disabled
      return !TableConfig.CHANGE_DATA_FEED_ENABLED.fromMetadata(metadata);
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
      if (TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata)) {
        throw new UnsupportedOperationException(
            "Feature `rowTracking` is not yet supported in Kernel.");
      }
      return false;
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

  public static final TableFeature ICEBERG_WRITER_COMPAT_V1 = new IcebergWriterCompatV1();

  private static class IcebergWriterCompatV1 extends TableFeature.WriterFeature
      implements FeatureAutoEnabledByMetadata {
    IcebergWriterCompatV1() {
      super("icebergWriterCompatV1", /* minWriterVersion = */ 7);
    }

    @Override
    public boolean metadataRequiresFeatureToBeEnabled(Protocol protocol, Metadata metadata) {
      return TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(metadata);
    }

    public @Override Set<TableFeature> requiredFeatures() {
      return Collections.singleton(ICEBERG_COMPAT_V2_W_FEATURE);
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
              VARIANT_RW_PREVIEW_FEATURE,
              ICEBERG_WRITER_COMPAT_V1));

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

  /** Does reader version supports explicitly specifying reader feature set in protocol? */
  public static boolean supportsReaderFeatures(int minReaderVersion) {
    return minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION;
  }

  /** Does writer version supports explicitly specifying writer feature set in protocol? */
  public static boolean supportsWriterFeatures(int minWriterVersion) {
    return minWriterVersion >= TABLE_FEATURES_MIN_WRITER_VERSION;
  }

  /** Returns the minimum reader/writer versions required to support all provided features. */
  public static Tuple2<Integer, Integer> minimumRequiredVersions(Set<TableFeature> features) {
    int minReaderVersion =
        features.stream().mapToInt(TableFeature::minReaderVersion).max().orElse(0);

    int minWriterVersion =
        features.stream().mapToInt(TableFeature::minWriterVersion).max().orElse(0);

    return new Tuple2<>(Math.max(minReaderVersion, 1), Math.max(minWriterVersion, 1));
  }

  /**
   * Upgrade the current protocol to satisfy all auto-update capable features required by the given
   * metadata. If the current protocol already satisfies the metadata requirements, return empty.
   *
   * @param newMetadata the new metadata to be applied to the table.
   * @param needDomainMetadataSupport whether the table needs to explicitly support domain metadata.
   * @param currentProtocol the current protocol of the table.
   * @return the upgraded protocol and the set of new features that were enabled in the upgrade.
   */
  public static Optional<Tuple2<Protocol, Set<TableFeature>>> autoUpgradeProtocolBasedOnMetadata(
      Metadata newMetadata, boolean needDomainMetadataSupport, Protocol currentProtocol) {

    Set<TableFeature> allNeededTableFeatures =
        extractAllNeededTableFeatures(newMetadata, currentProtocol);
    if (needDomainMetadataSupport) {
      allNeededTableFeatures =
          Stream.concat(allNeededTableFeatures.stream(), Stream.of(DOMAIN_METADATA_W_FEATURE))
              .collect(toSet());
    }
    Protocol required =
        new Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
            .withFeatures(allNeededTableFeatures)
            .normalized();

    // See if all the required features are already supported in the current protocol.
    if (!required.canUpgradeTo(currentProtocol)) {
      // `required` has one or more features that are not supported in `currentProtocol`.
      Set<TableFeature> newFeatures =
          new HashSet<>(required.getImplicitlyAndExplicitlySupportedFeatures());
      newFeatures.removeAll(currentProtocol.getImplicitlyAndExplicitlySupportedFeatures());
      return Optional.of(new Tuple2<>(required.merge(currentProtocol), newFeatures));
    } else {
      return Optional.empty();
    }
  }

  /** Utility method to check if the table with given protocol is readable by the Kernel. */
  public static void validateKernelCanReadTheTable(Protocol protocol, String tablePath) {
    if (protocol.getMinReaderVersion() > TABLE_FEATURES_MIN_READER_VERSION) {
      throw DeltaErrors.unsupportedReaderProtocol(tablePath, protocol.getMinReaderVersion());
    }

    Set<TableFeature> unsupportedFeatures =
        protocol.getImplicitlyAndExplicitlySupportedReaderWriterFeatures().stream()
            .filter(f -> !f.hasKernelReadSupport())
            .collect(toSet());

    if (!unsupportedFeatures.isEmpty()) {
      throw unsupportedReaderFeatures(
          tablePath, unsupportedFeatures.stream().map(TableFeature::featureName).collect(toSet()));
    }
  }

  /**
   * Utility method to check if the table with given protocol and metadata is writable by the
   * Kernel.
   */
  public static void validateKernelCanWriteToTable(
      Protocol protocol, Metadata metadata, String tablePath) {

    validateKernelCanReadTheTable(protocol, tablePath);

    if (protocol.getMinWriterVersion() > TABLE_FEATURES_MIN_WRITER_VERSION) {
      throw unsupportedWriterProtocol(tablePath, protocol.getMinWriterVersion());
    }

    Set<TableFeature> unsupportedFeatures =
        protocol.getImplicitlyAndExplicitlySupportedFeatures().stream()
            .filter(f -> !f.hasKernelWriteSupport(metadata))
            .collect(toSet());

    if (!unsupportedFeatures.isEmpty()) {
      throw unsupportedWriterFeatures(
          tablePath, unsupportedFeatures.stream().map(TableFeature::featureName).collect(toSet()));
    }
  }

  public static boolean isRowTrackingSupported(Protocol protocol) {
    return protocol.getImplicitlyAndExplicitlySupportedFeatures().contains(ROW_TRACKING_W_FEATURE);
  }

  public static boolean isDomainMetadataSupported(Protocol protocol) {
    return protocol
        .getImplicitlyAndExplicitlySupportedFeatures()
        .contains(DOMAIN_METADATA_W_FEATURE);
  }

  /////////////////////////////////////////////////////////////////////////////////
  /// Private methods                                                           ///
  /////////////////////////////////////////////////////////////////////////////////
  /**
   * Extracts all table features (and their dependency features) that are enabled by the given
   * metadata and supported in existing protocol.
   */
  private static Set<TableFeature> extractAllNeededTableFeatures(
      Metadata newMetadata, Protocol currentProtocol) {
    Set<TableFeature> protocolSupportedFeatures =
        currentProtocol.getImplicitlyAndExplicitlySupportedFeatures();

    Set<TableFeature> metadataEnabledFeatures =
        TableFeatures.TABLE_FEATURES.stream()
            .filter(f -> f instanceof FeatureAutoEnabledByMetadata)
            .filter(
                f ->
                    ((FeatureAutoEnabledByMetadata) f)
                        .metadataRequiresFeatureToBeEnabled(currentProtocol, newMetadata))
            .collect(toSet());

    // Each feature may have dependencies that are not yet enabled in the protocol.
    Set<TableFeature> newFeatures = getDependencyFeatures(metadataEnabledFeatures);
    return Stream.concat(protocolSupportedFeatures.stream(), newFeatures.stream()).collect(toSet());
  }

  /**
   * Returns the smallest set of table features that contains `features` and that also contains all
   * dependencies of all features in the returned set.
   */
  private static Set<TableFeature> getDependencyFeatures(Set<TableFeature> features) {
    Set<TableFeature> requiredFeatures = new HashSet<>(features);
    features.forEach(feature -> requiredFeatures.addAll(feature.requiredFeatures()));

    if (features.equals(requiredFeatures)) {
      return features;
    } else {
      return getDependencyFeatures(requiredFeatures);
    }
  }

  private static boolean hasInvariants(StructType tableSchema) {
    return !SchemaUtils.filterRecursively(
            tableSchema,
            // invariants are not allowed in maps or arrays
            /* recurseIntoMapOrArrayElements = */ false,
            /* stopOnFirstMatch */ true,
            /* filter */ field -> field.getMetadata().contains("delta.invariants"))
        .isEmpty();
  }

  private static boolean hasCheckConstraints(Metadata metadata) {
    return metadata.getConfiguration().keySet().stream()
        .anyMatch(s -> s.startsWith("delta.constraints."));
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
