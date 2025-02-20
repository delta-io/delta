/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.actions;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.TABLE_FEATURES_MIN_WRITER_VERSION;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.VectorUtils.stringArrayValue;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.*;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Protocol {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Public static variables and methods                                                       ///
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static Protocol fromColumnVector(ColumnVector vector, int rowId) {
    if (vector.isNullAt(rowId)) {
      return null;
    }

    return new Protocol(
        vector.getChild(0).getInt(rowId),
        vector.getChild(1).getInt(rowId),
        vector.getChild(2).isNullAt(rowId)
            ? emptySet()
            : new HashSet<>(VectorUtils.toJavaList(vector.getChild(2).getArray(rowId))),
        vector.getChild(3).isNullAt(rowId)
            ? emptySet()
            : new HashSet<>(VectorUtils.toJavaList(vector.getChild(3).getArray(rowId))));
  }

  public static final StructType FULL_SCHEMA =
      new StructType()
          .add("minReaderVersion", IntegerType.INTEGER, false /* nullable */)
          .add("minWriterVersion", IntegerType.INTEGER, false /* nullable */)
          .add("readerFeatures", new ArrayType(StringType.STRING, false /* contains null */))
          .add("writerFeatures", new ArrayType(StringType.STRING, false /* contains null */));

  private final int minReaderVersion;
  private final int minWriterVersion;
  private final Set<String> readerFeatures;
  private final Set<String> writerFeatures;

  // These are derived fields from minReaderVersion and minWriterVersion
  private final boolean supportsReaderFeatures;
  private final boolean supportsWriterFeatures;

  public Protocol(int minReaderVersion, int minWriterVersion) {
    this(minReaderVersion, minWriterVersion, emptySet(), emptySet());
  }

  public Protocol(
      int minReaderVersion,
      int minWriterVersion,
      Set<String> readerFeatures,
      Set<String> writerFeatures) {
    this.minReaderVersion = minReaderVersion;
    this.minWriterVersion = minWriterVersion;
    this.readerFeatures = requireNonNull(readerFeatures, "readerFeatures cannot be null");
    this.writerFeatures = requireNonNull(writerFeatures, "writerFeatures cannot be null");
    this.supportsReaderFeatures = TableFeatures.supportsReaderFeatures(minReaderVersion);
    this.supportsWriterFeatures = TableFeatures.supportsWriterFeatures(minWriterVersion);
  }

  public int getMinReaderVersion() {
    return minReaderVersion;
  }

  public int getMinWriterVersion() {
    return minWriterVersion;
  }

  public Set<String> getReaderFeatures() {
    return readerFeatures;
  }

  public Set<String> getWriterFeatures() {
    return writerFeatures;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Protocol{");
    sb.append("minReaderVersion=").append(minReaderVersion);
    sb.append(", minWriterVersion=").append(minWriterVersion);
    sb.append(", readerFeatures=").append(readerFeatures);
    sb.append(", writerFeatures=").append(writerFeatures);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Protocol protocol = (Protocol) o;
    return minReaderVersion == protocol.minReaderVersion
        && minWriterVersion == protocol.minWriterVersion
        && Objects.equals(readerFeatures, protocol.readerFeatures)
        && Objects.equals(writerFeatures, protocol.writerFeatures);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minReaderVersion, minWriterVersion, readerFeatures, writerFeatures);
  }

  /**
   * Encode as a {@link Row} object with the schema {@link Protocol#FULL_SCHEMA}.
   * Write any empty `readerFeatures` and `writerFeatures` as null.
   *
   * @return {@link Row} object with the schema {@link Protocol#FULL_SCHEMA}
   */
  public Row toRow() {
    Map<Integer, Object> protocolMap = new HashMap<>();
    protocolMap.put(0, minReaderVersion);
    protocolMap.put(1, minWriterVersion);
    if (!readerFeatures.isEmpty()) {
      protocolMap.put(2, stringArrayValue(new ArrayList<>(readerFeatures)));
    }
    if (!writerFeatures.isEmpty()) {
      protocolMap.put(3, stringArrayValue(new ArrayList<>(writerFeatures)));
    }

    return new GenericRow(Protocol.FULL_SCHEMA, protocolMap);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Public methods related to table features interaction with the protocol                    ///
  /////////////////////////////////////////////////////////////////////////////////////////////////
  /**
   * Get the set of features that are implicitly supported by the protocol. Features are implicitly
   * supported if the reader and/or writer version is less than the versions that supports the
   * explicit features specified in `readerFeatures` and `writerFeatures` sets. Examples:
   * <p>
   *     <ul>
   * <li>(minRV = 1, minWV = 7, readerFeatures=[], writerFeatures=[domainMetadata]) => []
   * <li>(minRV = 1, minWV = 3) => [appendOnly, invariants, checkConstraints]
   * <li>(minRV = 3, minWV = 7, readerFeatures=[v2Checkpoint], writerFeatures=[v2Checkpoint]) => []
   * <li>(minRV = 2, minWV = 6) => [appendOnly, invariants, checkConstraints,
   *  changeDataFeed, generatedColumns, columnMapping, identityColumns]
   *  </ul>
   */
  public Set<TableFeature> getImplicitlySupportedFeatures() {
    if (supportsReaderFeatures && supportsWriterFeatures) {
      return emptySet();
    } else {
      return TableFeatures.TABLE_FEATURES.stream()
          .filter(f -> !supportsReaderFeatures && f.minReaderVersion() <= minReaderVersion)
          .filter(f -> !supportsWriterFeatures && f.minWriterVersion() <= minWriterVersion)
          .collect(Collectors.toSet());
    }
  }

  /**
   * Get the set of features that are explicitly supported by the protocol. Features are explicitly
   * supported if they are present in the `readerFeatures` and/or `writerFeatures` sets. Examples:
   * <p>
   *    <ul>
   * <li>(minRV = 1, minWV = 7, writerFeatures=[appendOnly, invariants, checkConstraints]) =>
   *    [appendOnly, invariants, checkConstraints]
   * </li>(minRV = 3, minWV = 7, readerFeatures = [columnMapping], writerFeatures=[columnMapping, invariants]) =>
   *    [columnMapping, invariants]
   * </li>(minRV = 1, minWV = 2, readerFeatures = [], writerFeatures=[]) => []
   * </ul>
   */
  public Set<TableFeature> getExplicitlySupportedFeatures() {
    return Stream.of(readerFeatures, writerFeatures)
        .flatMap(Set::stream)
        .map(TableFeatures::getTableFeature) // if a feature is not known, will throw an exception
        .collect(Collectors.toSet());
  }

  /**
   * Get the set of features that are both implicitly and explicitly supported by the protocol.
   * Usually, the protocol has either implicit or explicit features, but not both. This API provides
   * a way to get all enabled features.
   */
  public Set<TableFeature> getImplicitlyAndExplicitlySupportedFeatures() {
    Set<TableFeature> supportedFeatures = new HashSet<>();
    supportedFeatures.addAll(getImplicitlySupportedFeatures());
    supportedFeatures.addAll(getExplicitlySupportedFeatures());
    return supportedFeatures;
  }

  /** Create a new {@link Protocol} object with the given {@link TableFeature} supported. */
  public Protocol withFeatures(Iterable<TableFeature> newFeatures) {
    Protocol result = this;
    for (TableFeature feature : newFeatures) {
      result = result.withFeature(feature);
    }
    return result;
  }

  /**
   * Get a new Protocol object that has `feature` supported. Writer-only features will be added to
   * `writerFeatures` field, and reader-writer features will be added to `readerFeatures` and
   * `writerFeatures` fields.
   *
   * <p>If `feature` is already implicitly supported in the current protocol's legacy reader or
   * writer protocol version, the new protocol will not modify the original protocol version, i.e.,
   * the feature will not be explicitly added to the protocol's `readerFeatures` or
   * `writerFeatures`. This is to avoid unnecessary protocol upgrade for feature that it already
   * supports.
   * <p>
   * Examples:
   * <ul>
   *     <li>current protocol (2, 5) and new feature to add 'invariants` -> (2, 5) as this protocol
   *     already supports 'invariants' implicitly.
   *     <li>current protocol is (1, 7, writerFeature='rowTracking,domainMetadata' and the new
   *     feature to add is 'appendOnly' -> (1, 7, writerFeature='rowTracking,domainMetadata,appendOnly')
   *     <li>current protocol is (1, 7, writerFeature='rowTracking,domainMetadata' and the new feature
   *     to add is 'columnMapping' -> throws UnsupportedOperationException as 'columnMapping' requires
   *     higher reader version (2) than the current protocol's reader version (1).
   * </ul>
   */
  public Protocol withFeature(TableFeature feature) {
    // Add required dependencies of the feature
    Protocol protocolWithDependencies = withFeatures(feature.requiredFeatures());

    if (feature.minReaderVersion() > protocolWithDependencies.minReaderVersion) {
      throw new UnsupportedOperationException(
          "TableFeature requires higher reader protocol version");
    }

    if (feature.minWriterVersion() > protocolWithDependencies.minWriterVersion) {
      throw new UnsupportedOperationException(
          "TableFeature requires higher writer protocol version");
    }

    boolean shouldAddToReaderFeatures =
        feature.isReaderWriterFeature()
            &&
            // protocol already has support for `readerFeatures` set and the new feature
            // can be explicitly added to the protocol's `readerFeatures`
            supportsReaderFeatures;

    Set<String> newReaderFeatures = protocolWithDependencies.readerFeatures;
    Set<String> newWriterFeatures = protocolWithDependencies.writerFeatures;

    if (shouldAddToReaderFeatures) {
      newReaderFeatures = new HashSet<>(protocolWithDependencies.readerFeatures);
      newReaderFeatures.add(feature.featureName());
    }

    if (supportsWriterFeatures) {
      newWriterFeatures = new HashSet<>(protocolWithDependencies.writerFeatures);
      newWriterFeatures.add(feature.featureName());
    }

    return new Protocol(
        protocolWithDependencies.minReaderVersion,
        protocolWithDependencies.minWriterVersion,
        newReaderFeatures,
        newWriterFeatures);
  }

  /**
   * Determine whether this protocol can be safely upgraded to a new protocol `to`. This means all
   * features supported by this protocol are supported by `to`.
   *
   * <p>Examples regarding feature status:
   *
   * <ul>
   *   <li>from `[appendOnly]` to `[appendOnly]` => allowed.
   *   <li>from `[appendOnly, changeDataFeed]` to `[appendOnly]` => not allowed.
   * </ul>
   */
  public boolean canUpgradeTo(Protocol to) {
    return to.getImplicitlyAndExplicitlySupportedFeatures()
        .containsAll(this.getImplicitlyAndExplicitlySupportedFeatures());
  }

  /**
   * Protocol normalization is the process of converting a table features protocol to the weakest
   * possible form. This primarily refers to converting a table features protocol to a legacy
   * protocol. A Table Features protocol can be represented with the legacy representation only when
   * the features set of the former exactly matches a legacy protocol.
   *
   * <p>Normalization can also decrease the reader version of a table features protocol when it is
   * higher than necessary.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>(1, 7, AppendOnly, Invariants, CheckConstraints) -> (1, 3)
   *   <li>(3, 7, RowTracking) -> (1, 7, RowTracking)
   * </ul>
   */
  public Protocol normalized() {
    // Normalization can only be applied to table feature protocols.
    if (!supportsWriterFeatures) {
      return this;
    }

    Tuple2<Integer, Integer> versions =
        TableFeatures.minimumRequiredVersions(getExplicitlySupportedFeatures());
    int minReaderVersion = versions._1;
    int minWriterVersion = versions._2;
    Protocol newProtocol = new Protocol(minReaderVersion, minWriterVersion);

    if (this.getImplicitlyAndExplicitlySupportedFeatures()
        .equals(newProtocol.getImplicitlyAndExplicitlySupportedFeatures())) {
      return newProtocol;
    } else {
      // means we have some feature that is added after table feature support.
      // Whatever the feature (reader or readerWriter), it is always going to
      // have minWriterVersion as 7. Required minReaderVersion
      // should be based on the supported features.
      return new Protocol(minReaderVersion, TABLE_FEATURES_MIN_WRITER_VERSION)
          .withFeatures(getExplicitlySupportedFeatures());
    }
  }

  /**
   * Protocol denormalization is the process of converting a legacy protocol to the equivalent table
   * features protocol. This is the inverse of protocol normalization. It can be used to allow
   * operations on legacy protocols that yield results which cannot be represented anymore by a
   * legacy protocol. For example
   *
   * <ul>
   *   <li>(1, 3) ->
   *       (1, 7, readerFeatures=[], writerFeatures=[appendOnly, invariants, checkConstraints])
   *   <li>(2, 5) ->
   *       (2, 7,
   *          readerFeatures=[],
   *          writerFeatures=[appendOnly, invariants, checkConstraints,
   *              changeDataFeed, generatedColumns, columnMapping])
   * </ul>
   */
  public Protocol denormalized() {
    // Denormalization can only be applied to legacy protocols.
    if (supportsWriterFeatures) {
      return this;
    }

    Tuple2<Integer, Integer> versions =
        TableFeatures.minimumRequiredVersions(getImplicitlySupportedFeatures());
    int minReaderVersion = versions._1;

    return new Protocol(minReaderVersion, TABLE_FEATURES_MIN_WRITER_VERSION)
        .withFeatures(getImplicitlySupportedFeatures());
  }

  /**
   * Helper method that applies both denormalization and normalization. This can be used to
   * normalize invalid legacy protocols such as (2, 3), (1, 5). A legacy protocol is invalid when
   * the version numbers are higher than required to support the implied feature set.
   */
  public Protocol denormalizedNormalized() {
    return this.denormalized().normalized();
  }

  /**
   * Merge this protocol with multiple `protocols` to have the highest reader and writer versions
   * plus all explicitly and implicitly supported features.
   */
  public Protocol merge(Protocol... others) {
    List<Protocol> protocols = new ArrayList<>();
    protocols.add(this);
    protocols.addAll(Arrays.asList(others));

    int mergedReaderVersion =
        protocols.stream().mapToInt(Protocol::getMinReaderVersion).max().orElse(0);

    int mergedWriterVersion =
        protocols.stream().mapToInt(Protocol::getMinWriterVersion).max().orElse(0);

    Set<String> mergedReaderFeatures =
        protocols.stream().flatMap(p -> p.readerFeatures.stream()).collect(Collectors.toSet());

    Set<String> mergedWriterFeatures =
        protocols.stream().flatMap(p -> p.writerFeatures.stream()).collect(Collectors.toSet());

    Set<TableFeature> mergedImplicitFeatures =
        protocols.stream()
            .flatMap(p -> p.getImplicitlySupportedFeatures().stream())
            .collect(Collectors.toSet());

    Protocol mergedProtocol =
        new Protocol(
                mergedReaderVersion,
                mergedWriterVersion,
                mergedReaderFeatures,
                mergedWriterFeatures)
            .withFeatures(mergedImplicitFeatures);

    // The merged protocol is always normalized in order to represent the protocol
    // with the weakest possible form. This enables backward compatibility.
    // This is preceded by a denormalization step. This allows to fix invalid legacy Protocols.
    // For example, (2, 3) is normalized to (1, 3). This is because there is no legacy feature
    // in the set with reader version 2 unless the writer version is at least 5.
    return mergedProtocol.denormalizedNormalized();
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  /// Legacy method which will be removed after the table feature integration is done           ///
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public Protocol withNewWriterFeatures(Set<String> writerFeatures) {
    Tuple2<Integer, Integer> newProtocolVersions =
        TableFeatures.minProtocolVersionFromAutomaticallyEnabledFeatures(writerFeatures);
    Set<String> newWriterFeatures = new HashSet<>(writerFeatures);
    if (this.writerFeatures != null) {
      newWriterFeatures.addAll(this.writerFeatures);
    }
    return new Protocol(
        newProtocolVersions._1,
        newProtocolVersions._2,
        this.readerFeatures == null ? null : new HashSet<>(this.readerFeatures),
        newWriterFeatures);
  }

  /** Validate the protocol contents represents a valid state */
  protected void validate() {
    checkArgument(minReaderVersion >= 1, "minReaderVersion should be at least 1");
    checkArgument(minWriterVersion >= 1, "minWriterVersion should be at least 1");

    // expect the reader and writer features to be empty if the protocol version does not support
    checkArgument(
            readerFeatures.isEmpty() || supportsReaderFeatures,
            "Reader features are not supported for the reader version: " + minReaderVersion);
    checkArgument(writerFeatures.isEmpty() || supportsWriterFeatures,
            "Writer features are not supported for the writer version: " + minWriterVersion);

    // If reader versions are supported, expect the writer versions to be supported as well
    // We don't have any reader only features.
    if (supportsReaderFeatures) {
      checkArgument(supportsWriterFeatures,
              "writer version doesn't support writer features: " + minWriterVersion);
    }

    if (supportsWriterFeatures) {
      // ensure that the reader version supports all the readerWriter features
      Set<TableFeature> supportedFeatures = getExplicitlySupportedFeatures();
      supportedFeatures.stream()
                      .filter(TableFeature::isReaderWriterFeature)
                      .forEach(feature -> {
                                checkArgument(feature.minReaderVersion() <= minReaderVersion,
                                        format("Reader version %d does not support readerWriter feature %s",
                                                minReaderVersion, feature.featureName()));

                                if (supportsReaderFeatures) {
                                  // if the protocol supports reader features, then it should be part of the readerFeatures
                                    checkArgument(readerFeatures.contains(feature.featureName()),
                                            format("ReaderWriter feature %s is not present in readerFeatures", feature.featureName()));
                                }
                              });
    } else {
      // ensure we don't get (minReaderVersion, minWriterVersion) that satisfy the readerWriter feature
      // version requirements. E.g. (1, 5) is invalid as writer version indicates columnMapping supported
      // but reader version does not support it (requires 2).
      TableFeatures.TABLE_FEATURES.stream()
              .filter(TableFeature::isReaderWriterFeature)
              .forEach(f -> {
                if (f.minWriterVersion() <= minWriterVersion) {
                  checkArgument(
                          f.minReaderVersion() <= minReaderVersion,
                          format("Reader version %d does not support readerWriter feature %s", minReaderVersion, f.featureName())
                  );
                }
              });
    }
  }
}
