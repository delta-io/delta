/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.clustering;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Resolved descriptor for a single clustering column on a Delta snapshot: the physical column
 * reference (as stored in the {@code delta.clustering} domain), the logical column reference (as it
 * appears in the snapshot's logical schema), and the column's data type.
 *
 * <p>The logical reference always carries the casing of the snapshot's schema (the schema walk
 * matches by case-insensitive name). With column mapping enabled the physical reference holds the
 * stable identifiers stored in the domain JSON and the two references diverge; without column
 * mapping, the references differ only when the domain JSON's column-name casing differs from the
 * schema's. Callers should not assume any particular format for the physical identifiers (e.g.
 * {@code col-<uuid>}); the format depends on the column-mapping mode and whether the table was new
 * at the time mapping was enabled. Nested-field clustering shows up as a {@link Column} with more
 * than one name part.
 *
 * <p>Produced by {@link io.delta.kernel.Snapshot#getClusteringColumnInfos()} and the static {@link
 * #resolve(StructType, Column)} factory.
 *
 * @since 4.3.0
 */
@Evolving
public final class ClusteringColumnInfo {

  /**
   * The Delta domain name that holds the clustering-columns configuration. Returned for callers
   * that want to query {@link io.delta.kernel.Snapshot#getDomainMetadata(String)} directly.
   */
  public static final String CLUSTERING_DOMAIN_NAME = ClusteringMetadataDomain.DOMAIN_NAME;

  private final Column physicalColumn;
  private final Column logicalColumn;
  private final DataType dataType;

  /**
   * Production callers should use {@link #resolve(StructType, Column)} to construct instances so
   * the (physical, logical, data type) triple stays consistent with the snapshot's schema.
   */
  @VisibleForTesting
  ClusteringColumnInfo(Column physicalColumn, Column logicalColumn, DataType dataType) {
    this.physicalColumn = Objects.requireNonNull(physicalColumn, "physicalColumn");
    this.logicalColumn = Objects.requireNonNull(logicalColumn, "logicalColumn");
    this.dataType = Objects.requireNonNull(dataType, "dataType");
  }

  /**
   * @return the physical column reference as stored in the {@code delta.clustering} domain.
   *     Multi-part for nested-field clustering.
   */
  public Column getPhysicalColumn() {
    return physicalColumn;
  }

  /**
   * @return the logical column reference resolved against the snapshot's schema (so its casing
   *     follows the schema). Multi-part for nested-field clustering. Without column mapping this
   *     equals the physical column whenever the domain JSON's casing matches the schema.
   */
  public Column getLogicalColumn() {
    return logicalColumn;
  }

  /** @return the data type of the column at the resolved logical path. */
  public DataType getDataType() {
    return dataType;
  }

  /**
   * Resolve a single physical clustering column against the given schema. Walks the physical name
   * path part-by-part and produces the corresponding logical column reference and data type.
   *
   * <p>The schema walk is column-mapping-aware: each part is matched by the column's physical-name
   * field metadata when present, falling back to the field name when the metadata is absent.
   *
   * @throws KernelException if a physical name part doesn't match any field at the current schema
   *     level.
   */
  public static ClusteringColumnInfo resolve(StructType schema, Column physicalColumn) {
    Tuple2<Column, DataType> logical =
        ColumnMapping.getLogicalColumnNameAndDataType(schema, physicalColumn);
    return new ClusteringColumnInfo(physicalColumn, logical._1, logical._2);
  }

  /**
   * Resolve every physical clustering column against the given schema. Returns descriptors in the
   * input order.
   *
   * @throws KernelException if any physical column cannot be resolved against {@code schema}.
   */
  public static List<ClusteringColumnInfo> resolveAll(
      StructType schema, List<Column> physicalColumns) {
    return physicalColumns.stream()
        .map(physCol -> resolve(schema, physCol))
        .collect(Collectors.toList());
  }

  /**
   * Parse the {@code delta.clustering} domain configuration JSON and resolve each physical
   * clustering column against the given schema.
   *
   * @throws KernelException if {@code domainJson} is not a valid clustering domain configuration,
   *     or if a physical clustering column cannot be resolved against {@code schema}.
   */
  public static List<ClusteringColumnInfo> resolveAllFromDomainJson(
      StructType schema, String domainJson) {
    return resolveAll(
        schema, ClusteringMetadataDomain.fromJsonConfiguration(domainJson).getClusteringColumns());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClusteringColumnInfo)) {
      return false;
    }
    ClusteringColumnInfo that = (ClusteringColumnInfo) o;
    return physicalColumn.equals(that.physicalColumn)
        && logicalColumn.equals(that.logicalColumn)
        && dataType.equals(that.dataType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(physicalColumn, logicalColumn, dataType);
  }

  @Override
  public String toString() {
    return "ClusteringColumnInfo{physical="
        + physicalColumn
        + ", logical="
        + logicalColumn
        + ", dataType="
        + dataType
        + "}";
  }
}
