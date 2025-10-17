/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewVersion;

/** Represents a change to table or view metadata. */

/**
 * Changes: L91 Added back the deprecated API in 1.9.0
 *              public AddSchema(Schema schema, int lastColumnId)
 */
public interface MetadataUpdate extends Serializable {
  default void applyTo(TableMetadata.Builder metadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a table", this.getClass().getSimpleName()));
  }

  default void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply update %s to a view", this.getClass().getSimpleName()));
  }

  class AssignUUID implements MetadataUpdate {
    private final String uuid;

    public AssignUUID(String uuid) {
      this.uuid = uuid;
    }

    public String uuid() {
      return uuid;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.assignUUID(uuid);
    }

    @Override
    public void applyTo(ViewMetadata.Builder metadataBuilder) {
      metadataBuilder.assignUUID(uuid);
    }
  }

  class UpgradeFormatVersion implements MetadataUpdate {
    private final int formatVersion;

    public UpgradeFormatVersion(int formatVersion) {
      this.formatVersion = formatVersion;
    }

    public int formatVersion() {
      return formatVersion;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.upgradeFormatVersion(formatVersion);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.upgradeFormatVersion(formatVersion);
    }
  }

  class AddSchema implements MetadataUpdate {
    private final Schema schema;
    private final int lastColumnId;

    public AddSchema(Schema schema) {
      this(schema, schema.highestFieldId());
    }

    /**
     * Set the schema
     * @deprecated in 1.9.0
     */
    @Deprecated
    public AddSchema(Schema schema, int lastColumnId) {
      this.schema = schema;
      this.lastColumnId = lastColumnId;
    }

    public Schema schema() {
      return schema;
    }

    public int lastColumnId() {
      return lastColumnId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSchema(schema, lastColumnId);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.addSchema(schema);
    }
  }

  class SetCurrentSchema implements MetadataUpdate {
    private final int schemaId;

    public SetCurrentSchema(int schemaId) {
      this.schemaId = schemaId;
    }

    public int schemaId() {
      return schemaId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setCurrentSchema(schemaId);
    }
  }

  class AddPartitionSpec implements MetadataUpdate {
    private final UnboundPartitionSpec spec;

    public AddPartitionSpec(PartitionSpec spec) {
      this(spec.toUnbound());
    }

    public AddPartitionSpec(UnboundPartitionSpec spec) {
      this.spec = spec;
    }

    public UnboundPartitionSpec spec() {
      return spec;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addPartitionSpec(spec);
    }
  }

  class SetDefaultPartitionSpec implements MetadataUpdate {
    private final int specId;

    public SetDefaultPartitionSpec(int specId) {
      this.specId = specId;
    }

    public int specId() {
      return specId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setDefaultPartitionSpec(specId);
    }
  }

  class RemovePartitionSpecs implements MetadataUpdate {
    private final Set<Integer> specIds;

    public RemovePartitionSpecs(Set<Integer> specIds) {
      this.specIds = specIds;
    }

    public Set<Integer> specIds() {
      return specIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSpecs(specIds);
    }
  }

  class RemoveSchemas implements MetadataUpdate {
    private final Set<Integer> schemaIds;

    public RemoveSchemas(Set<Integer> schemaIds) {
      this.schemaIds = schemaIds;
    }

    public Set<Integer> schemaIds() {
      return schemaIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSchemas(schemaIds);
    }
  }

  class AddSortOrder implements MetadataUpdate {
    private final UnboundSortOrder sortOrder;

    public AddSortOrder(SortOrder sortOrder) {
      this(sortOrder.toUnbound());
    }

    public AddSortOrder(UnboundSortOrder sortOrder) {
      this.sortOrder = sortOrder;
    }

    public UnboundSortOrder sortOrder() {
      return sortOrder;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSortOrder(sortOrder);
    }
  }

  class SetDefaultSortOrder implements MetadataUpdate {
    private final int sortOrderId;

    public SetDefaultSortOrder(int sortOrderId) {
      this.sortOrderId = sortOrderId;
    }

    public int sortOrderId() {
      return sortOrderId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setDefaultSortOrder(sortOrderId);
    }
  }

  class SetStatistics implements MetadataUpdate {
    private final StatisticsFile statisticsFile;

    public SetStatistics(StatisticsFile statisticsFile) {
      this.statisticsFile = statisticsFile;
    }

    public long snapshotId() {
      return statisticsFile.snapshotId();
    }

    public StatisticsFile statisticsFile() {
      return statisticsFile;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setStatistics(statisticsFile);
    }
  }

  class RemoveStatistics implements MetadataUpdate {
    private final long snapshotId;

    public RemoveStatistics(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeStatistics(snapshotId);
    }
  }

  class SetPartitionStatistics implements MetadataUpdate {
    private final PartitionStatisticsFile partitionStatisticsFile;

    public SetPartitionStatistics(PartitionStatisticsFile partitionStatisticsFile) {
      this.partitionStatisticsFile = partitionStatisticsFile;
    }

    public long snapshotId() {
      return partitionStatisticsFile.snapshotId();
    }

    public PartitionStatisticsFile partitionStatisticsFile() {
      return partitionStatisticsFile;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setPartitionStatistics(partitionStatisticsFile);
    }
  }

  class RemovePartitionStatistics implements MetadataUpdate {
    private final long snapshotId;

    public RemovePartitionStatistics(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removePartitionStatistics(snapshotId);
    }
  }

  class AddSnapshot implements MetadataUpdate {
    private final Snapshot snapshot;

    public AddSnapshot(Snapshot snapshot) {
      this.snapshot = snapshot;
    }

    public Snapshot snapshot() {
      return snapshot;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.addSnapshot(snapshot);
    }
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; Use {@link MetadataUpdate.RemoveSnapshots}
   *     instead.
   */
  @Deprecated
  class RemoveSnapshot implements MetadataUpdate {
    private final long snapshotId;

    public RemoveSnapshot(long snapshotId) {
      this.snapshotId = snapshotId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSnapshots(ImmutableSet.of(snapshotId));
    }
  }

  class RemoveSnapshots implements MetadataUpdate {
    private final Set<Long> snapshotIds;

    public RemoveSnapshots(long snapshotId) {
      this.snapshotIds = ImmutableSet.of(snapshotId);
    }

    public RemoveSnapshots(Set<Long> snapshotIds) {
      this.snapshotIds = snapshotIds;
    }

    public Set<Long> snapshotIds() {
      return snapshotIds;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeSnapshots(snapshotIds);
    }
  }

  class RemoveSnapshotRef implements MetadataUpdate {
    private final String refName;

    public RemoveSnapshotRef(String refName) {
      this.refName = refName;
    }

    public String name() {
      return refName;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeRef(refName);
    }
  }

  class SetSnapshotRef implements MetadataUpdate {
    private final String refName;
    private final Long snapshotId;
    private final SnapshotRefType type;
    private final Integer minSnapshotsToKeep;
    private final Long maxSnapshotAgeMs;
    private final Long maxRefAgeMs;

    public SetSnapshotRef(
        String refName,
        Long snapshotId,
        SnapshotRefType type,
        Integer minSnapshotsToKeep,
        Long maxSnapshotAgeMs,
        Long maxRefAgeMs) {
      this.refName = refName;
      this.snapshotId = snapshotId;
      this.type = type;
      this.minSnapshotsToKeep = minSnapshotsToKeep;
      this.maxSnapshotAgeMs = maxSnapshotAgeMs;
      this.maxRefAgeMs = maxRefAgeMs;
    }

    public String name() {
      return refName;
    }

    public String type() {
      return type.name().toLowerCase(Locale.ROOT);
    }

    public long snapshotId() {
      return snapshotId;
    }

    public Integer minSnapshotsToKeep() {
      return minSnapshotsToKeep;
    }

    public Long maxSnapshotAgeMs() {
      return maxSnapshotAgeMs;
    }

    public Long maxRefAgeMs() {
      return maxRefAgeMs;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      SnapshotRef ref =
          SnapshotRef.builderFor(snapshotId, type)
              .minSnapshotsToKeep(minSnapshotsToKeep)
              .maxSnapshotAgeMs(maxSnapshotAgeMs)
              .maxRefAgeMs(maxRefAgeMs)
              .build();
      metadataBuilder.setRef(refName, ref);
    }
  }

  class SetProperties implements MetadataUpdate {
    private final Map<String, String> updated;

    public SetProperties(Map<String, String> updated) {
      this.updated = updated;
    }

    public Map<String, String> updated() {
      return updated;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setProperties(updated);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setProperties(updated);
    }
  }

  class RemoveProperties implements MetadataUpdate {
    private final Set<String> removed;

    public RemoveProperties(Set<String> removed) {
      this.removed = removed;
    }

    public Set<String> removed() {
      return removed;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.removeProperties(removed);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.removeProperties(removed);
    }
  }

  class SetLocation implements MetadataUpdate {
    private final String location;

    public SetLocation(String location) {
      this.location = location;
    }

    public String location() {
      return location;
    }

    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.setLocation(location);
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setLocation(location);
    }
  }

  class AddViewVersion implements MetadataUpdate {
    private final ViewVersion viewVersion;

    public AddViewVersion(ViewVersion viewVersion) {
      this.viewVersion = viewVersion;
    }

    public ViewVersion viewVersion() {
      return viewVersion;
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.addVersion(viewVersion);
    }
  }

  class SetCurrentViewVersion implements MetadataUpdate {
    private final int versionId;

    public SetCurrentViewVersion(int versionId) {
      this.versionId = versionId;
    }

    public int versionId() {
      return versionId;
    }

    @Override
    public void applyTo(ViewMetadata.Builder viewMetadataBuilder) {
      viewMetadataBuilder.setCurrentVersionId(versionId);
    }
  }

  /**
   * Update to enable row lineage.
   *
   * @deprecated will be removed in 1.10.0; row lineage is required for all v3+ tables.
   */
  @Deprecated
  class EnableRowLineage implements MetadataUpdate {
    @Override
    public void applyTo(TableMetadata.Builder metadataBuilder) {
      metadataBuilder.enableRowLineage();
    }
  }
}
