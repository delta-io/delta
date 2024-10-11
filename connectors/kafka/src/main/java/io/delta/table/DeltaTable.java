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
package io.delta.table;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.actions.Metadata;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;

public class DeltaTable implements org.apache.iceberg.Table {
  static final String LAST_ASSIGNED_ID_KEY = "delta.columnMapping.maxColumnId";
  static final String NAME_MAPPING_KEY = "delta.universalFormat.iceberg.nameMapping";
  private static final String COLUMN_MAPPING_MODE_KEY = "delta.columnMapping.mode";
  private static final String UNIFORM_FORMAT_KEY = "delta.universalFormat.enabledFormats";

  private final TableIdentifier ident;
  private final String deltaTableLocation;
  private final Table deltaTable;
  private final Engine deltaEngine;
  private final HadoopFileIO io;
  private final LoadingCache<Long, DeltaSnapshot> snapshots;

  private DeltaSnapshot currentVersion = null;
  private long lastUpdateId = 0;
  private long earliestVersionId = 0;
  private long currentVersionId = 0;

  public DeltaTable(TableIdentifier ident, Configuration conf, String deltaTableLocation) {
    this.ident = ident;
    this.deltaTableLocation = deltaTableLocation;
    this.deltaEngine = DefaultEngine.create(conf);
    this.deltaTable = Table.forPath(deltaEngine, deltaTableLocation);
    this.io = new HadoopFileIO(conf);
    this.snapshots =
        Caffeine.newBuilder()
            .build(
                version -> {
                  try {
                    return new DeltaSnapshot(
                        deltaTable.getSnapshotAsOfVersion(deltaEngine, version));
                  } catch (KernelException versionMissing) {
                    return null;
                  }
                });

    refresh();
  }

  @Override
  public String name() {
    return ident.toString();
  }

  @Override
  public UUID uuid() {
    return UUID.fromString(properties().get(TableProperties.UUID));
  }

  @Override
  public void refresh() {
    this.currentVersionId = deltaTable.getLatestSnapshot(deltaEngine).getVersion(deltaEngine);
    this.currentVersion = snapshots.get(currentVersionId);
  }

  private boolean ensureWritable() {
    if (currentVersion.canWrite()) {
      return true;
    }

    // check if the table's name mapping should be updated to enable writes
    String uniform = currentVersion.metadata().getConfiguration().get(UNIFORM_FORMAT_KEY);
    if (uniform != null && uniform.toLowerCase(Locale.ROOT).contains("iceberg")) {
      if (currentVersion.columnMappingEnabled() && currentVersion.hasMissingFieldIds()) {
        // update the name mapping to assign the missing field IDs
        Pair<NameMapping, Integer> updated =
            DeltaTypeUtil.updateNameMapping(
                currentVersion.metadata().getSchema(),
                currentVersion.nameMapping(),
                currentVersion.lastAssignedFieldId());

        String updatedMappingStr = NameMappingParser.toJson(updated.first());
        String newLastAssignedId = String.valueOf(updated.second());

        updateProperties()
            .set(NAME_MAPPING_KEY, updatedMappingStr)
            .set(LAST_ASSIGNED_ID_KEY, newLastAssignedId)
            .commit();

        refresh();
      }
    }

    return currentVersion.canWrite();
  }

  @Override
  public BatchScan newBatchScan() {
    return new DeltaTableScan(this, deltaTable, deltaEngine);
  }

  @Override
  @Deprecated // TODO: deprecate everywhere
  public TableScan newScan() {
    throw new UnsupportedOperationException("Use newBatchScan instead");
  }

  @Override
  public Schema schema() {
    return snapshots.get(currentVersionId).schema();
  }

  @Override
  public Map<Integer, Schema> schemas() {
    // load all available snapshots
    loadAllSnapshots();

    ImmutableMap.Builder<Integer, Schema> schemas = ImmutableMap.builder();
    for (DeltaSnapshot snapshot : snapshots.asMap().values()) {
      Schema snapshotSchema = snapshot.schema();
      schemas.put(snapshotSchema.schemaId(), snapshotSchema);
    }

    return schemas.build();
  }

  @Override
  public PartitionSpec spec() {
    return snapshots.get(currentVersionId).spec();
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    PartitionSpec spec = spec();
    if (spec.isPartitioned()) {
      return ImmutableMap.of(
          PartitionSpec.unpartitioned().specId(),
          PartitionSpec.unpartitioned(),
          spec.specId(),
          spec);
    } else {
      return ImmutableMap.of(spec.specId(), spec);
    }
  }

  @Override
  public SortOrder sortOrder() {
    return SortOrder.unsorted();
  }

  @Override
  public Map<Integer, SortOrder> sortOrders() {
    return ImmutableMap.of(SortOrder.unsorted().orderId(), SortOrder.unsorted());
  }

  @Override
  public Map<String, String> properties() {
    return asProperties(currentVersion.metadata());
  }

  private Map<String, String> asProperties(Metadata metadata) {
    // TODO: construct NameMapping from the schema
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.putAll(metadata.getConfiguration());
    builder.put("format", "delta/" + metadata.getFormat().getProvider());
    builder.put(TableProperties.UUID, metadata.getId());
    metadata.getDescription().ifPresent(desc -> builder.put("comment", desc));
    metadata
        .getCreatedTime()
        .ifPresent(ts -> builder.put("created-at", DateTimeUtil.formatTimestampMillis(ts)));

    return builder.build();
  }

  @Override
  public String location() {
    return deltaTableLocation;
  }

  @Override
  public Map<String, SnapshotRef> refs() {
    return ImmutableMap.of("main", SnapshotRef.branchBuilder(currentVersionId).build());
  }

  @Override
  public Snapshot currentSnapshot() {
    return snapshots.get(currentVersionId);
  }

  @Override
  public DeltaSnapshot snapshot(long version) {
    return snapshots.get(version);
  }

  @Override
  public Iterable<Snapshot> snapshots() {
    return ImmutableList.copyOf(orderedSnapshots());
  }

  @Override
  public List<HistoryEntry> history() {
    return ImmutableList.copyOf(orderedSnapshots());
  }

  @Override
  public UpdateSchema updateSchema() {
    throw new UnsupportedOperationException("Cannot update Delta table schema");
  }

  @Override
  public UpdatePartitionSpec updateSpec() {
    throw new UnsupportedOperationException("Delta tables do not support partition spec updates");
  }

  @Override
  public UpdateProperties updateProperties() {
    return new DeltaPropertyUpdate(this, deltaTable, deltaEngine);
  }

  @Override
  public ReplaceSortOrder replaceSortOrder() {
    throw new UnsupportedOperationException("Cannot update Delta table sort order");
  }

  @Override
  public UpdateLocation updateLocation() {
    throw new UnsupportedOperationException("Delta tables do not support location updates");
  }

  @Override
  public AppendFiles newAppend() {
    Preconditions.checkState(
        ensureWritable(), "Cannot write to Delta table %s: must have stable field IDs", name());
    return new DeltaAppend(this, deltaTable, deltaEngine);
  }

  @Override
  public RewriteFiles newRewrite() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public RewriteManifests rewriteManifests() {
    throw new UnsupportedOperationException("Delta tables do not support manifest rewrite");
  }

  @Override
  public OverwriteFiles newOverwrite() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public RowDelta newRowDelta() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public ReplacePartitions newReplacePartitions() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public DeleteFiles newDelete() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public ExpireSnapshots expireSnapshots() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public ManageSnapshots manageSnapshots() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public Transaction newTransaction() {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public FileIO io() {
    // TODO: may need a FileIO that calls through the engine's FileSystemClient
    return io;
  }

  @Override
  public EncryptionManager encryption() {
    return PlaintextEncryptionManager.instance();
  }

  @Override
  public LocationProvider locationProvider() {
    // TODO: make sure this works with Delta
    return LocationProviders.locationsFor(deltaTableLocation, ImmutableMap.of());
  }

  @Override
  public List<StatisticsFile> statisticsFiles() {
    return ImmutableList.of();
  }

  private void loadAllSnapshots() {
    if (lastUpdateId != currentVersionId) {
      for (long versionId = currentVersionId; versionId > earliestVersionId; versionId -= 1) {
        Snapshot snapshot = snapshots.get(versionId);
        if (null == snapshot) {
          this.earliestVersionId = versionId;
        }
      }

      this.lastUpdateId = currentVersionId;
    }
  }

  private Collection<DeltaSnapshot> orderedSnapshots() {
    loadAllSnapshots();
    SortedMap<Long, DeltaSnapshot> sortedMap = Maps.newTreeMap();
    sortedMap.putAll(snapshots.asMap());
    return sortedMap.values();
  }
}
