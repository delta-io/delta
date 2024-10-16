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

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Pair;

class DeltaSnapshot implements Snapshot, HistoryEntry {
  private final SnapshotImpl wrapped;
  private final Metadata metadata;
  private final LogSegment log;
  private final Engine deltaEngine;
  private final String location;
  private Schema schema = null;
  private NameMapping mapping = null;
  private boolean hasMissingFieldIds = true;
  private PartitionSpec spec = null;
  private Map<String, String> summary = null;

  private List<DataFile> addedFiles = null;
  private List<DataFile> removedFiles = null;

  DeltaSnapshot(String location, io.delta.kernel.Snapshot snapshot, Engine deltaEngine) {
    Preconditions.checkState(
        snapshot instanceof SnapshotImpl,
        "Unsupported snapshot implementation: %s",
        snapshot.getClass());
    this.location = location;
    this.wrapped = (SnapshotImpl) snapshot;
    this.metadata = wrapped.getMetadata();
    this.log = wrapped.getLogSegment();
    this.deltaEngine = deltaEngine;
  }

  public io.delta.kernel.Snapshot deltaSnapshot() {
    return wrapped;
  }

  /** Returns whether the snapshot's current table schema can be written by Iceberg. */
  boolean canWrite() {
    return columnMappingEnabled() && !hasMissingFieldIds();
  }

  Metadata metadata() {
    return metadata;
  }

  public Schema schema() {
    if (null == schema) {
      // use the current version as the schema ID to ensure uniqueness
      int schemaId = Math.toIntExact(log.version);
      int lastAssignedId = lastAssignedFieldId();
      Pair<NameMapping, Integer> updatedMapping =
          DeltaTypeUtil.updateNameMapping(metadata.getSchema(), nameMapping(), lastAssignedId);

      // if the updated mapping contains new IDs, then some field IDs were missing
      this.hasMissingFieldIds = updatedMapping.second() != lastAssignedId;

      // convert the schema with the updated mapping so that the snapshot can be read
      this.schema =
          new Schema(
              schemaId,
              DeltaTypeUtil.convert(metadata.getSchema(), updatedMapping.first()).fields());
    }

    return schema;
  }

  public PartitionSpec spec() {
    if (null == spec) {
      this.spec =
          DeltaPartitionUtil.convert(
              schema(), DeltaFileUtil.asStringList(metadata.getPartitionColumns()));
    }

    return spec;
  }

  @Override
  public Integer schemaId() {
    return schema().schemaId();
  }

  @Override
  public long sequenceNumber() {
    return log.version;
  }

  @Override
  public long snapshotId() {
    return log.version;
  }

  @Override
  public Long parentId() {
    if (log.version == 0) {
      return null;
    }
    return log.version - 1;
  }

  @Override
  public long timestampMillis() {
    return log.lastCommitTimestamp;
  }

  @Override
  public List<ManifestFile> allManifests(FileIO io) {
    throw new UnsupportedOperationException("Delta tables do not contain manifests");
  }

  @Override
  public List<ManifestFile> dataManifests(FileIO io) {
    throw new UnsupportedOperationException("Delta tables do not contain manifests");
  }

  @Override
  public List<ManifestFile> deleteManifests(FileIO io) {
    throw new UnsupportedOperationException("Delta tables do not contain manifests");
  }

  @Override
  public String operation() {
    // the safest operation is "overwrite" because it may change any file
    return DataOperations.OVERWRITE;
  }

  @Override
  public Map<String, String> summary() {
    if (null == summary) {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      // TODO: find out where Delta kernel keeps the operation metadata
      //      Optional.ofNullable(commitInfo.getOperation())
      //          .ifPresent(op -> builder.put("delta-operation", op));
      //      Optional.ofNullable(commitInfo.getOperationParameters())
      //          .ifPresent(
      //              params -> {
      //                for (Map.Entry<String, String> param : params.entrySet()) {
      //                  if (param.getValue() != null) {
      //                    builder.put(param);
      //                  }
      //                }
      //              });
      //      commitInfo.getIsolationLevel().ifPresent(level -> builder.put("isolation-level",
      // level));
      //      commitInfo.getClusterId().ifPresent(cluster -> builder.put("cluster-id", cluster));
      //      commitInfo.getEngineInfo().ifPresent(engine -> builder.put("engine-info", engine));
      //      commitInfo
      //          .getIsBlindAppend()
      //          .ifPresent(blind -> builder.put("blind-append", blind.toString()));
      //
      //      // ignore CommitInfo#getVersion because it is set when accessing the commit info

      this.summary = builder.build();
    }

    return summary;
  }

  private void cacheChanges() {
    if (null == addedFiles) {
      ImmutableList.Builder<DataFile> addedBuilder = ImmutableList.builder();
      ImmutableList.Builder<DataFile> removedBuilder = ImmutableList.builder();

      CloseableIterator<FilteredColumnarBatch> columnarBatches =
          ((ScanImpl) deltaSnapshot().getScanBuilder(deltaEngine).build())
              .getScanFiles(deltaEngine, true /* includeStats */);

      while (columnarBatches.hasNext()) {
        FilteredColumnarBatch columnarBatch = columnarBatches.next();
        DeltaFileUtil.files(location, schema(), spec(), columnarBatch)
            .iterator()
            .forEachRemaining(
                pair -> {
                  DataFile dataFile = pair.first();
                  DeleteFile deleteFile = pair.second();
                  if (dataFile != null) {
                    addedBuilder.add(dataFile);
                  }
                });
      }

      this.addedFiles = addedBuilder.build();
      this.removedFiles = removedBuilder.build();
    }
  }

  @Override
  public Iterable<DataFile> addedDataFiles(FileIO io) {
    cacheChanges();
    return addedFiles;
  }

  @Override
  public Iterable<DataFile> removedDataFiles(FileIO io) {
    cacheChanges();
    return removedFiles;
  }

  @Override
  public String manifestListLocation() {
    return null;
  }

  boolean columnMappingEnabled() {
    return ColumnMapping.ColumnMappingMode.NAME.equals(
        ColumnMapping.getColumnMappingMode(metadata.getConfiguration()));
  }

  boolean hasMissingFieldIds() {
    schema(); // hasMissingFieldIds is set while converting the schema
    return hasMissingFieldIds;
  }

  int lastAssignedFieldId() {
    Map<String, String> config = metadata.getConfiguration();
    String lastAssignedIdString = config.get(DeltaTable.LAST_ASSIGNED_ID_KEY);
    if (lastAssignedIdString != null) {
      return Integer.parseInt(lastAssignedIdString);
    } else {
      return 0;
    }
  }

  NameMapping nameMapping() {
    if (null == mapping) {
      String nameMappingStr = metadata.getConfiguration().get(DeltaTable.NAME_MAPPING_KEY);
      if (nameMappingStr != null) {
        this.mapping = NameMappingParser.fromJson(nameMappingStr);
      }
    }

    return mapping;
  }
}
