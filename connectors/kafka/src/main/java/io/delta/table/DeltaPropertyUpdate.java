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

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterable;
import java.util.Map;
import java.util.Set;
import org.apache.commons.compress.utils.Sets;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class DeltaPropertyUpdate implements UpdateProperties {
  private final DeltaTable table;
  private final Table deltaTable;
  private final Engine deltaEngine;
  private final Map<String, String> adds = Maps.newHashMap();
  private final Set<String> removals = Sets.newHashSet();

  public DeltaPropertyUpdate(DeltaTable table, Table deltaTable, Engine deltaEngine) {
    this.table = table;
    this.deltaTable = deltaTable;
    this.deltaEngine = deltaEngine;
  }

  @Override
  public UpdateProperties set(String key, String value) {
    adds.put(key, value);
    return this;
  }

  @Override
  public UpdateProperties remove(String key) {
    removals.add(key);
    return this;
  }

  @Override
  public UpdateProperties defaultFormat(FileFormat format) {
    throw new UnsupportedOperationException("Delta tables support only Parquet files");
  }

  @Override
  public Map<String, String> apply() {
    SnapshotImpl snapshot = (SnapshotImpl) deltaTable.getLatestSnapshot(deltaEngine);
    return internalApply(snapshot.getMetadata());
  }

  private Map<String, String> internalApply(Metadata metadata) {
    Map<String, String> updated = Maps.newHashMap(metadata.getConfiguration());
    removals.forEach(updated::remove);
    updated.putAll(adds);

    return ImmutableMap.copyOf(updated);
  }

  @Override
  public void commit() {
    // TODO: add retries using Tasks
    SnapshotImpl snapshot = (SnapshotImpl) deltaTable.getLatestSnapshot(deltaEngine);
    Metadata latestMetadata = snapshot.getMetadata();

    Metadata newMetadata =
        new Metadata(
            latestMetadata.getId(),
            latestMetadata.getName(),
            latestMetadata.getDescription(),
            latestMetadata.getFormat(),
            latestMetadata.getSchemaString(),
            latestMetadata.getSchema(),
            latestMetadata.getPartitionColumns(),
            latestMetadata.getCreatedTime(),
            VectorUtils.stringStringMapValue(internalApply(latestMetadata)));

    deltaTable
        .createTransactionBuilder(deltaEngine, IcebergBuild.fullVersion(), Operation.MANUAL_UPDATE)
        .build(deltaEngine)
        .commit(
            deltaEngine,
            CloseableIterable.inMemoryIterable(
                toCloseableIterator(
                    ImmutableList.of(SingleAction.createMetadataSingleAction(newMetadata.toRow()))
                        .iterator())));

    table.refresh();
  }
}
