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

package io.delta.flink.sink.dynamic;

import io.delta.flink.table.DeltaTable;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test {@link DeltaTable} that records partition literal maps passed to {@link #writeParquet} and
 * arguments passed to {@link #commit} / {@link #refresh}, without Hadoop or a real Delta log.
 */
final class StubDeltaTable implements DeltaTable {

  private final String id;
  private final StructType schema;
  private final List<String> partitionColumns;

  private final AtomicInteger refreshCalls = new AtomicInteger();

  final List<Map<String, Literal>> partitionMaps = Collections.synchronizedList(new ArrayList<>());

  final List<CommitRecord> commitRecords = Collections.synchronizedList(new ArrayList<>());

  StubDeltaTable(String id, StructType schema, List<String> partitionColumns) {
    this.id = id;
    this.schema = schema;
    this.partitionColumns = partitionColumns;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @Override
  public void open() {}

  @Override
  public Optional<Snapshot> commit(
      CloseableIterable<Row> actions, String appId, long txnId, Map<String, String> properties) {
    int actionCount = 0;
    try (CloseableIterator<Row> it = actions.iterator()) {
      while (it.hasNext()) {
        it.next();
        actionCount++;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    commitRecords.add(new CommitRecord(appId, txnId, Map.copyOf(properties), actionCount));
    return Optional.empty();
  }

  @Override
  public void refresh() {
    refreshCalls.incrementAndGet();
  }

  int getRefreshCallCount() {
    return refreshCalls.get();
  }

  @Override
  public void close() {}

  @Override
  public CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues)
      throws IOException {
    partitionMaps.add(new LinkedHashMap<>(partitionValues));
    data.close();
    return new CloseableIterator<Row>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Row next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {}
    };
  }

  /** One {@link DeltaTable#commit} invocation recorded by this stub. */
  static final class CommitRecord {
    private final String appId;
    private final long txnId;
    private final Map<String, String> properties;
    private final int actionCount;

    private CommitRecord(
        String appId, long txnId, Map<String, String> properties, int actionCount) {
      this.appId = appId;
      this.txnId = txnId;
      this.properties = properties;
      this.actionCount = actionCount;
    }

    String getAppId() {
      return appId;
    }

    long getTxnId() {
      return txnId;
    }

    Map<String, String> getProperties() {
      return properties;
    }

    int getActionCount() {
      return actionCount;
    }
  }
}
