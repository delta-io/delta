/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink.mergestrategy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.delta.flink.Conf;
import io.delta.flink.sink.*;
import io.delta.flink.table.DeltaTable;
import io.delta.flink.table.ExceptionUtils;
import io.delta.kernel.expressions.Literal;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link MergeStrategy} used by append-mode sinks.
 *
 * <p>In append mode every incoming row is written via {@link DeltaTable#writeParquet} and no
 * existing files need to be modified. The writer must never call {@link #delete} in append mode; if
 * it does, that's a programming error and we fail loudly.
 */
public class AppendOnly
    implements MergeStrategy, RemovalListener<Map<String, String>, DeltaWriterTask> {

  private Cache<Map<String, String>, DeltaWriterTask> writerTasksByPartition;

  private DeltaSinkWriter writer;

  private List<DeltaWriterResult> completedWrites;

  public void init(DeltaSinkWriter writer) {
    this.writer = writer;
    this.completedWrites = new ArrayList<>();
    this.writerTasksByPartition =
        Caffeine.newBuilder()
            .executor(Runnable::run)
            .maximumSize(Conf.getInstance().getSinkWriterNumConcurrentFiles())
            .removalListener(this)
            .build();
  }

  @Override
  public void insert(
      List<Literal> primaryKey,
      Map<String, Literal> partitionValues,
      RowData element,
      SinkWriter.Context context) {

    try {
      writerTasksByPartition
          .get(
              MergeStrategy.writerKey(partitionValues),
              (key) -> writer.newWriterTask(partitionValues))
          .write(element, context);
    } catch (Exception e) {
      throw ExceptionUtils.wrap(e);
    }
  }

  @Override
  public void upsert(
      List<Literal> primaryKey,
      Map<String, Literal> partitionValues,
      RowData element,
      SinkWriter.Context context) {
    throw new IllegalStateException(
        "AppendOnlyMergeStrategy received a upsert record; this should only happen in "
            + "upsert mode.");
  }

  @Override
  public void delete(List<Literal> primaryKey, Map<String, Literal> partitionValues) {
    throw new IllegalStateException(
        "AppendOnlyMergeStrategy received a delete record; this should only happen in "
            + "upsert mode.");
  }

  @Override
  public List<DeltaWriterResult> merge() {
    writerTasksByPartition.invalidateAll();
    List<DeltaWriterResult> results = List.copyOf(completedWrites);
    completedWrites.clear();
    return results;
  }

  @Override
  public void onRemoval(
      @Nullable Map<String, String> key, @Nullable DeltaWriterTask value, RemovalCause cause) {
    // Close the evicted task and collect its result
    try {
      if (value != null) {
        completedWrites.addAll(value.complete());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
