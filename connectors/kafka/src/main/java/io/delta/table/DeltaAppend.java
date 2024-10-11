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
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.shaded.com.google.common.collect.Iterables;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class DeltaAppend implements AppendFiles {
  private final DeltaTable table;
  private final Table deltaTable;
  private final Engine deltaEngine;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final List<DataFile> appendedFiles = Lists.newArrayList();
  private final Map<String, String> parameters = Maps.newHashMap();

  DeltaAppend(DeltaTable table, Table deltaTable, Engine deltaEngine) {
    this.table = table;
    this.deltaTable = deltaTable;
    this.deltaEngine = deltaEngine;
    parameters.put("iceberg-operation", DataOperations.APPEND);
  }

  @Override
  public AppendFiles appendFile(DataFile file) {
    appendedFiles.add(file);
    summaryBuilder.addedFile(table.spec(), file);
    return this;
  }

  @Override
  public AppendFiles appendManifest(ManifestFile manifest) {
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, table.io(), table.specs())) {
      for (DataFile append : reader) {
        appendedFiles.add(append);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close manifest: " + manifest.path(), e);
    }

    return this;
  }

  @Override
  public AppendFiles set(String property, String value) {
    parameters.put(property, value);
    return this;
  }

  @Override
  public AppendFiles deleteWith(Consumer<String> deleteFunc) {
    return this;
  }

  @Override
  public AppendFiles stageOnly() {
    throw new UnsupportedOperationException("Cannot stage commits in Delta tables");
  }

  @Override
  public AppendFiles scanManifestsWith(ExecutorService executorService) {
    return this;
  }

  @Override
  public Snapshot apply() {
    return null; // TODO
  }

  @Override
  public void commit() {
    table.refresh();
    // Map<String, String> summary = summaryBuilder.build();
    long now = System.currentTimeMillis();

    Transaction transaction =
        deltaTable
            .createTransactionBuilder(deltaEngine, engineInfo(), Operation.WRITE)
            .build(deltaEngine);

    // pass the table location as a URI to construct relative paths
    URI root = URI.create(table.location());
    Iterable<Row> adds =
        Iterables.transform(
            appendedFiles,
            file -> DeltaFileUtil.addFile(root, table.schema(), table.spec(), now, file));

    transaction.commit(
        deltaEngine, CloseableIterable.inMemoryIterable(toCloseableIterator(adds.iterator())));
  }

  //  private static Map<String, String> toOperationMetrics(Map<String, String> summary) {
  //    return ImmutableMap.of(
  //        "numFiles",
  //        summary.get(SnapshotSummary.ADDED_FILES_PROP),
  //        "numOutputRows",
  //        summary.get(SnapshotSummary.ADDED_RECORDS_PROP),
  //        "numOutputBytes",
  //        summary.get(SnapshotSummary.ADDED_FILE_SIZE_PROP));
  //  }

  private static String engineInfo() {
    Map<String, String> env = EnvironmentContext.get();
    String engine = env.get(EnvironmentContext.ENGINE_NAME);
    String version = env.get(EnvironmentContext.ENGINE_VERSION);
    String icebergVersion = IcebergBuild.version();
    return String.format("%s/%s Iceberg/%s", engine, version, icebergVersion);
  }
}
