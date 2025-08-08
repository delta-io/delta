/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

public class CreateTableTransactionBuilderImpl implements CreateTableTransactionBuilder {

  private final String unresolvedPath;
  private final StructType schema;
  private final String engineInfo;

  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<DataLayoutSpec> dataLayoutSpec = Optional.empty();

  /**
   * Number of retries for concurrent write exceptions to resolve conflicts and retry commit. In
   * Delta-Spark, for historical reasons the number of retries is really high (10m). We are starting
   * with a lower number by default for now. If this is not sufficient we can update it.
   */
  private int maxRetries = 200;

  public CreateTableTransactionBuilderImpl(String tablePath, StructType schema, String engineInfo) {
    this.unresolvedPath = requireNonNull(tablePath, "tablePath is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.engineInfo = requireNonNull(engineInfo, "engineInfo is null");
  }

  @Override
  public CreateTableTransactionBuilder withTableProperties(Map<String, String> properties) {
    requireNonNull(properties, "properties cannot be null");
    this.tableProperties =
        Optional.of(
            Collections.unmodifiableMap(
                TableConfig.validateAndNormalizeDeltaProperties(properties)));
    return this;
  }

  @Override
  public CreateTableTransactionBuilder withDataLayoutSpec(DataLayoutSpec spec) {
    requireNonNull(spec, "spec cannot be null");
    this.dataLayoutSpec = Optional.of(spec);
    return this;
  }

  @Override
  public CreateTableTransactionBuilder withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
    this.maxRetries = maxRetries;
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    requireNonNull(engine, "engine cannot be null");
    String resolvedPath = resolveTablePath(engine, unresolvedPath);

    // Extract partition and clustering columns from the data layout spec
    Optional<List<String>> partitionColumns =
        dataLayoutSpec
            .filter(DataLayoutSpec::hasPartitioning)
            .map(
                spec ->
                    spec.getPartitionColumns().stream()
                        .map(col -> col.getNames()[0])
                        .collect(Collectors.toList()));
    Optional<List<Column>> clusteringColumns =
        dataLayoutSpec
            .filter(DataLayoutSpec::hasClustering)
            .map(DataLayoutSpec::getClusteringColumns);

    // Build the transaction metadata using TransactionMetadataFactory
    TransactionMetadataFactory.Output txnMetadata =
        TransactionMetadataFactory.buildCreateTableMetadata(
            resolvedPath,
            schema,
            tableProperties.orElse(Collections.emptyMap()),
            partitionColumns,
            clusteringColumns);

    Path dataPath = new Path(resolvedPath);
    Path logPath = new Path(dataPath, "_delta_log");
    return new TransactionImpl(
        true, // isCreateOrReplace
        dataPath,
        logPath,
        Optional.empty(), // no existing snapshot for create table
        engineInfo,
        Operation.CREATE_TABLE,
        txnMetadata.newProtocol,
        txnMetadata.newMetadata,
        Optional.empty(), // no setTransaction for create table
        txnMetadata.physicalNewClusteringColumns,
        maxRetries,
        0, // logCompactionInterval - using default for create table
        System::currentTimeMillis);
  }

  private String resolveTablePath(Engine engine, String tablePath) {
    try {
      return wrapEngineExceptionThrowsIO(
          () -> engine.getFileSystemClient().resolvePath(tablePath),
          "Resolving path %s",
          tablePath);
    } catch (IOException io) {
      throw new UncheckedIOException(io);
    }
  }
}
