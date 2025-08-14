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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.transaction.ReplaceTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ReplaceTableTransactionBuilderV2Impl implements ReplaceTableTransactionBuilder {

  private final SnapshotImpl snapshot;
  private final StructType schema;
  private final String engineInfo;

  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<DataLayoutSpec> dataLayoutSpec = Optional.empty();
  private Optional<Integer> userProvidedMaxRetries = Optional.empty();

  public ReplaceTableTransactionBuilderV2Impl(
      SnapshotImpl snapshot, StructType schema, String engineInfo) {
    this.snapshot = requireNonNull(snapshot, "snapshot is null");
    this.schema = requireNonNull(schema, "schema is null");
    this.engineInfo = requireNonNull(engineInfo, "engineInfo is null");
    TableFeatures.validateKernelCanWriteToTable(
        snapshot.getProtocol(), snapshot.getMetadata(), snapshot.getPath());
  }

  @Override
  public ReplaceTableTransactionBuilder withTableProperties(Map<String, String> properties) {
    requireNonNull(properties, "properties cannot be null");
    this.tableProperties =
        Optional.of(
            java.util.Collections.unmodifiableMap(
                TableConfig.validateAndNormalizeDeltaProperties(properties)));
    return this;
  }

  @Override
  public ReplaceTableTransactionBuilder withDataLayoutSpec(DataLayoutSpec spec) {
    requireNonNull(spec, "spec cannot be null");
    this.dataLayoutSpec = Optional.of(spec);
    return this;
  }

  @Override
  public ReplaceTableTransactionBuilder withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries must be >= 0");
    this.userProvidedMaxRetries = Optional.of(maxRetries);
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    requireNonNull(engine, "engine cannot be null");

    Optional<List<String>> partitionColumns =
        dataLayoutSpec.filter(DataLayoutSpec::hasPartitioning)
            .map(DataLayoutSpec::getPartitionColumnsAsStrings);

    Optional<List<Column>> clusteringColumns =
        dataLayoutSpec.filter(DataLayoutSpec::hasClustering)
            .map(DataLayoutSpec::getClusteringColumns);

    TransactionMetadataFactory.Output txnMetadata =
        TransactionMetadataFactory.buildReplaceTableMetadata(
            snapshot.getPath(),
            snapshot,
            schema,
            tableProperties.orElse(emptyMap()),
            partitionColumns,
            clusteringColumns);

    return new TransactionImpl(
        true, // isCreateOrReplace
        snapshot.getDataPath(),
        Optional.of(snapshot),
        engineInfo,
        Operation.REPLACE_TABLE,
        txnMetadata.newProtocol,
        txnMetadata.newMetadata,
        snapshot.getCommitter(),
        Optional.empty(), // no setTransaction for replace table
        txnMetadata.physicalNewClusteringColumns,
        // We don't support conflict resolution yet for replace so disable retries for now
        Optional.of(0),
        0, // logCompactionInterval
        System::currentTimeMillis);
  }
}
