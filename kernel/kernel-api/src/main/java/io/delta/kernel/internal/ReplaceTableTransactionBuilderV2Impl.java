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
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.transaction.ReplaceTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.*;

public class ReplaceTableTransactionBuilderV2Impl implements ReplaceTableTransactionBuilder {

  /**
   * Delta-specific properties that should be preserved during REPLACE operations, unless their
   * value is specifically set (overridden) during the REPLACE. All other properties should be
   * reset.
   *
   * <p>For example, suppose at the time of REPLACE the table has property 'delta.foo' = 'bar' and
   * that such property is included in this set.
   *
   * <ul>
   *   <li>If the REPLACE statement does not specify 'delta.foo', then the new table will still have
   *       'delta.foo' = 'bar'.
   *   <li>If the REPLACE statement specifies 'delta.foo' = 'baz', then the new table will of course
   *       have 'delta.foo' = 'baz'.
   * </ul>
   */
  static final Set<String> TABLE_PROPERTY_KEYS_TO_PRESERVE =
      new HashSet<String>() {
        {
          add(TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey());

          // Must retail all ICT properties, else a client would not know when ICT was enabled,
          // which could result in a failed query or incorrect results.
          //
          // If ICT is explicitly disabled during REPLACE (or during any operation), we should then
          // explicitly remove the ICT enablement version and timestamp properties.
          add(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey());
          add(TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey());
          add(TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey());
        }
      };

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
        dataLayoutSpec
            .filter(DataLayoutSpec::hasPartitioning)
            .map(DataLayoutSpec::getPartitionColumnsAsStrings);

    Optional<List<Column>> clusteringColumns =
        dataLayoutSpec
            .filter(DataLayoutSpec::hasClustering)
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
