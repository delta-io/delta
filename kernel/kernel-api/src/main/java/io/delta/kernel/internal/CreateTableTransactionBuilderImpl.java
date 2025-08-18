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
import static io.delta.kernel.internal.util.Utils.resolvePath;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.*;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import java.util.*;

public class CreateTableTransactionBuilderImpl implements CreateTableTransactionBuilder {

  private final String unresolvedPath;
  private final StructType schema;
  private final String engineInfo;

  private Optional<Map<String, String>> tableProperties = Optional.empty();
  private Optional<DataLayoutSpec> dataLayoutSpec = Optional.empty();
  private Optional<Integer> userProvidedMaxRetries = Optional.empty();
  private Optional<Committer> userProvidedCommitter = Optional.empty();

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
    this.userProvidedMaxRetries = Optional.of(maxRetries);
    return this;
  }

  @Override
  public CreateTableTransactionBuilder withCommitter(Committer committer) {
    userProvidedCommitter = Optional.of(requireNonNull(committer, "committer cannot be null"));
    return this;
  }

  @Override
  public Transaction build(Engine engine) {
    requireNonNull(engine, "engine cannot be null");
    String resolvedPath = resolvePath(engine, unresolvedPath);
    throwIfTableAlreadyExists(engine, resolvedPath);

    // Extract partition and clustering columns from the data layout spec
    Optional<List<String>> partitionColumns =
        dataLayoutSpec
            .filter(DataLayoutSpec::hasPartitioning)
            .map(DataLayoutSpec::getPartitionColumnsAsStrings);
    Optional<List<Column>> clusteringColumns =
        dataLayoutSpec
            .filter(DataLayoutSpec::hasClustering)
            .map(DataLayoutSpec::getClusteringColumns);

    TransactionMetadataFactory.Output txnMetadata =
        TransactionMetadataFactory.buildCreateTableMetadata(
            resolvedPath,
            schema,
            tableProperties.orElse(emptyMap()),
            partitionColumns,
            clusteringColumns);

    Path dataPath = new Path(resolvedPath);
    return new TransactionImpl(
        true, // isCreateOrReplace
        dataPath,
        Optional.empty(), // no existing snapshot for create table
        engineInfo,
        Operation.CREATE_TABLE,
        txnMetadata.newProtocol,
        txnMetadata.newMetadata,
        userProvidedCommitter.orElse(DefaultFileSystemManagedTableOnlyCommitter.INSTANCE),
        Optional.empty(), // no setTransaction for create table
        txnMetadata.physicalNewClusteringColumns,
        userProvidedMaxRetries,
        0, // logCompactionInterval - no compaction for new table
        System::currentTimeMillis);
  }

  private void throwIfTableAlreadyExists(Engine engine, String tablePath) {
    String catalogManagedFeaturePropKey =
        TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX
            + TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName();
    boolean isCatalogManaged =
        tableProperties
            .map(props -> props.get(catalogManagedFeaturePropKey))
            .map("supported"::equals)
            .orElse(false);
    if (isCatalogManaged) {
      // For catalog managed tables we assume the catalog has ensured the table loc is not already
      // a Delta table; return early
      return;
    }
    // Otherwise, try loading the latest snapshot to ensure the table does not exist
    try {
      Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
      throw new TableAlreadyExistsException(
          tablePath, "Found table with latest version " + snapshot.getVersion());
    } catch (TableNotFoundException tblf) {
      // This is the desired scenario as the table should not exist yet
    }
  }
}
