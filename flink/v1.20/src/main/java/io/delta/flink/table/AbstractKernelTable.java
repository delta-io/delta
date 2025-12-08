/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.table;

import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.delta.flink.Conf;
import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentTransactionException;
import io.delta.kernel.exceptions.ConcurrentWriteException;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract base class for {@link DeltaTable} implementations backed by the Delta Kernel.
 *
 * <p>{@code AbstractKernelTable} provides common functionality for interacting with Delta tables,
 * including access to table metadata, schema, partitioning information, and commit operations.
 * Concrete subclasses are responsible for supplying catalog-specific or filesystem-specific logic
 * such as table discovery, path resolution, and storage I/O.
 *
 * <p>This class centralizes shared behavior so that different table backends (e.g., Hadoop-based
 * tables, CCv2 catalog tables, custom catalogs) can implement only the backend-specific portions
 * while inheriting consistent Delta table semantics.
 *
 * <p>Subclasses must provide their own mechanisms for interpreting table identifiers and resolving
 * them into physical locations or catalog entries. See also @link{io.delta.flink.table.Catalog}
 */
public abstract class AbstractKernelTable implements DeltaTable {
  protected static String ENGINE_INFO = "DeltaSink";
  protected static Logger LOG = LoggerFactory.getLogger(AbstractKernelTable.class);

  protected final DeltaCatalog catalog;
  protected String tableId;
  protected String tableUUID;
  protected URI tablePath;
  protected final Map<String, String> configuration;
  /*
   * This is the TransactionStateRow in json. Needed mainly by {@link #writeParquet}
   */
  protected String serializedTableState;
  protected List<String> partitionColumns;

  private CacheManager cacheManager;
  private final Random randgen;

  protected transient volatile Engine engine;

  // These fields are not serializable. They will be reinitialized in {@link #open}
  protected transient StructType schema;
  protected transient Row tableState;
  protected transient CredentialManager credentialManager;
  // Single-thread thread pool for executing interruptible operation.
  protected transient ExecutorService refreshThreadPool = null;
  // Thread pool for all kinds of async works
  protected transient ExecutorService generalThreadPool = null;

  public AbstractKernelTable(
      DeltaCatalog catalog,
      String tableId,
      Map<String, String> conf,
      StructType schema,
      List<String> partitionColumns) {
    this.catalog = catalog;
    this.tableId = tableId;
    this.configuration = conf;
    this.schema = schema;
    this.partitionColumns = normalize(partitionColumns);

    this.cacheManager = CacheManager.getInstance();
    this.randgen = new Random(System.currentTimeMillis());

    this.refreshThreadPool = Executors.newSingleThreadExecutor();
    this.credentialManager = createCredentialManager();
    withRetry(
        () -> {
          initialize();
          return null;
        });
    open();
  }

  public AbstractKernelTable(DeltaCatalog catalog, String tableId, Map<String, String> conf) {
    this(catalog, tableId, conf, null, null);
  }

  protected void initialize() {
    DeltaCatalog.TableBrief info = catalog.getTable(tableId);
    tableUUID = info.uuid;
    tablePath = info.tablePath;
    // With an existing table, partitions loaded from the table take precedence
    final Optional<Snapshot> latestSnapshotOpt = snapshot();
    if (latestSnapshotOpt.isPresent()) {
      Snapshot latestSnapshot = latestSnapshotOpt.get();
      // We use a temporary transaction to generate a TransactionStateRow.
      // It serves as a holder for schema and partition columns.
      // The transaction will not be committed, and is discarded afterward.
      Row existingTableState =
          latestSnapshot
              .buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE)
              .build(getEngine())
              .getTransactionState(getEngine());
      this.serializedTableState = JsonUtils.rowToJson(existingTableState);
      this.schema = latestSnapshot.getSchema();
      this.partitionColumns = latestSnapshot.getPartitionColumnNames();
    } else {
      // Init the table in catalog
      getCatalog().createTable(tableId, schema, partitionColumns, configuration);
      CreateTableTransactionBuilder createTxnBuilder =
          TableManager.buildCreateTableTransaction(getTablePath().toString(), schema, ENGINE_INFO);
      if (!partitionColumns.isEmpty()) {
        createTxnBuilder.withDataLayoutSpec(
            DataLayoutSpec.partitioned(
                Optional.of(partitionColumns)
                    .map(
                        nonEmpty -> nonEmpty.stream().map(Column::new).collect(Collectors.toList()))
                    .orElseGet(Collections::emptyList)));
      }
      Row newTableState = createTxnBuilder.build(getEngine()).getTransactionState(getEngine());
      this.serializedTableState = JsonUtils.rowToJson(newTableState);
    }
  }

  // Engine will be invalidated when credentials expire
  protected Engine getEngine() {
    if (engine == null) {
      synchronized (this) {
        if (engine == null) {
          engine = createEngine();
        }
      }
    }
    return engine;
  }

  /**
   * Subclass may implement this method to generate an engine.
   *
   * @return engine to access the tables
   */
  protected Engine createEngine() {
    Configuration conf = new Configuration();

    // Built-in configurations for common file system access
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    conf.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs");

    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.path.style.access", "false");
    conf.set("fs.s3.impl.disable.cache", "true");
    conf.set("fs.s3a.impl.disable.cache", "true");

    conf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
    conf.set("fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
    conf.set("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");

    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    this.configuration.forEach(conf::set);
    this.credentialManager.getCredentials().forEach(conf::set);

    // Explicitly load external conf files
    // TODO this is because Flink does not auto load this file in Docker
    conf.addResource(new Path("/opt/flink/conf/core-site.xml"));

    return DefaultEngine.create(conf);
  }

  public CacheManager getCacheManager() {
    return cacheManager;
  }

  public void setCacheManager(CacheManager cacheManager) {
    this.cacheManager = cacheManager;
  }

  public DeltaCatalog getCatalog() {
    return catalog;
  }

  @Override
  public String getId() {
    return tableId;
  }

  public String getTableUUID() {
    return tableUUID;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  protected Row getWriteState() {
    return tableState;
  }

  /** The table storage location where all data and metadata files should be stored. */
  public URI getTablePath() {
    return tablePath;
  }

  @Override
  public StructType getSchema() {
    return schema;
  }

  @Override
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  private CredentialManager createCredentialManager() {
    return new CredentialManager(
        () -> catalog.getCredentials(this.getTableUUID()), this::onCredentialsRefreshed);
  }

  @Override
  public void open() {
    if (tableState == null) {
      // init all transient variables
      refreshThreadPool = Executors.newSingleThreadExecutor();
      generalThreadPool = Executors.newFixedThreadPool(Conf.getInstance().getTableThreadPoolSize());

      tableState = JsonUtils.rowFromJson(serializedTableState, TransactionStateRow.SCHEMA);
      schema = TransactionStateRow.getLogicalSchema(tableState);

      credentialManager = createCredentialManager();
    }
  }

  /**
   * Load snapshot using a separated thread. This will allow external request to interrupt the
   * thread during time-consuming operations in loading snapshot, such as log replay.
   *
   * @return loaded snapshot, null if the table does not exist
   */
  protected Optional<Snapshot> snapshot() {
    Function<String, Optional<Snapshot>> body =
        (key) -> {
          try {
            return Optional.of(refreshThreadPool.submit(this::loadLatestSnapshot).get());
          } catch (Exception e) {
            if (isTableNotFound.test(e)) {
              return Optional.empty();
            }
            throw new RuntimeException(e);
          }
        };
    String path = tablePath.toString();
    LOG.debug("Loading snapshot for path {}", path);
    return cacheManager.get(path, this::probeVersion, body);
  }

  @Override
  public synchronized void close() throws InterruptedException {
    LOG.info("Closing table : {}", getId());
    if (refreshThreadPool != null) {
      refreshThreadPool.shutdownNow();
      // This should return quickly if all tasks are interruptible
      refreshThreadPool.awaitTermination(10, TimeUnit.MINUTES);
      refreshThreadPool = null;
    }
  }

  /**
   * Subclass must implement this method to fetch a Kernel snapshot
   *
   * @return latest snapshot of the table
   */
  protected abstract Snapshot loadLatestSnapshot();

  /**
   * Subclass may implement this to achieve fast cache validation. This method is expected to be
   * faster than {@link #loadLatestSnapshot()}. The default implementation checks if a file with the
   * given version exists.
   *
   * @return the latest version of the table, null if unknown / not supported
   */
  protected boolean probeVersion(Long version) {
    try {
      new SnapshotManager(new io.delta.kernel.internal.fs.Path(getTablePath()))
          .getLogSegmentForVersion(getEngine(), Optional.of(version));
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void refresh() {
    refresh(null);
  }

  /** Refresh with the provided snapshot */
  protected void refresh(Snapshot snapshot) {
    withRetry(
        () -> {
          Snapshot currentSnapshot = snapshot;
          if (currentSnapshot == null) {
            currentSnapshot = snapshot().orElse(null);
          }
          if (currentSnapshot == null) {
            return null;
          }
          this.schema = currentSnapshot.getSchema();
          this.partitionColumns = currentSnapshot.getPartitionColumnNames();
          // Refresh table state
          this.tableState =
              currentSnapshot
                  .buildUpdateTableTransaction("dummy", Operation.WRITE)
                  .build(getEngine())
                  .getTransactionState(getEngine());
          this.serializedTableState = JsonUtils.rowToJson(this.tableState);
          return null;
        });
  }

  @Override
  public Optional<Snapshot> commit(
      CloseableIterable<Row> actions, String appId, long txnId, Map<String, String> properties) {
    return withRetry(
        () -> {
          Engine localEngine = getEngine();
          Transaction txn;
          Optional<Snapshot> snapshotOpt = snapshot();
          if (snapshotOpt.isPresent()) {
            Snapshot snapshot = snapshotOpt.get();
            UpdateTableTransactionBuilder txnBuilder =
                snapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE);
            txnBuilder.withTransactionId(appId, txnId);
            txnBuilder.withTablePropertiesAdded(properties);
            txn = txnBuilder.build(engine);
          } else {
            CreateTableTransactionBuilder txnBuilder =
                TableManager.buildCreateTableTransaction(
                    getTablePath().toString(), getSchema(), ENGINE_INFO);
            if (!getPartitionColumns().isEmpty()) {
              txnBuilder.withDataLayoutSpec(
                  DataLayoutSpec.partitioned(
                      getPartitionColumns().stream()
                          .map(Column::new)
                          .collect(Collectors.toList())));
            }
            txnBuilder.withTableProperties(properties);
            txn = txnBuilder.build(localEngine);
          }
          TransactionCommitResult result = txn.commit(localEngine, actions);
          result.getPostCommitSnapshot().ifPresent(this::refresh);

          Optional<Snapshot> postCommitSnapshot = result.getPostCommitSnapshot();
          postCommitSnapshot.ifPresent(
              snapshot -> {
                // Update cache
                cacheManager.put(getTablePath().toString(), snapshot);
                // Write checksum
                generalThreadPool.submit(
                    () -> {
                      try {
                        snapshot.writeChecksum(getEngine(), Snapshot.ChecksumWriteMode.SIMPLE);
                      } catch (Exception e) {
                        LOG.error("unable to write checksum", e);
                        throw new RuntimeException(e);
                      }
                    });
                // Occasionally write checkpoint
                if (randgen.nextDouble() < Conf.getInstance().getDeltaCheckpointFrequency()) {
                  generalThreadPool.submit(
                      () -> {
                        try {
                          snapshot.writeCheckpoint(getEngine());
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      });
                }
              });
          return postCommitSnapshot;
        });
  }

  @Override
  public CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues) {
    return withRetry(
        () -> {
          Engine localEngine = getEngine();
          Row writeState = getWriteState();

          final CloseableIterator<FilteredColumnarBatch> physicalData =
              Transaction.transformLogicalData(localEngine, writeState, data, partitionValues);

          final DataWriteContext writeContext =
              Transaction.getWriteContext(localEngine, writeState, partitionValues);
          LOG.debug("Writing file to path {} with suffix {}", getTablePath(), pathSuffix);
          final CloseableIterator<DataFileStatus> dataFiles =
              localEngine
                  .getParquetHandler()
                  .writeParquetFiles(
                      getTablePath().resolve(pathSuffix).toString(),
                      physicalData,
                      writeContext.getStatisticsColumns());
          return Transaction.generateAppendActions(
              localEngine, writeState, dataFiles, writeContext);
        });
  }

  protected <RET> RET withRetry(CheckedSupplier<RET> body) {
    RetryPolicy<Object> retryPolicy =
        RetryPolicy.builder()
            .handleIf(AbstractKernelTable::isRetryableException)
            .withBackoff(
                Duration.ofMillis(Conf.getInstance().getSinkRetryDelayMs()),
                Duration.ofMillis(Conf.getInstance().getSinkRetryMaxDelayMs()),
                2.0)
            .withMaxAttempts(Conf.getInstance().getSinkRetryMaxAttempt())
            .onRetry(
                e -> {
                  LOG.warn(
                      "Retrying attempt {} on exception {}",
                      e.getAttemptCount(),
                      e.getLastFailure());
                  if (CredentialManager.isCredentialsExpired.test(e.getLastFailure())) {
                    // Refresh credential
                    onCredentialsRefreshed();
                  } else {
                    // Reload snapshot
                    onSnapshotReloaded();
                  }
                })
            .build();
    Fallback<Object> fallback =
        Fallback.builder((Object) Optional.empty()).handleIf(isSwallowable).build();
    return Failsafe.with(retryPolicy, fallback).get(body);
  }

  /** Callback invoked when retry need to refresh credentials (credential exception) */
  protected void onCredentialsRefreshed() {
    // Force the recreation of engine (and reload credentials) next time on use.
    this.engine = null;
  }

  /** Callback invoked when retry need to reload snapshot (concurrent exception). */
  protected void onSnapshotReloaded() {
    // Client need to clean up snapshot cache if any
    cacheManager.invalidate(getTablePath().toString());
  }

  static Predicate<Throwable> isTableNotFound =
      ExceptionUtils.recursiveCheck(ex -> ex instanceof TableNotFoundException);

  static Predicate<Throwable> isSnapshotUpdated =
      ExceptionUtils.recursiveCheck(
          ex ->
              ex instanceof ConcurrentModificationException
                  || ex instanceof ConcurrentWriteException
                  || ex instanceof TableAlreadyExistsException);

  static Predicate<Throwable> isSwallowable =
      ExceptionUtils.recursiveCheck(ex -> ex instanceof ConcurrentTransactionException);

  /**
   * Check if an exception is retryable.
   *
   * @param e exception
   * @return true if the exception is Authentication or Concurrency related.
   */
  protected static boolean isRetryableException(Throwable e) {
    return CredentialManager.isCredentialsExpired.test(e) || isSnapshotUpdated.test(e);
  }

  /**
   * Normalizes the given URI string to a canonical form. The normalization includes:
   *
   * <ul>
   *   <li>Ensuring file URIs use the standard triple-slash form (e.g., {@code file:/abc/def} →
   *       {@code file:///abc/def}).
   *   <li>Appending a trailing slash to paths that do not already end with {@code /}.
   * </ul>
   *
   * <p>This method is useful for making URI comparisons consistent and avoiding issues caused by
   * variations in file URI formatting or missing trailing path delimiters.
   *
   * @param input the URI to normalize;
   * @return the normalized URI
   */
  public static URI normalize(URI input) {
    if (input == null) {
      return null;
    }
    URI target = input;
    if (target.getScheme() == null) {
      target = new File(input.toString()).toPath().toUri();
    } else if (target.getScheme().equals("file")) {
      // Normalize "file:/xxx/" to "file:///xxx/"
      target = new File(input).toPath().toUri();
    }
    try {
      // Normalize "abc://def/xxx" to "abc://def/xxx/"
      if (!target.getPath().endsWith("/")) {
        target =
            new URI(
                target.getScheme(),
                Optional.ofNullable(target.getHost()).orElse(""),
                target.getPath() + "/",
                target.getFragment());
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return target;
  }

  protected static List<String> normalize(List<String> rawPartitions) {
    if (rawPartitions == null) {
      return List.of();
    }
    return rawPartitions.stream().filter(StringUtils::isNotEmpty).collect(Collectors.toList());
  }
}
