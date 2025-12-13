package io.delta.flink.table;

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;

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
 * them into physical locations or catalog entries. See
 * also @link{io.delta.flink.table.DeltaCatalog}
 */
public abstract class AbstractKernelTable implements DeltaTable {
  protected static String ENGINE_INFO = "DeltaSink/Kernel";

  protected final Catalog catalog;
  protected String tableId;
  protected String uuid;
  protected URI tablePath;
  protected final Map<String, String> configuration;
  protected String serializableTableState;
  protected List<String> partitionColumns;

  // These fields are not serializable. They need to be recreated
  // after the table are serialized. Always access them using getters.
  protected transient StructType schema;
  protected transient Row tableState;
  protected transient Engine engine;

  public AbstractKernelTable(
      Catalog catalog,
      String tableId,
      Map<String, String> conf,
      StructType schema,
      List<String> partitionColumns) {
    this.catalog = catalog;
    this.tableId = tableId;
    this.configuration = conf;
    this.schema = schema;
    this.partitionColumns = partitionColumns;
    initialize();
  }

  public AbstractKernelTable(Catalog catalog, String tableId, Map<String, String> conf) {
    this(catalog, tableId, conf, null, null);
  }

  protected void initialize() {
    try {
      Catalog.TableBrief info = catalog.getTable(tableId);
      uuid = info.uuid;
      tablePath = URI.create(info.tablePath);
      loadExistingTable();
    } catch (TableNotFoundException e) {
      Preconditions.checkArgument(schema != null);
      initNewTable(schema, partitionColumns);
    }
  }

  protected void loadExistingTable() {
    // With an existing table, partitions loaded from the table take precedence
    final Snapshot latestSnapshot = loadLatestSnapshot();
    // We use a temporary transaction to generate a TransactionStateRow.
    // It serves as a holder for schema and partition columns.
    // The transaction will not be committed, and is discarded afterward.
    Row existingTableState =
        latestSnapshot
            .buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE)
            .build(getEngine())
            .getTransactionState(getEngine());
    this.serializableTableState = JsonUtils.rowToJson(existingTableState);
    this.schema = TransactionStateRow.getLogicalSchema(existingTableState);
    this.partitionColumns = TransactionStateRow.getPartitionColumnsList(existingTableState);
  }

  protected void initNewTable(StructType schema, List<String> partitionColumns) {
    Row newTableState =
        TableManager.buildCreateTableTransaction(getTablePath().toString(), schema, ENGINE_INFO)
            .withDataLayoutSpec(
                DataLayoutSpec.partitioned(
                    Optional.of(partitionColumns)
                        .map(
                            nonEmpty ->
                                nonEmpty.stream().map(Column::new).collect(Collectors.toList()))
                        .orElseGet(Collections::emptyList)))
            .build(getEngine())
            .getTransactionState(getEngine());
    this.serializableTableState = JsonUtils.rowToJson(newTableState);
  }

  protected Engine getEngine() {
    if (engine == null) {
      engine = createEngine();
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
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3.impl.disable.cache", "true");
    conf.set("fs.s3a.impl.disable.cache", "true");

    conf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
    conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
    conf.set("fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
    conf.set("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");

    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    this.configuration.forEach(conf::set);
    this.catalog.getCredentials(uuid).forEach(conf::set);

    return DefaultEngine.create(conf);
  }

  public Catalog getCatalog() {
    return catalog;
  }

  @Override
  public String getId() {
    return tableId;
  }

  public String getUuid() {
    return uuid;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  protected Row getWriteState() {
    if (tableState == null) {
      tableState = JsonUtils.rowFromJson(serializableTableState, TransactionStateRow.SCHEMA);
    }
    return tableState;
  }

  /**
   * Subclass must implement this method to fetch a Kernel snapshot
   *
   * @return latest snapshot of the table
   */
  protected abstract Snapshot loadLatestSnapshot();

  /** The table storage location where all data and metadata files should be stored. */
  public URI getTablePath() {
    return tablePath;
  }

  @Override
  public StructType getSchema() {
    if (schema == null) {
      schema = TransactionStateRow.getLogicalSchema(getWriteState());
    }
    return schema;
  }

  @Override
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @Override
  public void commit(CloseableIterable<Row> actions) {
    Engine localEngine = getEngine();
    Transaction txn;
    try {
      Snapshot snapshot = loadLatestSnapshot();
      txn = snapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE).build(engine);
      // We check the table's latest schema is still the same as committer schema.
      // The check is delayed here to detect external modification to the table schema.
      // TODO remove this after kernel support Column Mapping
      final StructType tableSchema = txn.getSchema(engine);
      final StructType committingSchema = getSchema();
      Preconditions.checkArgument(
          committingSchema.equivalent(tableSchema),
          String.format(
              "DeltaSink does not support schema evolution. "
                  + "Table schema: %s, Committer schema: %s",
              tableSchema, committingSchema));
    } catch (TableNotFoundException e) {
      CreateTableTransactionBuilder txnBuilder =
          TableManager.buildCreateTableTransaction(
              getTablePath().toString(), getSchema(), ENGINE_INFO);
      if (!getPartitionColumns().isEmpty()) {
        txnBuilder.withDataLayoutSpec(
            DataLayoutSpec.partitioned(
                getPartitionColumns().stream().map(Column::new).collect(Collectors.toList())));
      }
      txn = txnBuilder.build(localEngine);
    }
    txn.commit(localEngine, actions);
  }

  @Override
  public CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues)
      throws IOException {
    Engine localEngine = getEngine();
    Row writeState = getWriteState();

    final CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(localEngine, writeState, data, partitionValues);

    final DataWriteContext writeContext =
        Transaction.getWriteContext(localEngine, writeState, partitionValues);
    final CloseableIterator<DataFileStatus> dataFiles =
        localEngine
            .getParquetHandler()
            .writeParquetFiles(
                getTablePath().resolve(pathSuffix).toString(),
                physicalData,
                writeContext.getStatisticsColumns());
    return Transaction.generateAppendActions(localEngine, writeState, dataFiles, writeContext);
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
    if (input.getScheme() == null) {
      return new File(input.toString()).toPath().toUri();
    }
    // Normalize "file:/xxx/" to "file:///xxx/"
    if (input.getScheme().equals("file")) {
      return new File(input).toPath().toUri();
    }
    try {
      // Normalize "abc://def/xxx" to "abc://def/xxx/"
      if (!input.getPath().endsWith("/")) {
        return new URI(
            input.getScheme(), input.getHost(), input.getPath() + "/", input.getFragment());
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return input;
  }
}
