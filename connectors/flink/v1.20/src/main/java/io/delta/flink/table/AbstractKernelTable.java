package io.delta.flink.table;

import io.delta.flink.DeltaTable;
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

/** An abstract class providing access to Delta table backed by Delta kernel. */
public abstract class AbstractKernelTable implements DeltaTable {

  protected static String ENGINE_INFO = "DeltaSink/Kernel";

  protected URI tablePath;
  protected final Map<String, String> configuration;
  protected String serializableTableState;

  protected transient StructType schema;
  protected transient List<String> partitionColumns;
  protected transient Row tableState;
  protected transient Engine engine;

  public AbstractKernelTable(Map<String, String> conf) {
    this.configuration = new HashMap<>(conf);
  }

  public AbstractKernelTable(
      Map<String, String> conf, StructType schema, List<String> partitionColumns) {
    this.configuration = new HashMap<>(conf);
    this.schema = schema;
    this.partitionColumns = partitionColumns;
  }

  /** Subclass should call this method after init the variables */
  protected void postInit() {
    Objects.requireNonNull(tablePath);
    try {
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
    // It will be used as context for Writer and Committer.
    // The transaction will not be committed. It is discarded afterward.
    Row existingTableState =
        latestSnapshot
            .buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE)
            .build(getEngine())
            .getTransactionState(getEngine());
    this.serializableTableState = JsonUtils.rowToJson(existingTableState);
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
   * Subclass should implement this method to generate an engine.
   *
   * @return engine to access the tables
   */
  protected Engine createEngine() {
    Configuration conf = new Configuration();

    // Built-in configurations that can be overwritten
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    conf.set("fs.s3a.path.style.access", "true");
    conf.set("fs.s3.impl.disable.cache", "true");
    conf.set("fs.s3a.impl.disable.cache", "true");

    this.configuration.forEach(conf::set);
    return DefaultEngine.create(conf);
  }

  protected Row getWriteState() {
    if (tableState == null) {
      tableState = JsonUtils.rowFromJson(serializableTableState, TransactionStateRow.SCHEMA);
    }
    return tableState;
  }

  /**
   * Subclass should implement this method to fetch a Kernel snapshot
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
    if (partitionColumns == null) {
      partitionColumns = TransactionStateRow.getPartitionColumnsList(getWriteState());
    }
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
