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

package io.delta.flink;

import dev.failsafe.function.CheckedConsumer;
import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.transaction.DataLayoutSpec;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;
import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils;

/**
 * Abstract base class for JUnit 6 (Jupiter) tests providing common test utilities for Delta Lake
 * operations.
 */
public abstract class TestHelper {

  protected final Random random = new Random(System.currentTimeMillis());

  protected static <T> Consumer<T> wrap(CheckedConsumer<T> body) {
    return t -> {
      try {
        body.accept(t);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Executes the given function with a temporary directory. The directory is automatically deleted
   * after the function completes.
   *
   * @param f Consumer function that receives the temporary directory
   */
  protected void withTempDir(CheckedConsumer<File> f) {
    File tempDir = null;
    try {
      tempDir = Files.createTempDirectory(UUID.randomUUID().toString()).toFile();
      f.accept(tempDir);
    } catch (Throwable e) {
      throw new RuntimeException(e);
    } finally {
      if (tempDir != null) {
        FileUtils.deleteQuietly(tempDir);
      }
    }
  }

  /**
   * Creates a dummy row with a random ID.
   *
   * @return A Row containing a random integer ID
   */
  protected Row dummyRow() {
    int id = random.nextInt(1048576);
    Map<Integer, Object> map = new HashMap<>();
    map.put(0, id);
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    return new GenericRow(schema, map);
  }

  /**
   * Creates dummy file statistics with the specified number of records.
   *
   * @param numRecords Number of records in the file
   * @return DataFileStatistics with the given record count
   */
  protected DataFileStatistics dummyStatistics(long numRecords) {
    return new DataFileStatistics(
        numRecords,
        Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.emptyMap(),
        Optional.empty());
  }

  /**
   * Creates a dummy AddFile row for testing.
   *
   * @param schema Schema of the table
   * @param numRows Number of rows in the file
   * @param partitionValues Partition column values
   * @return Row representing an AddFile action
   */
  protected Row dummyAddFileRow(
      StructType schema, long numRows, Map<String, Literal> partitionValues) {
    AddFile addFile =
        AddFile.convertDataFileStatus(
            schema,
            URI.create("s3://abc/def"),
            new DataFileStatus(
                "s3://abc/def/" + UUID.randomUUID().toString(),
                1000L,
                2000L,
                Optional.of(dummyStatistics(numRows))),
            partitionValues,
            /* dataChange= */ true,
            /* tags= */ Collections.emptyMap(),
            /* baseRowId= */ Optional.empty(),
            /* defaultRowCommitVersion= */ Optional.empty(),
            /* deletionVectorDescriptor= */ Optional.empty());
    return SingleAction.createAddFileSingleAction(addFile.toRow());
  }

  /**
   * Creates a dummy writer context for testing.
   *
   * @param engine Delta Engine instance
   * @param tablePath Path to the table
   * @param schema Table schema
   * @param partitionCols Partition column names
   * @return Transaction state row
   */
  protected Row dummyWriterContext(
      Engine engine, String tablePath, StructType schema, List<String> partitionCols) {
    var txnBuilder = TableManager.buildCreateTableTransaction(tablePath, schema, "dummy");
    if (!partitionCols.isEmpty()) {
      List<Column> partitionColumns =
          partitionCols.stream().map(Column::new).collect(Collectors.toList());
      txnBuilder.withDataLayoutSpec(DataLayoutSpec.partitioned(partitionColumns));
    }
    var txn = txnBuilder.build(engine);
    return txn.getTransactionState(engine);
  }

  /** Overload with empty partition columns. */
  protected Row dummyWriterContext(Engine engine, String tablePath, StructType schema) {
    return dummyWriterContext(engine, tablePath, schema, Collections.emptyList());
  }

  /**
   * Creates a non-empty table with dummy data.
   *
   * @param engine Delta Engine instance
   * @param tablePath Path to the table
   * @param schema Table schema
   * @param partitionCols Partition column names
   * @param numRows Number of rows to add
   * @param properties Table properties
   * @return Optional snapshot after creation
   */
  protected Optional<Snapshot> createNonEmptyTable(
      Engine engine,
      String tablePath,
      StructType schema,
      List<String> partitionCols,
      long numRows,
      Map<String, String> properties) {
    var txnBuilder =
        TableManager.buildCreateTableTransaction(tablePath, schema, "dummy")
            .withTableProperties(properties);
    if (!partitionCols.isEmpty()) {
      List<Column> partitionColumns =
          partitionCols.stream().map(Column::new).collect(Collectors.toList());
      txnBuilder.withDataLayoutSpec(DataLayoutSpec.partitioned(partitionColumns));
    }
    var txn = txnBuilder.build(engine);

    Map<String, Literal> partitionMap = new HashMap<>();
    for (String colName : partitionCols) {
      partitionMap.put(colName, dummyRandomLiteral(schema.get(colName).getDataType()));
    }

    // Prepare some dummy AddFile
    AddFile dummyAddFile =
        AddFile.convertDataFileStatus(
            schema,
            URI.create(tablePath),
            new DataFileStatus(
                UUID.randomUUID().toString(), 1000L, 2000L, Optional.of(dummyStatistics(numRows))),
            partitionMap,
            true,
            Collections.emptyMap(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    return txn.commit(
            engine,
            CloseableIterable.inMemoryIterable(
                Utils.singletonCloseableIterator(
                    SingleAction.createAddFileSingleAction(dummyAddFile.toRow()))))
        .getPostCommitSnapshot();
  }

  /** Overloads with default values. */
  protected Optional<Snapshot> createNonEmptyTable(
      Engine engine, String tablePath, StructType schema) {
    return createNonEmptyTable(
        engine, tablePath, schema, Collections.emptyList(), 0L, Collections.emptyMap());
  }

  protected Optional<Snapshot> createNonEmptyTable(
      Engine engine, String tablePath, StructType schema, List<String> partitionCols) {
    return createNonEmptyTable(
        engine, tablePath, schema, partitionCols, 0L, Collections.emptyMap());
  }

  protected Optional<Snapshot> createNonEmptyTable(
      Engine engine,
      String tablePath,
      StructType schema,
      List<String> partitionCols,
      long numRows) {
    return createNonEmptyTable(
        engine, tablePath, schema, partitionCols, numRows, Collections.emptyMap());
  }

  protected Optional<Snapshot> createNonEmptyTable(
      Engine engine,
      String tablePath,
      StructType schema,
      List<String> partitionCols,
      Map<String, String> properties) {
    return createNonEmptyTable(engine, tablePath, schema, partitionCols, 0L, properties);
  }

  /**
   * Makes a random write to an existing table.
   *
   * @param engine Delta Engine instance
   * @param tablePath Path to the table
   * @param schema Table schema
   * @param partitionCols Partition column names
   * @return Optional snapshot after write
   */
  protected Optional<Snapshot> writeTable(
      Engine engine, String tablePath, StructType schema, List<String> partitionCols) {
    Map<String, Literal> partitionMap = new HashMap<>();
    for (String colName : partitionCols) {
      partitionMap.put(colName, dummyRandomLiteral(schema.get(colName).getDataType()));
    }

    // Prepare some dummy AddFile
    AddFile dummyAddFile =
        AddFile.convertDataFileStatus(
            schema,
            URI.create(tablePath),
            new DataFileStatus(UUID.randomUUID().toString(), 1000L, 2000L, Optional.empty()),
            partitionMap,
            true,
            Collections.emptyMap(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    var txn =
        TableManager.loadSnapshot(tablePath)
            .build(engine)
            .buildUpdateTableTransaction("dummy", Operation.WRITE)
            .build(engine);

    return txn.commit(
            engine,
            CloseableIterable.inMemoryIterable(
                Utils.singletonCloseableIterator(
                    SingleAction.createAddFileSingleAction(dummyAddFile.toRow()))))
        .getPostCommitSnapshot();
  }

  /** Overload with empty partition columns. */
  protected Optional<Snapshot> writeTable(Engine engine, String tablePath, StructType schema) {
    return writeTable(engine, tablePath, schema, Collections.emptyList());
  }

  /**
   * Verifies table content using a custom checker function.
   *
   * @param tablePath Path to the table
   * @param checker Consumer that receives version, AddFiles, and properties
   */
  protected void verifyTableContent(String tablePath, TableContentChecker checker) {
    Engine engine = DefaultEngine.create(new Configuration());
    Snapshot snapshot = TableManager.loadSnapshot(tablePath).build(engine);
    var filesList =
        ((ScanImpl) snapshot.getScanBuilder().build()).getScanFiles(engine, true).toInMemoryList();

    List<AddFile> actions =
        StreamSupport.stream(filesList.spliterator(), false)
            .flatMap(
                scanFile ->
                    StreamSupport.stream(scanFile.getRows().toInMemoryList().spliterator(), false))
            .map(row -> new AddFile(row.getStruct(0)))
            .collect(Collectors.toList());

    Map<String, String> properties = snapshot.getTableProperties();
    checker.check(snapshot.getVersion(), actions, properties);
  }

  /** Functional interface for table content verification. */
  @FunctionalInterface
  protected interface TableContentChecker {
    void check(long version, Iterable<AddFile> addFiles, Map<String, String> properties);
  }

  /**
   * Reads a Parquet file and returns its rows.
   *
   * @param filePath Path to the Parquet file
   * @param schema Expected schema
   * @return List of rows from the file
   */
  protected List<Row> readParquet(Path filePath, StructType schema) {
    try {
      FileStatus fileStatus =
          FileStatus.of(
              filePath.toString(),
              Files.size(filePath),
              Files.getLastModifiedTime(filePath).toMillis());

      var results =
          DefaultEngine.create(new Configuration())
              .getParquetHandler()
              .readParquetFiles(
                  Utils.singletonCloseableIterator(fileStatus), schema, Optional.empty());

      return StreamSupport.stream(results.toInMemoryList().spliterator(), false)
          .flatMap(
              result ->
                  StreamSupport.stream(
                      result.getData().getRows().toInMemoryList().spliterator(), false))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read Parquet file: " + filePath, e);
    }
  }

  /**
   * Creates a random literal value for the given data type.
   *
   * @param dataType Data type for the literal
   * @return Random literal of the specified type
   */
  protected Literal dummyRandomLiteral(DataType dataType) {
    if (dataType.equals(IntegerType.INTEGER)) {
      return Literal.ofInt(random.nextInt());
    } else if (dataType.equals(StringType.STRING)) {
      return Literal.ofString("p" + random.nextInt());
    } else if (dataType.equals(LongType.LONG)) {
      return Literal.ofLong(random.nextLong());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported data type for random literal: " + dataType);
    }
  }

  /**
   * Checks if an object is serializable by attempting to serialize and deserialize it.
   *
   * @param input Object to check for serializability
   * @throws AssertionError if the object cannot be serialized or deserialized correctly
   */
  protected void checkSerializability(Object input) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(input);
      oos.close();

      byte[] bytes = baos.toByteArray();
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      Object restored = ois.readObject();
      ois.close();

      if (!restored.getClass().equals(input.getClass())) {
        throw new AssertionError(
            "Restored object class "
                + restored.getClass()
                + " does not match input class "
                + input.getClass());
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new AssertionError("Object is not serializable", e);
    }
  }
}
