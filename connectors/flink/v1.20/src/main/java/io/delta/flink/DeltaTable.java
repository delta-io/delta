package io.delta.flink;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This is an interface to access the Delta table. It contains table info such as table schema,
 * partition columns, and also implements methods to read and write table content.
 */
public interface DeltaTable extends Serializable {

  /** An id that can uniquely identify the table. */
  String getId();

  StructType getSchema();

  List<String> getPartitionColumns();

  /**
   * Commit a new version to the table.
   *
   * @param actions actions to be committed
   */
  void commit(CloseableIterable<Row> actions);

  /**
   * Write parquet files and create an associated addfile action.
   *
   * @param pathSuffix string to be appended to the table path
   * @param data row data to be written
   * @param partitionValues partition values
   * @return a row representing the written action
   */
  CloseableIterator<Row> writeParquet(
      String pathSuffix,
      CloseableIterator<FilteredColumnarBatch> data,
      Map<String, Literal> partitionValues)
      throws IOException;
}
