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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.DataWriteContext;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DecimalType;

/**
 * Per-task writer that buffers Spark {@link InternalRow}s, converts them to Kernel format, runs
 * transformLogicalData → writeParquetFiles → generateAppendActions, and returns a commit message
 * with the serialized Delta log actions.
 *
 * <p>Uses table schema (not LogicalWriteInfo schema) for Kernel so nullability matches the table;
 * the query schema can have different nullability (e.g. non-null id) and Kernel requires exact
 * match. Column order is assumed to match between InternalRow and table schema.
 */
public class DeltaKernelDataWriter implements DataWriter<InternalRow> {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  /** Table schema; used for Kernel and adapter so nullability matches table. */
  private final org.apache.spark.sql.types.StructType tableSchema;

  private final List<String> partitionColumnNames;
  private final Map<String, String> options;
  private final int partitionId;
  private final long taskId;
  private final int[] partitionColumnOrdinals;

  private final List<InternalRow> rowBuffer = new ArrayList<>();
  private final StructType kernelSchema;

  public DeltaKernelDataWriter(
      String tablePath,
      Configuration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      org.apache.spark.sql.types.StructType tableSchema,
      List<String> partitionColumnNames,
      Map<String, String> options,
      int partitionId,
      long taskId) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.tableSchema = tableSchema;
    List<String> requestedPartitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : Collections.emptyList();
    if (requestedPartitionColumnNames.isEmpty()) {
      requestedPartitionColumnNames =
          TransactionStateRow.getPartitionColumnsList(serializedTxnState.getRow());
    }
    this.partitionColumnNames = requestedPartitionColumnNames;
    this.options = options != null ? options : Collections.emptyMap();
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(tableSchema);
    this.partitionColumnOrdinals =
        resolvePartitionColumnOrdinals(tableSchema, this.partitionColumnNames);
  }

  @Override
  public void write(InternalRow record) {
    rowBuffer.add(projectDataColumns(record).copy());
  }

  /**
   * Projects the InternalRow to only include data columns matching the table schema. In COW
   * operations, Spark may prepend metadata columns (e.g. file_path) to the row, causing a mismatch
   * between record.numFields() and tableSchema.length(). This method strips the leading metadata
   * columns by computing an offset.
   */
  private InternalRow projectDataColumns(InternalRow record) {
    int numDataCols = tableSchema.length();
    if (record.numFields() == numDataCols) {
      return record;
    }
    int offset = record.numFields() - numDataCols;
    org.apache.spark.sql.catalyst.expressions.GenericInternalRow projected =
        new org.apache.spark.sql.catalyst.expressions.GenericInternalRow(numDataCols);
    for (int i = 0; i < numDataCols; i++) {
      projected.update(i, record.get(i + offset, tableSchema.apply(i).dataType()));
    }
    return projected;
  }

  @Override
  public WriterCommitMessage commit() {
    if (rowBuffer.isEmpty()) {
      return new DeltaKernelWriterCommitMessage(Collections.emptyList());
    }

    Engine engine = DefaultEngine.create(hadoopConf);
    Row txnState = serializedTxnState.getRow();
    List<SerializableKernelRowWrapper> serializedActions = new ArrayList<>();

    if (partitionColumnNames.isEmpty()) {
      serializedActions.addAll(writePartition(engine, txnState, rowBuffer, Collections.emptyMap()));
      return new DeltaKernelWriterCommitMessage(serializedActions);
    }

    Map<PartitionKey, PartitionWriteGroup> partitionGroups = new LinkedHashMap<>();
    for (InternalRow internalRow : rowBuffer) {
      Map<String, Literal> partitionValues = buildPartitionValues(internalRow);
      PartitionKey partitionKey = new PartitionKey(extractPartitionKeyValues(internalRow));
      partitionGroups
          .computeIfAbsent(partitionKey, ignored -> new PartitionWriteGroup(partitionValues))
          .rows
          .add(internalRow);
    }

    for (PartitionWriteGroup partitionGroup : partitionGroups.values()) {
      serializedActions.addAll(
          writePartition(engine, txnState, partitionGroup.rows, partitionGroup.partitionValues));
    }

    return new DeltaKernelWriterCommitMessage(serializedActions);
  }

  private List<SerializableKernelRowWrapper> writePartition(
      Engine engine,
      Row txnState,
      List<InternalRow> partitionRows,
      Map<String, Literal> partitionValues) {
    List<Row> kernelRows = new ArrayList<>(partitionRows.size());
    for (InternalRow internalRow : partitionRows) {
      kernelRows.add(new InternalRowToKernelRowAdapter(internalRow, tableSchema, kernelSchema));
    }

    ColumnarBatch batch = new DefaultRowBasedColumnarBatch(kernelSchema, kernelRows);
    FilteredColumnarBatch filteredBatch =
        new FilteredColumnarBatch(batch, java.util.Optional.empty());

    CloseableIterator<FilteredColumnarBatch> dataIter =
        Utils.toCloseableIterator(Collections.singletonList(filteredBatch).iterator());

    CloseableIterator<FilteredColumnarBatch> physicalData =
        Transaction.transformLogicalData(engine, txnState, dataIter, partitionValues);

    DataWriteContext writeContext = Transaction.getWriteContext(engine, txnState, partitionValues);

    CloseableIterator<DataFileStatus> dataFiles;
    try {
      dataFiles =
          engine
              .getParquetHandler()
              .writeParquetFiles(
                  writeContext.getTargetDirectory(),
                  physicalData,
                  writeContext.getStatisticsColumns());
    } catch (IOException e) {
      throw new RuntimeException("Failed to write Parquet files", e);
    }

    CloseableIterator<Row> actionRowsIter =
        Transaction.generateAppendActions(engine, txnState, dataFiles, writeContext);

    List<SerializableKernelRowWrapper> serializedActions = new ArrayList<>();
    try {
      while (actionRowsIter.hasNext()) {
        serializedActions.add(new SerializableKernelRowWrapper(actionRowsIter.next()));
      }
    } finally {
      try {
        actionRowsIter.close();
      } catch (Exception ignored) {
        // best effort
      }
      try {
        dataFiles.close();
      } catch (Exception ignored) {
        // best effort
      }
      try {
        physicalData.close();
      } catch (Exception ignored) {
        // best effort
      }
    }

    return serializedActions;
  }

  private Map<String, Literal> buildPartitionValues(InternalRow row) {
    Map<String, Literal> partitionValues = new LinkedHashMap<>();
    for (int i = 0; i < partitionColumnNames.size(); i++) {
      int ordinal = partitionColumnOrdinals[i];
      partitionValues.put(
          partitionColumnNames.get(i),
          toPartitionLiteral(row, ordinal, tableSchema.fields()[ordinal].dataType()));
    }
    return partitionValues;
  }

  private Object[] extractPartitionKeyValues(InternalRow row) {
    Object[] keyValues = new Object[partitionColumnNames.size()];
    for (int i = 0; i < partitionColumnNames.size(); i++) {
      int ordinal = partitionColumnOrdinals[i];
      keyValues[i] =
          extractPartitionKeyValue(row, ordinal, tableSchema.fields()[ordinal].dataType());
    }
    return keyValues;
  }

  private Literal toPartitionLiteral(
      InternalRow row, int ordinal, org.apache.spark.sql.types.DataType sparkDataType) {
    io.delta.kernel.types.DataType kernelDataType = kernelSchema.at(ordinal).getDataType();
    if (row.isNullAt(ordinal)) {
      return Literal.ofNull(kernelDataType);
    }

    if (sparkDataType instanceof org.apache.spark.sql.types.BooleanType) {
      return Literal.ofBoolean(row.getBoolean(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ByteType) {
      return Literal.ofByte(row.getByte(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ShortType) {
      return Literal.ofShort(row.getShort(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.IntegerType) {
      return Literal.ofInt(row.getInt(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.LongType) {
      return Literal.ofLong(row.getLong(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.FloatType) {
      return Literal.ofFloat(row.getFloat(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DoubleType) {
      return Literal.ofDouble(row.getDouble(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.StringType) {
      return Literal.ofString(row.getUTF8String(ordinal).toString());
    } else if (sparkDataType instanceof org.apache.spark.sql.types.BinaryType) {
      return Literal.ofBinary(row.getBinary(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DateType) {
      return Literal.ofDate(row.getInt(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampType) {
      return Literal.ofTimestamp(row.getLong(ordinal));
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampNTZType) {
      return Literal.ofTimestampNtz(row.getLong(ordinal));
    } else if (sparkDataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkDataType;
      return Literal.ofDecimal(
          row.getDecimal(ordinal, decimalType.precision(), decimalType.scale()).toJavaBigDecimal(),
          decimalType.precision(),
          decimalType.scale());
    }

    throw new UnsupportedOperationException(
        "Unsupported partition column type in DSv2 write path: " + sparkDataType);
  }

  private Object extractPartitionKeyValue(
      InternalRow row, int ordinal, org.apache.spark.sql.types.DataType sparkDataType) {
    if (row.isNullAt(ordinal)) {
      return null;
    }

    if (sparkDataType instanceof org.apache.spark.sql.types.BooleanType) {
      return row.getBoolean(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ByteType) {
      return row.getByte(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.ShortType) {
      return row.getShort(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.IntegerType) {
      return row.getInt(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.LongType) {
      return row.getLong(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.FloatType) {
      return row.getFloat(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DoubleType) {
      return row.getDouble(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.StringType) {
      return row.getUTF8String(ordinal).toString();
    } else if (sparkDataType instanceof org.apache.spark.sql.types.BinaryType) {
      byte[] bytes = row.getBinary(ordinal);
      return bytes == null ? null : bytes.clone();
    } else if (sparkDataType instanceof org.apache.spark.sql.types.DateType) {
      return row.getInt(ordinal);
    } else if (sparkDataType instanceof org.apache.spark.sql.types.TimestampType
        || sparkDataType instanceof org.apache.spark.sql.types.TimestampNTZType) {
      return row.getLong(ordinal);
    } else if (sparkDataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) sparkDataType;
      return row.getDecimal(ordinal, decimalType.precision(), decimalType.scale())
          .toJavaBigDecimal();
    }

    throw new UnsupportedOperationException(
        "Unsupported partition column type in DSv2 write path: " + sparkDataType);
  }

  private static int[] resolvePartitionColumnOrdinals(
      org.apache.spark.sql.types.StructType tableSchema, List<String> partitionColumnNames) {
    int[] ordinals = new int[partitionColumnNames.size()];
    for (int i = 0; i < partitionColumnNames.size(); i++) {
      ordinals[i] = resolvePartitionColumnOrdinal(tableSchema, partitionColumnNames.get(i));
    }
    return ordinals;
  }

  private static int resolvePartitionColumnOrdinal(
      org.apache.spark.sql.types.StructType tableSchema, String partitionColumnName) {
    org.apache.spark.sql.types.StructField[] fields = tableSchema.fields();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().equalsIgnoreCase(partitionColumnName)) {
        return i;
      }
    }

    throw new IllegalArgumentException(
        "Partition column not found in table schema: " + partitionColumnName);
  }

  private static final class PartitionWriteGroup {
    private final Map<String, Literal> partitionValues;
    private final List<InternalRow> rows = new ArrayList<>();

    private PartitionWriteGroup(Map<String, Literal> partitionValues) {
      this.partitionValues = partitionValues;
    }
  }

  private static final class PartitionKey {
    private final Object[] values;

    private PartitionKey(Object[] values) {
      this.values = values;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof PartitionKey)) {
        return false;
      }
      PartitionKey that = (PartitionKey) other;
      return Arrays.deepEquals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(values);
    }
  }

  @Override
  public void abort() {
    // No Kernel commit; buffer is discarded.
  }

  @Override
  public void close() {
    // No-op; resources are released in commit() or abort().
  }
}
