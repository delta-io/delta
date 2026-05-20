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

package io.delta.flink.sink.mergestrategy;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.flink.kernel.ColumnVectorUtils;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * Copy-on-write {@link Upsert}: for each candidate file, read it through Kernel, build a selection
 * vector that masks out rows matching the deletion filter, write the survivors to a new Parquet
 * file co-located with the source, and emit a {@code RemoveFile} for the source plus zero-or-more
 * {@code AddFile}s for the rewrite.
 *
 * <p>The {@code RemoveFile} is built via {@link
 * io.delta.kernel.internal.actions.AddFile#toRemoveFileRow}, so it carries over the source's stats,
 * partition values, tags, baseRowId, defaultRowCommitVersion and deletion vector — every field
 * Delta requires for an {@code extendedFileMetadata} RemoveFile.
 */
public class CoWUpsert extends Upsert {

  public CoWUpsert() {
    super(new ScanLocator());
  }

  /**
   * Read the source Parquet, mask out rows matching {@code filter} via a selection vector, stream
   * the survivors through {@link DeltaTable#writeParquet} (co-located with the source), and prepend
   * a {@code RemoveFile} for the source to the resulting {@code AddFile} stream. If every row
   * matches the filter, no {@code AddFile} is emitted — just the {@code RemoveFile}.
   *
   * @throws UncheckedIOException if the Parquet read or write fails
   */
  @Override
  protected CloseableIterator<Row> deleteRecords(
      Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
    Engine engine = table.getEngine();
    final StructType schema = table.getSchema();

    // Source file metadata from the scan file row.
    final FileStatus source = InternalScanFileUtils.getAddFileStatus(addFile);
    final Map<String, String> partStrings = InternalScanFileUtils.getPartitionValues(addFile);
    // addFile has the SCAN_FILE_SCHEMA shape (add, tableRoot), not the SingleAction shape.
    // Use InternalScanFileUtils.ADD_FILE_ORDINAL — the ordinal of "add" within SCAN_FILE_SCHEMA.
    final AddFile sourceAddFile =
        new AddFile(addFile.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));

    // Convert serialized partition values to typed Literals — required by table.writeParquet for
    // the new AddFile.
    final Map<String, Literal> partLiterals = toLiteralPartitionMap(schema, partStrings);

    // Read → filter → write pipeline. The filter is applied lazily, batch-by-batch, via a
    // selection vector so we don't materialize whole rows just to drop a few.
    final CloseableIterator<FilteredColumnarBatch> survivors;
    final CloseableIterator<Row> addActions;
    try {
      survivors =
          engine
              .getParquetHandler()
              .readParquetFiles(singletonCloseableIterator(source), schema, Optional.empty())
              .map(result -> applyDeletionFilter(result.getData(), filter));

      // Co-locate the rewrite with the source file so the new file inherits the same partition
      // directory layout (Delta requires AddFile.partitionValues to match the path-encoded
      // partition; staying in the same folder is the safest way to keep them consistent).
      // ParquetFileWriter generates UUID-suffixed filenames so two rewrites of the same source
      // file can't collide.
      String pathSuffix = sourceAddFile.getPath();
      int slash = sourceAddFile.getPath().lastIndexOf('/');
      if (slash >= 0) {
        pathSuffix = sourceAddFile.getPath().substring(0, slash);
      }
      addActions = table.writeParquet(pathSuffix, survivors, partLiterals);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to rewrite " + sourceAddFile.getPath(), e);
    }

    // RemoveFile for the source file. AddFile.toRemoveFileRow carries over path, partition
    // values, size, stats, tags, baseRowId, defaultRowCommitVersion and deletionVector — i.e.
    // every field Delta requires for an "extendedFileMetadata" RemoveFile.
    final Row removeAction =
        SingleAction.createRemoveFileSingleAction(
            sourceAddFile.toRemoveFileRow(true /* dataChange */, Optional.empty()));

    // Emit RemoveFile first, then the AddFile(s). Delta commits don't care about action order
    // within the same commit, but emitting Remove-before-Add reads more naturally.
    return singletonCloseableIterator(removeAction).combine(addActions);
  }

  /**
   * Wrap {@code batch} with a selection vector that excludes rows matching {@code filter}.
   *
   * <p>{@link FilteredColumnarBatch}'s selection vector follows the SQL convention <em>"true =
   * keep, false = drop"</em>. Our caller's contract is the opposite — {@code filter.test(...)}
   * returns {@code true} for rows to be <em>deleted</em> — so we negate when building the selection
   * vector. The underlying batch's column vectors are reused unmodified; no per-row materialization
   * on the rewrite path.
   */
  private static FilteredColumnarBatch applyDeletionFilter(
      ColumnarBatch batch, BiPredicate<ColumnarBatch, Integer> filter) {
    return new FilteredColumnarBatch(
        batch, ColumnVectorUtils.filter(batch.getSize(), rowId -> !filter.test(batch, rowId)));
  }

  /**
   * Convert the partition string map from a scan-file row into a typed-Literal map keyed by the
   * partition column names — the form expected by {@link DeltaTable#writeParquet}.
   */
  private static Map<String, Literal> toLiteralPartitionMap(
      StructType schema, Map<String, String> partitionStrings) {
    final Map<String, Literal> out = new HashMap<>(partitionStrings.size());
    for (Map.Entry<String, String> e : partitionStrings.entrySet()) {
      final int idx = schema.indexOf(e.getKey());
      if (idx < 0) {
        throw new IllegalStateException(
            "Partition column " + e.getKey() + " not found in table schema " + schema);
      }
      final DataType type = schema.fields().get(idx).getDataType();
      out.put(e.getKey(), parsePartitionLiteral(type, e.getValue()));
    }
    return out;
  }

  /**
   * Parse the Delta-serialized string form of a partition value into a typed {@link Literal}.
   *
   * <p>Mirrors {@code io.delta.kernel.internal.util.PartitionUtils#literalForPartitionValue}, which
   * is package-private and not callable from here.
   *
   * <p>Timestamp, decimal, and binary partition values aren't yet covered — they require Delta's
   * specific string-to-microseconds / string-to-BigDecimal parsing logic; add support for them when
   * a real workload demands them.
   */
  private static Literal parsePartitionLiteral(DataType type, String value) {
    if (value == null) {
      return Literal.ofNull(type);
    }
    if (type instanceof BooleanType) return Literal.ofBoolean(Boolean.parseBoolean(value));
    if (type instanceof ByteType) return Literal.ofByte(Byte.parseByte(value));
    if (type instanceof ShortType) return Literal.ofShort(Short.parseShort(value));
    if (type instanceof IntegerType) return Literal.ofInt(Integer.parseInt(value));
    if (type instanceof LongType) return Literal.ofLong(Long.parseLong(value));
    if (type instanceof FloatType) return Literal.ofFloat(Float.parseFloat(value));
    if (type instanceof DoubleType) return Literal.ofDouble(Double.parseDouble(value));
    if (type instanceof StringType) return Literal.ofString(value);
    if (type instanceof DateType) {
      return Literal.ofDate((int) LocalDate.parse(value).toEpochDay());
    }
    throw new UnsupportedOperationException(
        "Unsupported partition column type for upsert rewrite: " + type);
  }
}
