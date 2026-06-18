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
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.flink.kernel.dv.BinDVAccess;
import io.delta.flink.kernel.dv.DVAccess;
import io.delta.flink.kernel.dv.RoaringBitmapArray;
import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.DeltaTable;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Merge-on-read {@link Upsert}: for each candidate file, walk its row positions and add every
 * filter-matched row index into a {@link RoaringBitmapArray}, merged with the file's existing
 * deletion vector (if any). Write the resulting bitmap to a new {@code deletion_vector_<UUID>.bin}
 * under the table root and emit a {@link AddFile}/{@code RemoveFile} pair that points at the same
 * data file but carries the new {@link DeletionVectorDescriptor}. The Parquet itself is untouched.
 *
 * <p>Compatible with Spark's {@code DeletionVectorStore} on-disk layout (see {@link BinDVAccess}).
 */
public class MoRUpsert extends Upsert {

  private final DVAccess dvAccess;

  public MoRUpsert() {
    super(new ScanLocator());
    this.dvAccess = new BinDVAccess();
  }

  @Override
  protected CloseableIterator<Row> deleteRecords(
      Row addFile, BiPredicate<ColumnarBatch, Integer> filter) {
    final Engine engine = table.getEngine();
    final StructType schema = table.getSchema();
    final FileStatus source = InternalScanFileUtils.getAddFileStatus(addFile);
    final AddFile sourceAddFile =
        new AddFile(addFile.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL));

    // 1. Start from the file's existing DV (if any) so this is an additive update, not a
    //    replacement.
    final RoaringBitmapArray bitmap = loadExistingDv(engine, sourceAddFile.getDeletionVector());
    final long existingCardinality = bitmap.cardinality();

    // 2. Walk the source Parquet and mark every filter-matched row position.
    try (CloseableIterator<ColumnarBatch> data =
        engine
            .getParquetHandler()
            .readParquetFiles(singletonCloseableIterator(source), schema, Optional.empty())
            .map(FileReadResult::getData)) {
      long rowIdx = 0L;
      while (data.hasNext()) {
        ColumnarBatch batch = data.next();
        int batchSize = batch.getSize();
        for (int i = 0; i < batchSize; i++) {
          if (filter.test(batch, i)) {
            bitmap.add(rowIdx + i);
          }
        }
        rowIdx += batchSize;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read source file " + source.getPath(), e);
    }

    final long newCardinality = bitmap.cardinality();
    if (newCardinality == existingCardinality) {
      // No new rows matched; the existing DV (if any) is still authoritative -- leave the file
      // alone. We can hit this when the locator over-reports candidate files (e.g. a stats-pruned
      // scan that surfaces a file containing no actual PK match).
      return toCloseableIterator(Collections.emptyIterator());
    }

    // 3. Hand the bitmap to DVAccess; it picks the file name, in-file offset and encoding for us.
    final DeletionVectorDescriptor newDv =
        dvAccess.store(engine, table.getTablePath().toString(), bitmap);

    // 4. Emit RemoveFile(source) + AddFile(same data file path, new DV). The data file is reused
    //    as-is; only the DV pointer changes.
    final Row removeAction =
        SingleAction.createRemoveFileSingleAction(
            sourceAddFile.toRemoveFileRow(true /* dataChange */, Optional.empty()));
    final Row addAction =
        SingleAction.createAddFileSingleAction(addFileRowWithNewDv(sourceAddFile, newDv));
    return toCloseableIterator(Arrays.asList(removeAction, addAction).iterator());
  }

  @Override
  public Row markRemovesOnStaged(DeltaTable table, Row stagedAddFile, Set<Integer> positions) {
    AbstractKernelTable kernelTable = (AbstractKernelTable) table;
    String tableRoot = kernelTable.getTablePath().toString();
    RoaringBitmapArray bitmap = new RoaringBitmapArray();
    bitmap.addAll(positions);
    DeletionVectorDescriptor dvDesc = dvAccess.store(kernelTable.getEngine(), tableRoot, bitmap);
    AddFile oldAddFile = new AddFile(stagedAddFile.getStruct(SingleAction.ADD_FILE_ORDINAL));
    Row newAddFileRow = addFileRowWithNewDv(oldAddFile, dvDesc);
    return SingleAction.createAddFileSingleAction(newAddFileRow);
  }

  /**
   * Load the existing deletion vector attached to {@code dvOpt}, or return an empty bitmap if the
   * file has no DV. Both UUID ({@code "u"}) and path ({@code "p"}) storage are supported; inline
   * DVs ({@code "i"}) are intentionally rejected -- new writes go through {@link DVAccess#store},
   * which produces path-based descriptors, and we don't generate inline DVs anywhere.
   */
  private RoaringBitmapArray loadExistingDv(
      Engine engine, Optional<DeletionVectorDescriptor> dvOpt) {
    if (!dvOpt.isPresent()) {
      return new RoaringBitmapArray();
    }
    DeletionVectorDescriptor dv = dvOpt.get();
    if (dv.isInline()) {
      throw new UnsupportedOperationException(
          "Inline deletion vectors are not supported by Flink upserts (DV "
              + dv.getUniqueId()
              + ")");
    }
    return dvAccess.read(engine, dv.getAbsolutePath(table.getTablePath().toString()));
  }

  /**
   * Build a fresh {@link AddFile#FULL_SCHEMA} row from {@code source}'s fields, replacing only the
   * {@code deletionVector}. Mirrors {@link AddFile#toRemoveFileRow}'s field-by-field copy so the
   * resulting row is independent of the source's runtime schema (which may or may not include
   * stats).
   *
   * <p>Exposed for {@link io.delta.flink.sink.DeltaUpsertWriterTask}, which needs the same
   * AddFile-with-new-DV splice when decorating the writer task's outputs.
   */
  public static Row addFileRowWithNewDv(AddFile source, DeletionVectorDescriptor newDv) {
    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(AddFile.FULL_SCHEMA.indexOf("path"), source.getPath());
    fieldMap.put(AddFile.FULL_SCHEMA.indexOf("partitionValues"), source.getPartitionValues());
    fieldMap.put(AddFile.FULL_SCHEMA.indexOf("size"), source.getSize());
    fieldMap.put(AddFile.FULL_SCHEMA.indexOf("modificationTime"), source.getModificationTime());
    fieldMap.put(AddFile.FULL_SCHEMA.indexOf("dataChange"), true);
    fieldMap.put(AddFile.FULL_SCHEMA.indexOf("deletionVector"), newDv.toRow());
    source.getTags().ifPresent(t -> fieldMap.put(AddFile.FULL_SCHEMA.indexOf("tags"), t));
    source
        .getBaseRowId()
        .ifPresent(id -> fieldMap.put(AddFile.FULL_SCHEMA.indexOf("baseRowId"), id));
    source
        .getDefaultRowCommitVersion()
        .ifPresent(v -> fieldMap.put(AddFile.FULL_SCHEMA.indexOf("defaultRowCommitVersion"), v));
    source.getStatsJson().ifPresent(s -> fieldMap.put(AddFile.FULL_SCHEMA.indexOf("stats"), s));
    return new GenericRow(AddFile.FULL_SCHEMA, fieldMap);
  }
}
