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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.SerializableScanData;
import io.delta.spark.internal.v2.utils.SnapshotScanHelper;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * A serializable single-partition RDD that lazily streams Kernel scan files into Spark Rows. Holds
 * a {@link SerializableScanData} which captures everything needed to reconstruct a {@link
 * io.delta.kernel.internal.replay.LogReplay} on any executor.
 */
@SuppressWarnings("unchecked")
class ScanFileRDD extends RDD<Row> {
  private final SerializableScanData scanData;

  ScanFileRDD(SparkContext sc, SerializableScanData scanData) {
    super(
        sc,
        (scala.collection.immutable.Seq<Dependency<?>>)
            (scala.collection.immutable.Seq<?>) scala.collection.immutable.Nil$.MODULE$,
        scala.reflect.ClassTag$.MODULE$.apply(Row.class));
    this.scanData = scanData;
  }

  @Override
  public Partition[] getPartitions() {
    return new Partition[] {
      new Partition() {
        @Override
        public int index() {
          return 0;
        }
      }
    };
  }

  @Override
  public scala.collection.Iterator<Row> compute(Partition split, TaskContext context) {
    CloseableIterator<FilteredColumnarBatch> filesIter =
        SnapshotScanHelper.rebuildScanFilesIterator(scanData);

    if (context != null) {
      context.addTaskCompletionListener(
          taskCtx -> {
            try {
              filesIter.close();
            } catch (IOException e) {
              /* best-effort */
            }
          });
    }

    Iterator<Row> javaIter =
        new Iterator<Row>() {
          private FilteredColumnarBatch currentBatch = null;
          private int currentRowId = 0;
          private int currentBatchSize = 0;
          private Row nextRow = null;

          @Override
          public boolean hasNext() {
            while (nextRow == null) {
              if (currentBatch != null && currentRowId < currentBatchSize) {
                Optional<AddFile> addOpt = StreamingHelper.getAddFile(currentBatch, currentRowId);
                currentRowId++;
                if (addOpt.isPresent()) {
                  nextRow = addFileToSparkRow(addOpt.get());
                }
              } else if (filesIter.hasNext()) {
                currentBatch = filesIter.next();
                currentRowId = 0;
                currentBatchSize = currentBatch.getData().getSize();
              } else {
                return false;
              }
            }
            return true;
          }

          @Override
          public Row next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            Row row = nextRow;
            nextRow = null;
            return row;
          }
        };

    return scala.jdk.javaapi.CollectionConverters.asScala(javaIter);
  }

  /** Convert a Kernel {@link AddFile} to a Spark {@link Row}. */
  @SuppressWarnings("unchecked")
  static Row addFileToSparkRow(AddFile addFile) {
    scala.collection.Map<String, String> partitionValues =
        scala.jdk.javaapi.CollectionConverters.asScala(
            VectorUtils.toJavaMap(addFile.getPartitionValues()));

    Row dvRow = null;
    Optional<DeletionVectorDescriptor> dvOpt = addFile.getDeletionVector();
    if (dvOpt.isPresent()) {
      DeletionVectorDescriptor dv = dvOpt.get();
      dvRow =
          RowFactory.create(
              dv.getStorageType(),
              dv.getPathOrInlineDv(),
              dv.getOffset().orElse(null),
              dv.getSizeInBytes(),
              dv.getCardinality());
    }

    scala.collection.Map<String, String> tags =
        addFile
            .getTags()
            .map(
                t ->
                    (scala.collection.Map<String, String>)
                        scala.jdk.javaapi.CollectionConverters.asScala(
                            (Map<String, String>) (Map<?, ?>) VectorUtils.toJavaMap(t)))
            .orElse(null);

    return RowFactory.create(
        addFile.getPath(),
        partitionValues,
        addFile.getSize(),
        addFile.getModificationTime(),
        addFile.getDataChange(),
        dvRow,
        tags,
        addFile.getBaseRowId().orElse(null),
        addFile.getDefaultRowCommitVersion().orElse(null));
  }

  /** Convert a Spark {@link Row} back to a Kernel {@link AddFile}. */
  static AddFile sparkRowToAddFile(Row row) {
    String path = row.getString(0);
    Map<String, String> partitionValues = row.getJavaMap(1);
    long size = row.getLong(2);
    long modificationTime = row.getLong(3);
    boolean dataChange = row.getBoolean(4);

    Optional<DeletionVectorDescriptor> dvOpt = Optional.empty();
    if (!row.isNullAt(5)) {
      Row dvRow = row.getStruct(5);
      dvOpt =
          Optional.of(
              new DeletionVectorDescriptor(
                  dvRow.getString(0),
                  dvRow.getString(1),
                  dvRow.isNullAt(2) ? Optional.empty() : Optional.of(dvRow.getInt(2)),
                  dvRow.getInt(3),
                  dvRow.getLong(4)));
    }

    Optional<Map<String, String>> tagsOpt = Optional.empty();
    if (!row.isNullAt(6)) {
      tagsOpt = Optional.of(row.getJavaMap(6));
    }

    Optional<Long> baseRowId = row.isNullAt(7) ? Optional.empty() : Optional.of(row.getLong(7));
    Optional<Long> defaultRowCommitVersion =
        row.isNullAt(8) ? Optional.empty() : Optional.of(row.getLong(8));

    MapValue kernelPartVals = VectorUtils.stringStringMapValue(partitionValues);
    Optional<MapValue> kernelTags = tagsOpt.map(VectorUtils::stringStringMapValue);

    Map<Integer, Object> fieldMap = new HashMap<>();
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("path"), path);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("partitionValues"), kernelPartVals);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("size"), size);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("modificationTime"), modificationTime);
    fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("dataChange"), dataChange);
    dvOpt.ifPresent(
        dv -> fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("deletionVector"), dv.toRow()));
    kernelTags.ifPresent(t -> fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("tags"), t));
    baseRowId.ifPresent(id -> fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("baseRowId"), id));
    defaultRowCommitVersion.ifPresent(
        v -> fieldMap.put(AddFile.SCHEMA_WITHOUT_STATS.indexOf("defaultRowCommitVersion"), v));

    return new AddFile(new GenericRow(AddFile.SCHEMA_WITHOUT_STATS, fieldMap));
  }
}
