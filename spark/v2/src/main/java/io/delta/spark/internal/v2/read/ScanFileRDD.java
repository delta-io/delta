/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import io.delta.kernel.Scan;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.KernelRowToSparkRow;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableReadOnlySnapshot;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag$;

/**
 * A single-partition Spark RDD that reconstructs a Kernel {@link Scan} on the executor from a
 * {@link SerializableReadOnlySnapshot} and lazily streams {@link AddFile} records as Spark {@link
 * Row}s. The RDD output schema matches {@link AddFile#SCHEMA_WITHOUT_STATS}.
 *
 * <p>Single partition is an intentional limitation; a future version will use Kernel's plan API for
 * multi-partition replay. The downstream sort is still distributed.
 */
public class ScanFileRDD extends RDD<Row> {

  private static final Logger LOG = LoggerFactory.getLogger(ScanFileRDD.class);

  public static final StructType SPARK_SCHEMA =
      SchemaUtils.convertKernelSchemaToSparkSchema(AddFile.SCHEMA_WITHOUT_STATS);

  private final SerializableReadOnlySnapshot serializableSnapshot;

  public ScanFileRDD(SparkContext sc, SerializableReadOnlySnapshot serializableSnapshot) {
    super(
        sc,
        scala.collection.immutable.Seq$.MODULE$.<org.apache.spark.Dependency<?>>empty(),
        ClassTag$.MODULE$.apply(Row.class));
    this.serializableSnapshot = serializableSnapshot;
  }

  private static final class SinglePartition implements Partition, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int index() {
      return 0;
    }
  }

  @Override
  public Partition[] getPartitions() {
    return new Partition[] {new SinglePartition()};
  }

  @Override
  public scala.collection.Iterator<Row> compute(Partition split, TaskContext context) {
    Engine engine = DefaultEngine.create(serializableSnapshot.getHadoopConf());
    Scan scan = serializableSnapshot.toScan();

    CloseableIterator<FilteredColumnarBatch> batchIter;
    try {
      batchIter = scan.getScanFiles(engine);
    } catch (KernelEngineException e) {
      throw e;
    } catch (Exception e) {
      throw new KernelEngineException("open scan files on executor", e);
    }

    AddFileLazyIterator lazyIter = new AddFileLazyIterator(batchIter);

    if (context != null) {
      context.addTaskCompletionListener(
          ctx -> {
            try {
              batchIter.close();
            } catch (IOException e) {
              LOG.warn("Failed to close scan file iterator for distributed initial snapshot", e);
            }
          });
    }

    return lazyIter;
  }

  /**
   * Lazy Scala iterator that streams AddFile rows one at a time from the underlying Kernel batch
   * iterator. No eager materialization into a list.
   */
  private static final class AddFileLazyIterator implements scala.collection.Iterator<Row> {

    private final CloseableIterator<FilteredColumnarBatch> batchIter;

    private FilteredColumnarBatch currentBatch;
    private int currentRowId;
    private int currentBatchSize;
    private Row nextRow;

    AddFileLazyIterator(CloseableIterator<FilteredColumnarBatch> batchIter) {
      this.batchIter = batchIter;
      this.currentBatch = null;
      this.currentRowId = 0;
      this.currentBatchSize = 0;
      this.nextRow = null;
    }

    @Override
    public boolean hasNext() {
      if (nextRow != null) {
        return true;
      }
      nextRow = advance();
      return nextRow != null;
    }

    @Override
    public Row next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      Row result = nextRow;
      nextRow = null;
      return result;
    }

    private Row advance() {
      while (true) {
        while (currentRowId < currentBatchSize) {
          int rowId = currentRowId++;
          Optional<AddFile> addOpt = StreamingHelper.getAddFile(currentBatch, rowId);
          if (addOpt.isPresent()) {
            return new KernelRowToSparkRow(addOpt.get().toRow(), SPARK_SCHEMA);
          }
        }
        if (!batchIter.hasNext()) {
          return null;
        }
        currentBatch = batchIter.next();
        currentRowId = 0;
        currentBatchSize = currentBatch.getData().getSize();
      }
    }
  }
}
