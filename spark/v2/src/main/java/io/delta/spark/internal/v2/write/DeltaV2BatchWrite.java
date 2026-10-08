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

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.delta.DeltaOperations;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.v2.interop.DeltaV2OptimisticTransaction;
import org.apache.spark.sql.delta.v2.kernel.KernelActionUtils$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/**
 * BatchWrite for DSv2 batch append using Spark's Parquet path. Obtains the optimistic transaction
 * created by {@link DeltaV2Write#toBatch}. When Spark requests a writer factory, the shared {@link
 * DeltaV2WriteContext} builds its Kernel transaction and captures the executor state.
 *
 * <p>The optimistic transaction lives only on the driver and is never serialized. Executors receive
 * only serializable state: transaction state row, Hadoop conf, OutputWriterFactory, and
 * schema/partition ordinals; the per-partition target directory is derived on the executor.
 */
class DeltaV2BatchWrite implements Write, BatchWrite {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaV2BatchWrite.class);

  private final DeltaV2WriteContext context;
  private final DeltaV2OptimisticTransaction optimisticTransaction;

  DeltaV2BatchWrite(
      DeltaV2OptimisticTransaction optimisticTransaction, DeltaV2WriteContext context) {
    this.context = requireNonNull(context, "context is null");
    this.optimisticTransaction =
        requireNonNull(optimisticTransaction, "optimisticTransaction is null");
  }

  @Override
  public BatchWrite toBatch() {
    return this;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return context.buildDataWriterFactory(Operation.WRITE);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<Action> actions = new ArrayList<>();
    for (WriterCommitMessage msg : messages) {
      if (msg instanceof DeltaV2WriterCommitMessage) {
        for (SerializableKernelRowWrapper wrapper :
            ((DeltaV2WriterCommitMessage) msg).getActionRows()) {
          List<Action> rowActions =
              CollectionConverters.asJava(
                  KernelActionUtils$.MODULE$.actionsFromKernelRow(wrapper.getRow()));
          if (rowActions.size() != 1) {
            throw new IllegalArgumentException(
                "Expected exactly one action from writer commit message, but found "
                    + rowActions.size()
                    + " actions");
          }
          Action action = rowActions.get(0);
          if (!(action instanceof AddFile)) {
            throw new UnsupportedOperationException(
                "DeltaV2BatchWrite does not support '"
                    + action.getClass().getSimpleName()
                    + "' actions");
          }
          actions.add(action);
        }
      }
    }

    long version =
        optimisticTransaction.commit(
            CollectionConverters.asScala(actions).toSeq(), writeOperation());
    LOG.info("DSv2 batch write committed at version {}", version);
  }

  private DeltaOperations.Operation writeOperation() {
    Seq<String> partitionBy = ScalaUtils.toScalaList(context.getPartitionSchema().fieldNames());
    Option<String> userMetadata =
        Option.apply(context.getWriteInfo().options().get(DeltaOptions.USER_METADATA_OPTION()));
    return new DeltaOperations.Write(
        /* mode = */ SaveMode.Append,
        /* partitionBy = */ Option.apply(partitionBy),
        /* predicate = */ Option.empty(),
        /* userMetadata = */ userMetadata,
        /* isDynamicPartitionOverwrite = */ Option.empty(),
        /* canOverwriteSchema = */ Option.empty(),
        /* canMergeSchema = */ Option.empty(),
        /* replaceOnCond = */ Option.empty(),
        /* replaceUsingCols = */ Option.empty());
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.warn(
        "DSv2 batch write aborted. {} task messages will not be committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        messages != null ? messages.length : 0);
  }
}
