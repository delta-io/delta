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
package io.delta.spark.internal.v2.write;

import static io.delta.spark.internal.v2.utils.ScalaUtils.toJavaMap;
import static io.delta.spark.internal.v2.utils.ScalaUtils.toScalaList;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.GenerateIcebergCompatActionUtils;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import org.apache.spark.sql.delta.DeltaColumnMapping;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils;
import org.apache.spark.sql.connector.expressions.filter.AlwaysTrue;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Executes a metadata-only DELETE on the DSv2 Delta table: the set of files whose rows all match
 * the delete condition are removed in their entirety by committing {@code RemoveFile} actions,
 * without scanning or rewriting any data.
 */
public final class DeltaMetadataOnlyDeleteExecutor {

  private static final Logger LOG =
      LoggerFactory.getLogger(DeltaMetadataOnlyDeleteExecutor.class);

  private DeltaMetadataOnlyDeleteExecutor() {}

  /** Selects and removes every file whose rows match {@code predicates}. */
  public static void deleteWhere(
      Engine engine,
      SnapshotImpl initialSnapshot,
      Optional<CatalogTable> catalogTable,
      Predicate[] predicates) {
    requireNonNull(engine, "engine is null");
    requireNonNull(initialSnapshot, "initialSnapshot is null");
    requireNonNull(catalogTable, "catalogTable is null");
    requireNonNull(predicates, "predicates is null");

    List<AddFile> filesToRemove =
        selectFilesToRemove(engine, initialSnapshot, catalogTable, predicates);

    if (filesToRemove.isEmpty()) {
      LOG.info("Metadata-only delete matched no files; nothing to commit");
      return;
    }

    deleteFiles(engine, initialSnapshot, filesToRemove);
  }

  private static List<AddFile> selectFilesToRemove(
      Engine engine,
      SnapshotImpl initialSnapshot,
      Optional<CatalogTable> catalogTable,
      Predicate[] predicates) {
    DeltaV2Snapshot snapshot =
        new DeltaV2Snapshot(initialSnapshot);

    List<Expression> catalystFilters = new ArrayList<>(predicates.length);
    for (Predicate predicate : predicates) {
      if (!(predicate instanceof AlwaysTrue)) {
        catalystFilters.add(toCatalyst(predicate));
      }
    }

    return CollectionConverters.asJava(
        snapshot
            .filesForScan(
                toScalaList(catalystFilters.toArray(new Expression[0])),
                true)
            .files());
  }

  private static Expression toCatalyst(Predicate predicate) {
    // Conversion is all-or-nothing: None means this predicate or one of its children is
    // unsupported.
    Option<Expression> expression = V2ExpressionUtils.toCatalyst(predicate);
    if (expression.isEmpty()) {
      throw new IllegalStateException("Could not convert " + predicate + " to Catalyst");
    }
    return expression.get();
  }

  private static void deleteFiles(
      Engine engine, SnapshotImpl initialSnapshot, List<AddFile> filesToRemove) {
    // The operation is committed as WRITE, not DELETE as Kernel Transactions don't support it.
    Transaction transaction =
        initialSnapshot
            .buildUpdateTableTransaction(DeltaV2BatchWrite.getEngineInfo(), Operation.WRITE)
            .build(engine);

    org.apache.spark.sql.types.StructType sparkSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    StructType physicalSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(
            DeltaColumnMapping.renameColumns(sparkSchema));
    long removeTimestamp = System.currentTimeMillis();

    List<Row> removeActionRows = new ArrayList<>(filesToRemove.size());
    for (AddFile file : filesToRemove) {
      removeActionRows.add(createRemoveFileActionRow(file, physicalSchema, removeTimestamp));
    }

    CloseableIterable<Row> dataActions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(removeActionRows.iterator()));
    TransactionCommitResult result = transaction.commit(engine, dataActions);
    LOG.info(
        "Metadata-only delete committed at version {}, removed {} files",
        result.getVersion(),
        filesToRemove.size());
  }

  // Kernel's extended-metadata RemoveFile builder does not accept tags, so this conversion drops
  // them. This conversion code is temporary.
  private static Row createRemoveFileActionRow(
      AddFile file, StructType physicalSchema, long removeTimestamp) {
    MapValue partitionValues =
        VectorUtils.stringStringMapValue(toJavaMap(file.partitionValues()));
    Optional<DataFileStatistics> stats =
        Optional.ofNullable(file.stats())
            .flatMap(json -> DataFileStatistics.deserializeFromJson(json, physicalSchema));
    Row removeFileRow =
        GenerateIcebergCompatActionUtils.createRemoveFileRowWithExtendedFileMetadata(
            file.path(),
            removeTimestamp,
            /* dataChange = */ true,
            partitionValues,
            file.size(),
            stats,
            physicalSchema,
            toJavaLong(file.baseRowId()),
            toJavaLong(file.defaultRowCommitVersion()),
            toKernelDeletionVector(file));
    return SingleAction.createRemoveFileSingleAction(removeFileRow);
  }

  private static Optional<DeletionVectorDescriptor> toKernelDeletionVector(AddFile file) {
    return Optional.ofNullable(file.deletionVector())
        .map(
            deletionVector ->
                new DeletionVectorDescriptor(
                    deletionVector.storageType(),
                    deletionVector.pathOrInlineDv(),
                    toJavaInt(deletionVector.offset()),
                    deletionVector.sizeInBytes(),
                    deletionVector.cardinality()));
  }

  private static Optional<Long> toJavaLong(scala.Option<Object> value) {
    return value.isDefined()
        ? Optional.of(((Number) value.get()).longValue())
        : Optional.empty();
  }

  private static Optional<Integer> toJavaInt(scala.Option<Object> value) {
    return value.isDefined() ? Optional.of((Integer) value.get()) : Optional.empty();
  }
}
