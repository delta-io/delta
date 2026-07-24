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
package io.delta.spark.internal.v2.read.metadata;

import io.delta.spark.internal.v2.utils.CloseableIterator;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * Read-function decorator that materialises the DSv2 {@code _metadata} struct column for each row
 * read from a {@link PartitionedFile}.
 *
 * <p>One {@link MetadataValueSetterBuilder} per requested {@code _metadata} subfield is held by the
 * supplied {@link MetadataStructSchemaContext}. {@link #apply} binds each setter to the current
 * file and runs them in pruned-struct order against the inner reader's row, writing into a
 * caller-owned {@link GenericInternalRow}. The same row is reused across rows of the same file -
 * setters write their slots in place, so there is no per-row {@code Object[]} copy.
 *
 * <p>Output row layout: {@code data columns + _metadata + partition columns}. Partition columns are
 * projected out of the inner row using the ordinals tracked by {@link MetadataStructSchemaContext};
 * the parquet read schema (with row-tracking helper columns when applicable) is also produced by
 * the same context.
 */
public class MetadataStructReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final MetadataStructSchemaContext metadataContext;

  private MetadataStructReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      MetadataStructSchemaContext metadataContext) {
    this.baseReadFunc = Objects.requireNonNull(baseReadFunc);
    this.metadataContext = Objects.requireNonNull(metadataContext);
  }

  /** Wraps a base reader to materialise {@code _metadata}. */
  public static MetadataStructReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      MetadataStructSchemaContext metadataContext) {
    return new MetadataStructReadFunction(baseReadFunc, metadataContext);
  }


  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    BoundMetadataValueSetter[] bound =
        Arrays.stream(metadataContext.getValueSetterBuilders())
            .map(builder -> builder.buildWithFile(file))
            .toArray(BoundMetadataValueSetter[]::new);

    GenericInternalRow metadataRow = new GenericInternalRow(bound.length);
    GenericInternalRow metadataStruct = new GenericInternalRow(1);
    metadataStruct.update(0, metadataRow);

    ProjectingInternalRow dataProjection =
        ProjectingInternalRow.apply(
            metadataContext.getDataSchema(), metadataContext.getDataColumnsOrdinals());
    ProjectingInternalRow partitionProjection =
        ProjectingInternalRow.apply(
            metadataContext.getPartitionSchema(), metadataContext.getPartitionColumnsOrdinals());
    JoinedRow joinedDataAndMetadata = new JoinedRow();
    JoinedRow joinedWithPartitions = new JoinedRow();
    boolean hasPartitionColumns = metadataContext.hasPartitionColumns();

    Iterator<InternalRow> baseIterator = baseReadFunc.apply(file);

    return CloseableIterator.wrap(baseIterator)
        .mapCloseable(
            row -> {
              for (int i = 0; i < bound.length; i++) {
                bound[i].setValue(metadataRow, i, row);
              }
              dataProjection.project(row);
              InternalRow withMetadata =
                  (InternalRow) joinedDataAndMetadata.apply(dataProjection, metadataStruct);
              if (hasPartitionColumns) {
                partitionProjection.project(row);
                return (InternalRow) joinedWithPartitions.apply(withMetadata, partitionProjection);
              }
              return withMetadata;
            });
  }
}
