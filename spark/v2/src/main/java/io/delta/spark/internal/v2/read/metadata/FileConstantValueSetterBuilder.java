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

import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;

/**
 * Builder for a {@code _metadata} field whose value is a per-file constant. Wraps a {@code
 * PartitionedFile -> Any} extractor sourced from {@code
 * DeltaParquetFileFormat#fileConstantMetadataExtractors} (which augments Spark's base map with
 * Delta-specific extractors like {@code base_row_id} and {@code default_row_commit_version}).
 *
 * <p>{@link #buildWithFile} reads the value once; the returned bound setter writes the same value
 * for every row read from the file.
 */
public final class FileConstantValueSetterBuilder implements MetadataValueSetterBuilder {

  private final Function1<PartitionedFile, Object> extractor;

  public FileConstantValueSetterBuilder(Function1<PartitionedFile, Object> extractor) {
    this.extractor = Objects.requireNonNull(extractor);
  }

  @Override
  public BoundMetadataValueSetter buildWithFile(PartitionedFile file) {
    // Mirror Spark's FileFormat#updateMetadataInternalRow: wrap the raw extractor value in a
    // Catalyst Literal so primitives (e.g. String -> UTF8String) get converted to their internal
    // representations once, here, rather than on every row.
    Literal literal = Literal$.MODULE$.apply(extractor.apply(file));
    return new ConstantBoundSetter(literal.value());
  }

  /**
   * Bound setter that always writes the captured Catalyst value (or null). {@code null} is
   * preserved by writing it through {@link
   * org.apache.spark.sql.catalyst.expressions.GenericInternalRow#update}, which is equivalent to
   * {@code setNullAt} for the read-side use case where downstream reads use type-specific accessors
   * against a nullable struct field.
   */
  private static final class ConstantBoundSetter implements BoundMetadataValueSetter, Serializable {
    private final Object value;

    ConstantBoundSetter(Object value) {
      this.value = value;
    }

    @Override
    public void setValue(
        org.apache.spark.sql.catalyst.expressions.GenericInternalRow metadataRow,
        int ordinal,
        org.apache.spark.sql.catalyst.InternalRow innerRow) {
      if (value == null) {
        metadataRow.setNullAt(ordinal);
      } else {
        metadataRow.update(ordinal, value);
      }
    }
  }
}
