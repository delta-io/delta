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
import org.apache.spark.sql.execution.datasources.PartitionedFile;

/**
 * Builder for a {@link BoundMetadataValueSetter}, parametrised by a {@link PartitionedFile}.
 *
 * <p>One instance is registered per requested subfield of the DSv2 {@code _metadata} struct (e.g.
 * {@code file_path}, {@code file_size}, {@code row_id}, {@code row_commit_version}). {@link
 * MetadataStructSchemaContext} owns the ordered array of builders in pruned-struct field order;
 * {@link MetadataStructReadFunction#apply} invokes {@link #buildWithFile} once per file and runs
 * the resulting bound setters per row.
 *
 * <p>Three implementations exist today, all under {@code v2/read/metadata/}:
 *
 * <ul>
 *   <li>{@link FileConstantValueSetterBuilder} — for fields whose value is a constant of the file
 *       (Spark file-source base fields plus any Delta-specific file-constant extractors exposed by
 *       {@code DeltaParquetFileFormat#fileConstantMetadataExtractors}).
 *   <li>{@link RowIdValueSetterBuilder} / {@link RowCommitVersionValueSetterBuilder} — for the two
 *       row-tracking fields whose value is computed per row from materialised helper columns plus
 *       file-constant fallbacks.
 * </ul>
 *
 * <p>All three flow through the same per-row materialisation step, keeping the per-field plumbing
 * uniform regardless of where the value comes from.
 */
public interface MetadataValueSetterBuilder extends Serializable {

  /**
   * Returns a {@link BoundMetadataValueSetter} parametrised by {@code file}'s constant metadata.
   * The returned setter is invoked per row by {@link MetadataStructReadFunction}.
   */
  BoundMetadataValueSetter buildWithFile(PartitionedFile file);
}
