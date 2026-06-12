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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;

/**
 * Builder for {@code _metadata.row_commit_version}. Per-row coalesce against the materialised
 * row-commit-version helper column: if non-null, use it; otherwise fall back to the file's {@code
 * default_row_commit_version} constant.
 */
public final class RowCommitVersionValueSetterBuilder implements MetadataValueSetterBuilder {

  private final int materializedRowCommitVersionIdx;

  public RowCommitVersionValueSetterBuilder(int materializedRowCommitVersionIdx) {
    this.materializedRowCommitVersionIdx = materializedRowCommitVersionIdx;
  }

  @Override
  public BoundMetadataValueSetter buildWithFile(PartitionedFile file) {
    // default_row_commit_version is required whenever row tracking is enabled on the file; fail
    // fast with an actionable error rather than NPE if it's missing.
    Object rawDefault =
        file.otherConstantMetadataColumnValues()
            .get(DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME())
            .getOrElse(() -> null);
    if (rawDefault == null) {
      throw new IllegalStateException(
          "Missing '"
              + DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME()
              + "' on PartitionedFile '"
              + file.filePath()
              + "' for row tracking - every Delta "
              + "file with row tracking enabled must carry a default_row_commit_version constant.");
    }
    long defaultCommitVersion = ((Number) rawDefault).longValue();
    return new Bound(defaultCommitVersion, materializedRowCommitVersionIdx);
  }

  private static final class Bound implements BoundMetadataValueSetter, Serializable {
    private final long defaultCommitVersion;
    private final int materializedRowCommitVersionIdx;

    Bound(long defaultCommitVersion, int materializedRowCommitVersionIdx) {
      this.defaultCommitVersion = defaultCommitVersion;
      this.materializedRowCommitVersionIdx = materializedRowCommitVersionIdx;
    }

    @Override
    public void setValue(GenericInternalRow metadataRow, int ordinal, InternalRow innerRow) {
      long version;
      if (innerRow.isNullAt(materializedRowCommitVersionIdx)) {
        version = defaultCommitVersion;
      } else {
        version = innerRow.getLong(materializedRowCommitVersionIdx);
      }
      metadataRow.setLong(ordinal, version);
    }
  }
}
