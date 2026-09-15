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

/**
 * File-scoped resolver produced by {@link MetadataValueSetterBuilder#buildWithFile}. Writes one
 * {@code _metadata} struct ordinal per row.
 */
public interface BoundMetadataValueSetter extends Serializable {

  /**
   * Writes this field's value at {@code ordinal} of the caller-owned {@code metadataRow}. May read
   * from {@code innerRow} (the raw row from the inner Parquet reader, including any row-tracking
   * helper columns), or simply write a value captured at file-bind time.
   */
  void setValue(GenericInternalRow metadataRow, int ordinal, InternalRow innerRow);
}
