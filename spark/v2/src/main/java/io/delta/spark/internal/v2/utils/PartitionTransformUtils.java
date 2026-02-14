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
package io.delta.spark.internal.v2.utils;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;

/** Shared validation and extraction for Spark DSv2 partition {@link Transform}s. */
public final class PartitionTransformUtils {

  private PartitionTransformUtils() {}

  /**
   * Validate partition transforms and extract top-level column names.
   *
   * <p>Only identity transforms on top-level columns are supported.
   *
   * @param partitions Spark partition transforms
   * @return ordered list of partition column names
   * @throws UnsupportedOperationException if a transform is not an identity transform or references
   *     a nested column
   * @throws IllegalArgumentException if a transform has an unexpected number of references
   */
  public static List<String> extractPartitionColumnNames(Transform[] partitions) {
    requireNonNull(partitions, "partitions is null");
    final List<String> columnNames = new ArrayList<>(partitions.length);
    for (Transform transform : partitions) {
      if (!(transform instanceof IdentityTransform)) {
        throw new UnsupportedOperationException(
            "Partitioning by expressions is not supported: " + transform.name());
      }
      NamedReference[] refs = transform.references();
      if (refs == null || refs.length != 1) {
        throw new IllegalArgumentException("Invalid partition transform: " + transform);
      }
      String[] fieldNames = refs[0].fieldNames();
      if (fieldNames == null || fieldNames.length != 1) {
        throw new UnsupportedOperationException(
            "Partition columns must be top-level columns: " + refs[0].describe());
      }
      columnNames.add(fieldNames[0]);
    }
    return columnNames;
  }
}
