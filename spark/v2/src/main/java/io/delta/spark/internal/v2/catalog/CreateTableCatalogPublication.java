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
package io.delta.spark.internal.v2.catalog;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.expressions.Transform;

/** Catalog registration payload derived from a committed version-0 Delta snapshot. */
public final class CreateTableCatalogPublication {
  private final Column[] columns;
  private final Transform[] partitions;
  private final Map<String, String> properties;

  public CreateTableCatalogPublication(
      Column[] columns, Transform[] partitions, Map<String, String> properties) {
    this.columns = columns;
    this.partitions = partitions;
    this.properties = properties;
  }

  public Column[] getColumns() {
    return columns;
  }

  public Transform[] getPartitions() {
    return partitions;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
