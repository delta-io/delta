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

package io.delta.flink.sink.dynamic;

import org.apache.flink.table.data.RowData;

/**
 * A record destined for a dynamically-routed Delta table.
 *
 * <p>Each {@code DynamicRow} carries:
 *
 * <ul>
 *   <li>the target table identifier as a single string (e.g. {@code "catalog.schema.table"}),
 *   <li>the table schema as a JSON string, used only during Parquet file generation and discarded
 *       afterward,
 *   <li>the actual record payload,
 *   <li>the partition column names that define the table's partition layout,
 *   <li>the partition column values for the record, aligned to the partition column order.
 * </ul>
 */
public interface DynamicRow {

  /**
   * Returns the table identifier, e.g. {@code "catalog.schema.table"}.
   *
   * @return table name
   */
  String getTableName();

  /**
   * Returns the JSON-encoded Delta {@code StructType} schema for this record's table. Used only
   * when the table needs to be created; discarded after file generation.
   *
   * @return JSON schema string
   */
  String getSchemaStr();

  /**
   * Returns the record payload.
   *
   * @return row data
   */
  RowData getRow();

  /**
   * Returns the partition column names that define this table's partition layout. Used when
   * creating a new table. The ordering must match {@link #getPartitionValues()}.
   *
   * @return partition column names
   */
  String[] getPartitionColumns();

  /**
   * Returns the partition column values, ordered to match {@link #getPartitionColumns()}.
   *
   * @return partition values as strings
   */
  String[] getPartitionValues();
}
