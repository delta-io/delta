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

package io.delta.spark.internal.v2.catalog;

import org.apache.spark.sql.delta.ColumnTypeChangeSupport;
import org.apache.spark.sql.delta.v2.interop.AbstractMetadata;
import org.apache.spark.sql.delta.v2.interop.AbstractProtocol;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsSchemaEvolution;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableChange;

/**
 * Shim to build the DSv2 Delta table connector against different Spark versions.
 * This is the shim for Spark 4.2:
 * - Schema evolution support in DSV2 (add columns, change type) was added in Spark 4.2.
 */
public abstract class DeltaV2TableShims implements SupportsSchemaEvolution {

  /** Implemented in DeltaV2Table to provide access to the table protocol/metadata. */
  protected abstract AbstractProtocol protocol();
  protected abstract AbstractMetadata metadata();

  /**
   * Whether an analyzer-proposed schema-evolution column change (adding a column or widening a
   * column type) can be applied to this Delta table. Adding a column is always allowed; changing a
   * column type delegates to the connector-agnostic {@link ColumnTypeChangeSupport}.
   */
  @Override
  public boolean supportsColumnChange(TableChange.ColumnChange change) {
    if (change instanceof TableChange.AddColumn) {
      return true;
    }
    if (change instanceof TableChange.UpdateColumnType) {
      TableChange.UpdateColumnType typeChange = (TableChange.UpdateColumnType) change;
      return ColumnTypeChangeSupport.supportsTypeChange(
          SparkSession.active(),
          protocol(),
          metadata(),
          ScalaUtils.toScalaList(typeChange.fieldNames()),
          typeChange.newDataType());
    }
    return false;
  }

  /** The schema evolution capability to advertise. */
  public static Optional<TableCapability> schemaEvolutionCapability() {
    return Optional.of(TableCapability.AUTOMATIC_SCHEMA_EVOLUTION);
  }
}
