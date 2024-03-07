/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.commands.columnmapping

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types.StructType

/**
 * A command to remove the column mapping from a table.
 */
class RemoveColumnMappingCommand(
    val deltaLog: DeltaLog,
    val catalogOpt: Option[CatalogTable])
  extends ImplicitMetadataOperation {
  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = true

  /**
   * Remove the column mapping from the table.
   * @param removeColumnMappingTableProperty - whether to remove the column mapping property from
   *                                         the table instead of setting it to 'none'
   */
  def run(spark: SparkSession, removeColumnMappingTableProperty: Boolean): Unit = {
    val schema = deltaLog.update().schema
    verifySchemaFieldNames(schema)
  }

  /**
   * Verify none of the schema fields contain invalid column names.
   */
  protected def verifySchemaFieldNames(schema: StructType) = {
    val invalidColumnNames =
      SchemaUtils.findInvalidColumnNamesInSchema(schema)
    if (invalidColumnNames.nonEmpty) {
      throw DeltaErrors
        .foundInvalidColumnNamesWhenRemovingColumnMapping(invalidColumnNames)
    }
  }
}

object RemoveColumnMappingCommand {
  def apply(
      deltaLog: DeltaLog,
      catalogOpt: Option[CatalogTable]): RemoveColumnMappingCommand = {
    new RemoveColumnMappingCommand(deltaLog, catalogOpt)
  }
}
