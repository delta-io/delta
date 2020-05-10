/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.tables.execution

import org.apache.spark.sql.delta.commands.ConvertToDeltaCommand
import io.delta.tables.DeltaTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

trait DeltaConvertBase {
  def executeConvert(
      spark: SparkSession,
      tableIdentifier: TableIdentifier,
      partitionSchema: Option[StructType],
      deltaPath: Option[String]): DeltaTable = {
    val cvt = ConvertToDeltaCommand(tableIdentifier, partitionSchema, deltaPath)
    cvt.run(spark)
    if (cvt.isCatalogTable(spark.sessionState.analyzer, tableIdentifier)) {
      DeltaTable.forName(spark, tableIdentifier.toString)
    } else {
      DeltaTable.forPath(spark, tableIdentifier.table)
    }
  }
}

object DeltaConvert extends DeltaConvertBase {}
