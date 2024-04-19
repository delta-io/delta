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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.StructField

object ColumnDefinitionShims {

  /**
   * Helps handle a breaking change in [[org.apache.spark.sql.catalyst.plans.logical.CreateTable]]
   * between Spark 3.5 and Spark 4.0:
   * - In 3.5, `CreateTable` accepts a `tableSchema: StructType`.
   * - In 4.0, `CreateTable` accepts a `columns: Seq[ColumnDefinition]`.
   */
  def parseColumns(columns: Seq[StructField], sqlParser: ParserInterface): Seq[ColumnDefinition] = {
    columns.map(ColumnDefinition.fromV1Column(_, sqlParser)).toSeq
  }
}
