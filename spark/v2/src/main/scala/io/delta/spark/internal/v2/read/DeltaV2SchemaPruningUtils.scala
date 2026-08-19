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
package io.delta.spark.internal.v2.read

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.types.StructType

private[v2] object DeltaV2SchemaPruningUtils {

  /**
   * Applies Spark's nested pruning inside requested roots while retaining every table root.
   * Required non-table fields, such as metadata columns, are appended to the result.
   */
  def retainRootColumns(
      tableSchema: StructType,
      requiredSchema: StructType,
      resolver: Resolver): StructType = {
    val tableFields = tableSchema.fields.map { tableField =>
      requiredSchema.fields.find(requiredField => resolver(tableField.name, requiredField.name))
        .getOrElse(tableField)
    }
    val nonTableFields = requiredSchema.fields.filterNot { requiredField =>
      tableSchema.fields.exists(tableField => resolver(tableField.name, requiredField.name))
    }
    StructType(tableFields ++ nonTableFields)
  }

}
