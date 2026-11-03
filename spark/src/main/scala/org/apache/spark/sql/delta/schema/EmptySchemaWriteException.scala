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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.parquet.schema.InvalidSchemaException

import org.apache.spark.SparkException
import org.apache.spark.sql.types.{NullType, StructType}

object EmptySchemaWriteException {

  /**
   * Checks whether this is a Parquet writer exception due to an empty group in schema.
   */
  def isEmptySchemaWriteException(t: Throwable): Boolean = t match {
    case se: SparkException if
      se.getCause.isInstanceOf[InvalidSchemaException] &&
      Option(se.getCause.getMessage)
        .exists(_.contains("Cannot write a schema with an empty group")) => true
    case _ => false
  }

  /**
   * Decides whether this is a write to a table with no columns or a struct with no fields. Both
   * can be due to NullType being dropped from the written data. Always throws a more specific
   * exception, or the original exception if matching a specific case fails.
   *
   * This should only be called if [[isEmptySchemaWriteException]] returns true for the exception.
   */
  def throwEmptySchemaWriteException(t: Throwable, metadata: Metadata): Unit = {
    assert(isEmptySchemaWriteException(t))
    if (metadata.dataSchema.fields.isEmpty) {
      throw DeltaErrors.cannotWriteEmptySchemaTableNoColumns()
    }
    if (metadata.dataSchema.fields.forall(_.dataType.isInstanceOf[NullType])) {
      throw DeltaErrors.cannotWriteEmptySchemaTableAllVoidColumns()
    }
    val (columnPath, columnType) =
      SchemaUtils.findColumnPaths(metadata.dataSchema) {
        case StructType(fields) if fields.isEmpty => true
        case StructType(fields) if fields.forall(_.dataType.isInstanceOf[NullType]) => true
        case _ => false
      }.headOption.getOrElse {
        throw t // Shouldn't really happen, in case we get here due to some other error
      }
    val structFields = columnType.asInstanceOf[StructType].fields
    if (structFields.isEmpty) {
      throw DeltaErrors.cannotWriteEmptySchemaStructNoFields(columnPath.parts)
    }
    if (structFields.forall(_.dataType.isInstanceOf[NullType])) {
      throw DeltaErrors.cannotWriteEmptySchemaStructAllVoidFields(columnPath.parts)
    }
    throw t // Also shouldn't happen
  }
}
