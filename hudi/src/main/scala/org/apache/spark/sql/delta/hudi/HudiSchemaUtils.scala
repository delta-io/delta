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

package org.apache.spark.sql.delta.hudi

import org.apache.avro.{LogicalTypes, Schema}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.types._

import java.util
import scala.collection.JavaConverters._

object HudiSchemaUtils extends DeltaLogging {

  /////////////////
  // Public APIs //
  /////////////////
  def convertDeltaSchemaToHudiSchema(deltaSchema: StructType): Schema = {
    /**
     * Recursively (i.e. for all nested elements) transforms the delta DataType `elem` into its
     * corresponding Avro type.
     */
    def transform[E <: DataType](elem: E, isNullable: Boolean, currentPath: String): Schema =
    elem match {
      case StructType(fields) =>

        val avroFields: util.List[Schema.Field] = fields.map(f =>
          new Schema.Field(
            f.name,
            transform(f.dataType, f.nullable, s"$currentPath.${f.name}"),
            f.getComment().orNull)).toList.asJava
        finalizeSchema(
          Schema.createRecord(currentPath, null, null, false, avroFields),
          isNullable)

      case ArrayType(elementType, containsNull) =>
        finalizeSchema(
          Schema.createArray(transform(elementType, containsNull, currentPath)),
          isNullable)

      case MapType(keyType, valueType, valueContainsNull) =>
        finalizeSchema(
          Schema.createMap(transform(valueType, valueContainsNull, currentPath)),
          isNullable)

      case atomicType: AtomicType => convertAtomic(atomicType, isNullable)

      case other =>
        throw new UnsupportedOperationException(s"Cannot convert Delta type $other to Hudi")
    }

    transform(deltaSchema, false, "root")
  }

  private def finalizeSchema(targetSchema: Schema, isNullable: Boolean): Schema = {
    if (isNullable) return Schema.createUnion(Schema.create(Schema.Type.NULL), targetSchema)
    targetSchema
  }

  private def convertAtomic[E <: DataType](elem: E, isNullable: Boolean) = elem match {
    case StringType => finalizeSchema(Schema.create(Schema.Type.STRING), isNullable)
    case LongType => finalizeSchema(Schema.create(Schema.Type.LONG), isNullable)
    case IntegerType | ShortType => finalizeSchema(Schema.create(Schema.Type.INT), isNullable)
    case FloatType => finalizeSchema(Schema.create(Schema.Type.FLOAT), isNullable)
    case DoubleType => finalizeSchema(Schema.create(Schema.Type.DOUBLE), isNullable)
    case d: DecimalType => finalizeSchema(LogicalTypes.decimal(d.precision, d.scale)
      .addToSchema(Schema.create(Schema.Type.BYTES)), isNullable)
    case BooleanType => finalizeSchema(Schema.create(Schema.Type.BOOLEAN), isNullable)
    case BinaryType => finalizeSchema(Schema.create(Schema.Type.BYTES), isNullable)
    case DateType => finalizeSchema(
      LogicalTypes.date.addToSchema(Schema.create(Schema.Type.INT)), isNullable)
    case TimestampType => finalizeSchema(
      LogicalTypes.timestampMicros.addToSchema(Schema.create(Schema.Type.LONG)), isNullable)
    case TimestampNTZType => finalizeSchema(
      LogicalTypes.localTimestampMicros.addToSchema(Schema.create(Schema.Type.LONG)), isNullable)
    case _ => throw new UnsupportedOperationException(s"Could not convert atomic type $elem")
  }
}
