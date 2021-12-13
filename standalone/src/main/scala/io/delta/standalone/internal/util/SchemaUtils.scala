/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import io.delta.standalone.exceptions.DeltaStandaloneException
import io.delta.standalone.types.{ArrayType, DataType, MapType, StructField, StructType}

import io.delta.standalone.internal.exception.DeltaErrors

private[standalone] object SchemaUtils {

  /**
   * Verifies that the column names are acceptable by Parquet and henceforth Delta. Parquet doesn't
   * accept the characters ' ,;{}()\n\t'. We ensure that neither the data columns nor the partition
   * columns have these characters.
   */
  def checkFieldNames(names: Seq[String]): Unit = {
    ParquetSchemaConverter.checkFieldNames(names)

    // The method checkFieldNames doesn't have a valid regex to search for '\n'. That should be
    // fixed in Apache Spark, and we can remove this additional check here.
    names.find(_.contains("\n")).foreach(col => throw DeltaErrors.invalidColumnName(col))
  }

  /**
   * Go through the schema to look for unenforceable NOT NULL constraints and throw when they're
   * encountered.
   */
  def checkUnenforceableNotNullConstraints(schema: StructType): Unit = {
    def checkField(path: Seq[String], f: StructField): Unit = f.getDataType match {
      case a: ArrayType => if (!matchesNullableType(a.getElementType)) {
        throw DeltaErrors.nestedNotNullConstraint(
          prettyFieldName(path :+ f.getName), a.getElementType, nestType = "element")
      }
      case m: MapType =>
        val keyTypeNullable = matchesNullableType(m.getKeyType)
        val valueTypeNullable = matchesNullableType(m.getValueType)

        if (!keyTypeNullable) {
          throw DeltaErrors.nestedNotNullConstraint(
            prettyFieldName(path :+ f.getName), m.getKeyType, nestType = "key")
        }
        if (!valueTypeNullable) {
          throw DeltaErrors.nestedNotNullConstraint(
            prettyFieldName(path :+ f.getName), m.getValueType, nestType = "value")
        }
      case _ => // nothing
    }

    def traverseColumns[E <: DataType](path: Seq[String], dt: E): Unit = dt match {
      case s: StructType =>
        s.getFields.foreach { field =>
          checkField(path, field)
          traverseColumns(path :+ field.getName, field.getDataType)
        }
      case a: ArrayType =>
        traverseColumns(path :+ "element", a.getElementType)
      case m: MapType =>
        traverseColumns(path :+ "key", m.getKeyType)
        traverseColumns(path :+ "value", m.getValueType)
      case _ => // nothing
    }

    traverseColumns(Seq.empty, schema)
  }

  /**
   * As the Delta table updates, the schema may change as well. This method defines whether a new
   * schema can replace a pre-existing schema of a Delta table. Our rules are to return false if
   * the new schema:
   *   - Drops any column that is present in the current schema
   *   - Converts nullable=true to nullable=false for any column
   *   - Changes any datatype
   */
  def isWriteCompatible(existingSchema: StructType, newSchema: StructType): Boolean = {

    def isDatatypeWriteCompatible(_existingType: DataType, _newType: DataType): Boolean = {
      (_existingType, _newType) match {
        case (e: StructType, n: StructType) =>
          isWriteCompatible(e, n)
        case (e: ArrayType, n: ArrayType) =>
          // if existing elements are nullable, so should be the new element
          (!e.containsNull() || n.containsNull()) &&
            isDatatypeWriteCompatible(e.getElementType, n.getElementType)
        case (e: MapType, n: MapType) =>
          // if existing value is nullable, so should be the new value
          (!e.valueContainsNull || n.valueContainsNull) &&
            isDatatypeWriteCompatible(e.getKeyType, n.getKeyType) &&
            isDatatypeWriteCompatible(e.getValueType, n.getValueType)
        case (a, b) => a == b
      }
    }

    def isStructWriteCompatible(_existingSchema: StructType, _newSchema: StructType): Boolean = {
      val existing = toFieldMap(_existingSchema.getFields)
      // scalastyle:off caselocale
      val existingFieldNames = _existingSchema.getFieldNames.map(_.toLowerCase).toSet
      assert(existingFieldNames.size == _existingSchema.length,
        "Delta tables don't allow field names that only differ by case")
      val newFields = _newSchema.getFieldNames.map(_.toLowerCase).toSet
      assert(newFields.size == _newSchema.length,
        "Delta tables don't allow field names that only differ by case")
      // scalastyle:on caselocale

      if (!existingFieldNames.subsetOf(newFields)) {
        // Dropped a column that was present in the DataFrame schema
        return false
      }
      _newSchema.getFields.forall { newField =>
        // new fields are fine, they just won't be returned
        existing.get(newField.getName).forall { existingField =>
          // we know the name matches modulo case - now verify exact match
          (existingField.getName == newField.getName
            // if existing value is nullable, so should be the new value
            && (!existingField.isNullable || newField.isNullable)
            // and the type of the field must be compatible, too
            && isDatatypeWriteCompatible(existingField.getDataType, newField.getDataType))
        }
      }
    }

    isStructWriteCompatible(existingSchema, newSchema)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def toFieldMap(fields: Seq[StructField]): Map[String, StructField] = {
    CaseInsensitiveMap(fields.map(field => field.getName -> field).toMap)
  }

  /**
   * This is a simpler version of Delta OSS SchemaUtils::typeAsNullable. Instead of returning the
   * nullable DataType, returns true if the input `dt` matches the nullable DataType.
   */
  private def matchesNullableType(dt: DataType): Boolean = dt match {
    case s: StructType => s.getFields.forall { field =>
      field.isNullable && matchesNullableType(field.getDataType)
    }

    case a: ArrayType => a.getElementType match {
      case s: StructType =>
        a.containsNull() && matchesNullableType(s)
      case _ =>
        a.containsNull()
    }

    case m: MapType => (m.getKeyType, m.getValueType) match {
      case (s1: StructType, s2: StructType) =>
        m.valueContainsNull() && matchesNullableType(s1) && matchesNullableType(s2)
      case (s1: StructType, _) =>
        m.valueContainsNull() && matchesNullableType(s1)
      case (_, s2: StructType) =>
        m.valueContainsNull() && matchesNullableType(s2)
      case _ => true
    }

    case _ => true
  }

  private def prettyFieldName(columnPath: Seq[String]): String =
    columnPath.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  private object ParquetSchemaConverter {
    def checkFieldNames(names: Seq[String]): Unit = {
      names.foreach(checkFieldName)
    }

    def checkFieldName(name: String): Unit = {
      // ,;{}()\n\t= and space are special characters in Parquet schema
      checkConversionRequirement(
        !name.matches(".*[ ,;{}()\n\t=].*"),
        s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
           |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
    }

    def checkConversionRequirement(f: => Boolean, message: String): Unit = {
      if (!f) {
        throw new DeltaStandaloneException(message)
      }
    }
  }
}
