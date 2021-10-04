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

package io.delta.standalone.internal.util

import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.types.{ArrayType, DataType, MapType, StructField, StructType}

private[internal] object SchemaUtils {

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

  private def prettyFieldName(columnPath: Seq[String]): String =
    columnPath.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  private object ParquetSchemaConverter {
    def checkFieldName(name: String): Unit = {
      // ,;{}()\n\t= and space are special characters in Parquet schema
      checkConversionRequirement(
        !name.matches(".*[ ,;{}()\n\t=].*"),
        s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
           |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
    }

    def checkFieldNames(names: Seq[String]): Unit = {
      names.foreach(checkFieldName)
    }

    def checkConversionRequirement(f: => Boolean, message: String): Unit = {
      if (!f) {
        // TODO: AnalysisException ?
        throw new RuntimeException(message)
      }
    }
  }
}
