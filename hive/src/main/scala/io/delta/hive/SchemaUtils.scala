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

package io.delta.hive

import io.delta.standalone.types.{ArrayType, DataType, MapType, StructField, StructType}

object SchemaUtils {

  /**
   * Compare an existing schema to a specified new schema and
   * return a message describing the first difference found, if any:
   *   - different field name or datatype
   *   - different metadata
   */
  def reportDifferences(existingSchema: StructType, specifiedSchema: StructType): Seq[String] = {

    def canOrNot(can: Boolean) = if (can) "can" else "can not"

    def isOrNon(b: Boolean) = if (b) "" else "non-"

    def missingFieldsMessage(fields: Set[String]): String = {
      s"Specified schema is missing field(s): ${fields.mkString(", ")}"
    }

    def additionalFieldsMessage(fields: Set[String]): String = {
      s"Specified schema has additional field(s): ${fields.mkString(", ")}"
    }

    def fieldNullabilityMessage(field: String, specified: Boolean, existing: Boolean): String = {
      s"Field $field is ${isOrNon(specified)}nullable in specified " +
        s"schema but ${isOrNon(existing)}nullable in existing schema."
    }

    def arrayNullabilityMessage(field: String, specified: Boolean, existing: Boolean): String = {
      s"Array field $field ${canOrNot(specified)} contain null in specified schema " +
        s"but ${canOrNot(existing)} in existing schema"
    }

    def valueNullabilityMessage(field: String, specified: Boolean, existing: Boolean): String = {
      s"Map field $field ${canOrNot(specified)} contain null values in specified schema " +
        s"but ${canOrNot(existing)} in existing schema"
    }

    def typeDifferenceMessage(field: String, specified: DataType, existing: DataType): String = {
      s"""Specified type for $field is different from existing schema:
         |Specified: ${specified.getTypeName}
         |Existing:  ${existing.getTypeName}""".stripMargin
    }

    // prefix represents the nested field(s) containing this schema
    def structDifference(existing: StructType, specified: StructType, prefix: String)
      : Seq[String] = {

      // 1. ensure set of fields is the same
      val existingFieldNames = existing.getFieldNames.toSet
      val specifiedFieldNames = specified.getFieldNames.toSet

      val missingFields = existingFieldNames diff specifiedFieldNames
      val missingFieldsDiffs =
        if (missingFields.isEmpty) Nil
        else Seq(missingFieldsMessage(missingFields.map(prefix + _)))

      val extraFields = specifiedFieldNames diff existingFieldNames
      val extraFieldsDiffs =
        if (extraFields.isEmpty) Nil
        else Seq(additionalFieldsMessage(extraFields.map(prefix + _)))

      // 2. ensure order of fields is the same
      val columnsOutOfOrder = missingFields.isEmpty && extraFields.isEmpty &&
        !existing.getFieldNames.sameElements(specified.getFieldNames)
      val columnsOutOfOrderMsg = if (columnsOutOfOrder) Seq("Columns out of order") else Nil

      // 3. for each common field, ensure it has the same type and metadata
      val existingFields = toFieldMap(existing.getFields)
      val specifiedFields = toFieldMap(specified.getFields)
      val fieldsDiffs = (existingFieldNames intersect specifiedFieldNames).flatMap(
        (name: String) => fieldDifference(existingFields(name), specifiedFields(name), prefix))

      missingFieldsDiffs ++ extraFieldsDiffs ++ fieldsDiffs ++ columnsOutOfOrderMsg
    }

    def fieldDifference(existing: StructField, specified: StructField, prefix: String)
      : Seq[String] = {

      val name = s"$prefix${existing.getName}"
      val nullabilityDiffs =
        if (existing.isNullable == specified.isNullable) Nil
        else Seq(fieldNullabilityMessage(s"$name", specified.isNullable, existing.isNullable))
      val typeDiffs =
        typeDifference(existing.getDataType, specified.getDataType, name)

      nullabilityDiffs ++ typeDiffs
    }

    def typeDifference(existing: DataType, specified: DataType, field: String)
      : Seq[String] = {

      (existing, specified) match {
        case (e: StructType, s: StructType) => structDifference(e, s, s"$field.")
        case (e: ArrayType, s: ArrayType) => arrayDifference(e, s, s"$field[]")
        case (e: MapType, s: MapType) => mapDifference(e, s, s"$field")
        case (e, s) if e != s => Seq(typeDifferenceMessage(field, s, e))
        case _ => Nil
      }
    }

    def arrayDifference(existing: ArrayType, specified: ArrayType, field: String): Seq[String] = {

      val elementDiffs =
        typeDifference(existing.getElementType, specified.getElementType, field)
      val nullabilityDiffs =
        if (existing.containsNull == specified.containsNull) Nil
        else Seq(arrayNullabilityMessage(field, specified.containsNull, existing.containsNull))

      elementDiffs ++ nullabilityDiffs
    }

    def mapDifference(existing: MapType, specified: MapType, field: String): Seq[String] = {

      val keyDiffs =
        typeDifference(existing.getKeyType, specified.getKeyType, s"$field[key]")
      val valueDiffs =
        typeDifference(existing.getValueType, specified.getValueType, s"$field[value]")
      val nullabilityDiffs =
        if (existing.valueContainsNull == specified.valueContainsNull) Nil
        else Seq(
          valueNullabilityMessage(field, specified.valueContainsNull, existing.valueContainsNull))

      keyDiffs ++ valueDiffs ++ nullabilityDiffs
    }

    structDifference(existingSchema, specifiedSchema, "")
  }

  private def toFieldMap(fields: Seq[StructField]): Map[String, StructField] = {
    CaseInsensitiveMap(fields.map(field => field.getName -> field).toMap)
  }
}
