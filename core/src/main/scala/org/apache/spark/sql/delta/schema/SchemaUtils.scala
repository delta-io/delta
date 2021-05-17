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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale

import scala.collection.Set._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaErrors, GeneratedColumn}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Resolver, TypeCoercion, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object SchemaUtils {
  // We use case insensitive resolution while writing into Delta
  val DELTA_COL_RESOLVER: (String, String) => Boolean =
    org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
  private val ARRAY_ELEMENT_INDEX = 0
  private val MAP_KEY_INDEX = 0
  private val MAP_VALUE_INDEX = 1

  /**
   * Finds `StructField`s that match a given check `f`. Returns the path to the column, and the
   * field.
   *
   * @param checkComplexTypes While `StructType` is also a complex type, since we're returning
   *                          StructFields, we definitely recurse into StructTypes. This flag
   *                          defines whether we should recurse into ArrayType and MapType.
   */
  def filterRecursively(
      schema: StructType,
      checkComplexTypes: Boolean)(f: StructField => Boolean): Seq[(Seq[String], StructField)] = {
    def recurseIntoComplexTypes(
        complexType: DataType,
        columnStack: Seq[String]): Seq[(Seq[String], StructField)] = complexType match {
      case s: StructType =>
        s.fields.flatMap { sf =>
          val includeLevel = if (f(sf)) Seq((columnStack, sf)) else Nil
          includeLevel ++ recurseIntoComplexTypes(sf.dataType, columnStack :+ sf.name)
        }
      case a: ArrayType if checkComplexTypes => recurseIntoComplexTypes(a.elementType, columnStack)
      case m: MapType if checkComplexTypes =>
        recurseIntoComplexTypes(m.keyType, columnStack :+ "key") ++
          recurseIntoComplexTypes(m.valueType, columnStack :+ "value")
      case _ => Nil
    }

    recurseIntoComplexTypes(schema, Nil)
  }

  /** Copied over from DataType for visibility reasons. */
  def typeExistsRecursively(dt: DataType)(f: DataType => Boolean): Boolean = dt match {
    case s: StructType =>
      f(s) || s.fields.exists(field => typeExistsRecursively(field.dataType)(f))
    case a: ArrayType =>
      f(a) || typeExistsRecursively(a.elementType)(f)
    case m: MapType =>
      f(m) || typeExistsRecursively(m.keyType)(f) || typeExistsRecursively(m.valueType)(f)
    case other =>
      f(other)
  }

  /** Turns the data types to nullable in a recursive manner for nested columns. */
  def typeAsNullable(dt: DataType): DataType = dt match {
    case s: StructType => s.asNullable
    case a @ ArrayType(s: StructType, _) => a.copy(s.asNullable, containsNull = true)
    case a: ArrayType => a.copy(containsNull = true)
    case m @ MapType(s1: StructType, s2: StructType, _) =>
      m.copy(s1.asNullable, s2.asNullable, valueContainsNull = true)
    case m @ MapType(s1: StructType, _, _) =>
      m.copy(keyType = s1.asNullable, valueContainsNull = true)
    case m @ MapType(_, s2: StructType, _) =>
      m.copy(valueType = s2.asNullable, valueContainsNull = true)
    case other => other
  }

  /**
   * Drops null types from the DataFrame if they exist. We don't have easy ways of generating types
   * such as MapType and ArrayType, therefore if these types contain NullType in their elements,
   * we will throw an AnalysisException.
   */
  def dropNullTypeColumns(df: DataFrame): DataFrame = {
    val schema = df.schema
    if (!typeExistsRecursively(schema)(_.isInstanceOf[NullType])) return df
    def generateSelectExpr(sf: StructField, nameStack: Seq[String]): Column = sf.dataType match {
      case st: StructType =>
        val nested = st.fields.flatMap { f =>
          if (f.dataType.isInstanceOf[NullType]) {
            None
          } else {
            Some(generateSelectExpr(f, nameStack :+ sf.name))
          }
        }
        struct(nested: _*).alias(sf.name)
      case a: ArrayType if typeExistsRecursively(a)(_.isInstanceOf[NullType]) =>
        val colName = UnresolvedAttribute.apply(nameStack :+ sf.name).name
        throw new AnalysisException(
          s"Found nested NullType in column $colName which is of ArrayType. Delta doesn't " +
            "support writing NullType in complex types.")
      case m: MapType if typeExistsRecursively(m)(_.isInstanceOf[NullType]) =>
        val colName = UnresolvedAttribute.apply(nameStack :+ sf.name).name
        throw new AnalysisException(
          s"Found nested NullType in column $colName which is of MapType. Delta doesn't " +
            "support writing NullType in complex types.")
      case _ =>
        val colName = UnresolvedAttribute.apply(nameStack :+ sf.name).name
        col(colName).alias(sf.name)
    }

    val selectExprs = schema.flatMap { f =>
      if (f.dataType.isInstanceOf[NullType]) None else Some(generateSelectExpr(f, Nil))
    }
    df.select(selectExprs: _*)
  }

  /**
   * Drops null types from the schema if they exist. We do not recurse into Array and Map types,
   * because we do not expect null types to exist in those columns, as Delta doesn't allow it during
   * writes.
   */
  def dropNullTypeColumns(schema: StructType): StructType = {
    def recurseAndRemove(struct: StructType): Seq[StructField] = {
      struct.flatMap {
        case sf @ StructField(_, s: StructType, _, _) =>
          Some(sf.copy(dataType = StructType(recurseAndRemove(s))))
        case StructField(_, n: NullType, _, _) => None
        case other => Some(other)
      }
    }
    StructType(recurseAndRemove(schema))
  }

  /**
   * Returns all column names in this schema as a flat list. For example, a schema like:
   *   | - a
   *   | | - 1
   *   | | - 2
   *   | - b
   *   | - c
   *   | | - nest
   *   |   | - 3
   *   will get flattened to: "a", "a.1", "a.2", "b", "c", "c.nest", "c.nest.3"
   */
  def explodeNestedFieldNames(schema: StructType): Seq[String] = {
    def explode(schema: StructType): Seq[Seq[String]] = {
      def recurseIntoComplexTypes(complexType: DataType): Seq[Seq[String]] = {
        complexType match {
          case s: StructType => explode(s)
          case a: ArrayType => recurseIntoComplexTypes(a.elementType)
          case m: MapType =>
            recurseIntoComplexTypes(m.keyType).map(Seq("key") ++ _) ++
              recurseIntoComplexTypes(m.valueType).map(Seq("value") ++ _)
          case _ => Nil
        }
      }

      schema.flatMap {
        case StructField(name, s: StructType, _, _) =>
          Seq(Seq(name)) ++ explode(s).map(nested => Seq(name) ++ nested)
        case StructField(name, a: ArrayType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(a).map(nested => Seq(name) ++ nested)
        case StructField(name, m: MapType, _, _) =>
          Seq(Seq(name)) ++ recurseIntoComplexTypes(m).map(nested => Seq(name) ++ nested)
        case f => Seq(f.name) :: Nil
      }
    }

    explode(schema).map(UnresolvedAttribute.apply(_).name)
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param schema the schema to check for duplicates
   * @param colType column type name, used in an exception message
   */
  def checkColumnNameDuplication(schema: StructType, colType: String): Unit = {
    val columnNames = explodeNestedFieldNames(schema)
    // scalastyle:off caselocale
    val names = columnNames.map(_.toLowerCase)
    // scalastyle:on caselocale
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"$x"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }

  /**
   * Rewrite the query field names according to the table schema. This method assumes that all
   * schema validation checks have been made and this is the last operation before writing into
   * Delta.
   */
  def normalizeColumnNames(baseSchema: StructType, data: Dataset[_]): DataFrame = {
    val dataSchema = data.schema
    val dataFields = explodeNestedFieldNames(dataSchema).toSet
    val tableFields = explodeNestedFieldNames(baseSchema).toSet
    if (dataFields.subsetOf(tableFields)) {
      data.toDF()
    } else {
      // Check that nested columns don't need renaming. We can't handle that right now
      val topLevelDataFields = dataFields.map(UnresolvedAttribute.parseAttributeName(_).head)
      if (topLevelDataFields.subsetOf(tableFields)) {
        val columnsThatNeedRenaming = dataFields -- tableFields
        throw new AnalysisException("Nested fields need renaming to avoid data loss. " +
          s"Fields:\n${columnsThatNeedRenaming.mkString("[", ", ", "]")}.\n" +
          s"Original schema:\n${baseSchema.treeString}")
      }

      val baseFields = toFieldMap(baseSchema)
      val aliasExpressions = dataSchema.map { field =>
        val originalCase: String = baseFields.get(field.name) match {
          case Some(original) => original.name
          case None =>
            throw new AnalysisException(
              s"Can't resolve column ${field.name} in ${baseSchema.treeString}")
        }
        if (originalCase != field.name) {
          fieldToColumn(field).as(originalCase)
        } else {
          fieldToColumn(field)
        }
      }
      data.select(aliasExpressions: _*)
    }
  }

  /**
   * As the Delta snapshots update, the schema may change as well. This method defines whether the
   * new schema of a Delta table can be used with a previously analyzed LogicalPlan. Our
   * rules are to return false if:
   *   - Dropping any column that was present in the DataFrame schema
   *   - Converting nullable=false to nullable=true for any column
   *   - Any change of datatype
   */
  def isReadCompatible(existingSchema: StructType, readSchema: StructType): Boolean = {

    def isDatatypeReadCompatible(existing: DataType, newtype: DataType): Boolean = {
      (existing, newtype) match {
        case (e: StructType, n: StructType) =>
          isReadCompatible(e, n)
        case (e: ArrayType, n: ArrayType) =>
          // if existing elements are non-nullable, so should be the new element
          (e.containsNull || !n.containsNull) &&
            isDatatypeReadCompatible(e.elementType, n.elementType)
        case (e: MapType, n: MapType) =>
          // if existing value is non-nullable, so should be the new value
          (e.valueContainsNull || !n.valueContainsNull) &&
            isDatatypeReadCompatible(e.keyType, n.keyType) &&
            isDatatypeReadCompatible(e.valueType, n.valueType)
        case (a, b) => a == b
      }
    }

    def isStructReadCompatible(existing: StructType, newtype: StructType): Boolean = {
      val existing = toFieldMap(existingSchema)
      // scalastyle:off caselocale
      val existingFieldNames = existingSchema.fieldNames.map(_.toLowerCase).toSet
      assert(existingFieldNames.size == existingSchema.length,
        "Delta tables don't allow field names that only differ by case")
      val newFields = readSchema.fieldNames.map(_.toLowerCase).toSet
      assert(newFields.size == readSchema.length,
        "Delta tables don't allow field names that only differ by case")
      // scalastyle:on caselocale

      if (!existingFieldNames.subsetOf(newFields)) {
        // Dropped a column that was present in the DataFrame schema
        return false
      }
      readSchema.forall { newField =>
        // new fields are fine, they just won't be returned
        existing.get(newField.name).forall { existingField =>
          // we know the name matches modulo case - now verify exact match
          (existingField.name == newField.name
            // if existing value is non-nullable, so should be the new value
            && (existingField.nullable || !newField.nullable)
            // and the type of the field must be compatible, too
            && isDatatypeReadCompatible(existingField.dataType, newField.dataType))
        }
      }
    }

    isStructReadCompatible(existingSchema, readSchema)
  }

  /**
   * Compare an existing schema to a specified new schema and
   * return a message describing the first difference found, if any:
   *   - different field name or datatype
   *   - different metadata
   */
  def reportDifferences(existingSchema: StructType, specifiedSchema: StructType): Seq[String] = {

    def canOrNot(can: Boolean) = if (can) "can" else "can not"
    def isOrNon(b: Boolean) = if (b) "" else "non-"

    def missingFieldsMessage(fields: Set[String]) : String = {
      s"Specified schema is missing field(s): ${fields.mkString(", ")}"
    }
    def additionalFieldsMessage(fields: Set[String]) : String = {
      s"Specified schema has additional field(s): ${fields.mkString(", ")}"
    }
    def fieldNullabilityMessage(field: String, specified: Boolean, existing: Boolean) : String = {
      s"Field $field is ${isOrNon(specified)}nullable in specified " +
        s"schema but ${isOrNon(existing)}nullable in existing schema."
    }
    def arrayNullabilityMessage(field: String, specified: Boolean, existing: Boolean) : String = {
      s"Array field $field ${canOrNot(specified)} contain null in specified schema " +
        s"but ${canOrNot(existing)} in existing schema"
    }
    def valueNullabilityMessage(field: String, specified: Boolean, existing: Boolean) : String = {
      s"Map field $field ${canOrNot(specified)} contain null values in specified schema " +
        s"but ${canOrNot(existing)} in existing schema"
    }
    def removeGenerationExpressionMetadata(metadata: Metadata): Metadata = {
      new MetadataBuilder()
        .withMetadata(metadata)
        .remove(GENERATION_EXPRESSION_METADATA_KEY)
        .build()
    }
    def metadataDifferentMessage(field: String, specified: Metadata, existing: Metadata)
      : String = {
      val specifiedGenerationExpr = GeneratedColumn.getGenerationExpressionStr(specified)
      val existingGenerationExpr = GeneratedColumn.getGenerationExpressionStr(existing)
      var metadataDiffMessage = ""
      if (specifiedGenerationExpr != existingGenerationExpr) {
        metadataDiffMessage +=
          s"""Specified generation expression for field $field is different from existing schema:
             |Specified: ${specifiedGenerationExpr.getOrElse("")}
             |Existing:  ${existingGenerationExpr.getOrElse("")}""".stripMargin
      }
      val specifiedMetadataWithoutGenerationExpr = removeGenerationExpressionMetadata(specified)
      val existingMetadataWithoutGenerationExpr = removeGenerationExpressionMetadata(existing)
      if (specifiedMetadataWithoutGenerationExpr != existingMetadataWithoutGenerationExpr) {
        if (metadataDiffMessage.nonEmpty) metadataDiffMessage += "\n"
        metadataDiffMessage +=
          s"""Specified metadata for field $field is different from existing schema:
             |Specified: $specifiedMetadataWithoutGenerationExpr
             |Existing:  $existingMetadataWithoutGenerationExpr""".stripMargin
      }
      metadataDiffMessage
    }
    def typeDifferenceMessage(field: String, specified: DataType, existing: DataType)
      : String = {
      s"""Specified type for $field is different from existing schema:
         |Specified: ${specified.typeName}
         |Existing:  ${existing.typeName}""".stripMargin
    }

    // prefix represents the nested field(s) containing this schema
    def structDifference(existing: StructType, specified: StructType, prefix: String)
      : Seq[String] = {

      // 1. ensure set of fields is the same
      val existingFieldNames = existing.fieldNames.toSet
      val specifiedFieldNames = specified.fieldNames.toSet

      val missingFields = existingFieldNames diff specifiedFieldNames
      val missingFieldsDiffs =
        if (missingFields.isEmpty) Nil
        else Seq(missingFieldsMessage(missingFields.map(prefix + _)))

      val extraFields = specifiedFieldNames diff existingFieldNames
      val extraFieldsDiffs =
        if (extraFields.isEmpty) Nil
        else Seq(additionalFieldsMessage(extraFields.map(prefix + _)))

      // 2. for each common field, ensure it has the same type and metadata
      val existingFields = toFieldMap(existing)
      val specifiedFields = toFieldMap(specified)
      val fieldsDiffs = (existingFieldNames intersect specifiedFieldNames).flatMap(
        (name: String) => fieldDifference(existingFields(name), specifiedFields(name), prefix))

      missingFieldsDiffs ++ extraFieldsDiffs ++ fieldsDiffs
    }

    def fieldDifference(existing: StructField, specified: StructField, prefix: String)
      : Seq[String] = {

      val name = s"$prefix${existing.name}"
      val nullabilityDiffs =
        if (existing.nullable == specified.nullable) Nil
        else Seq(fieldNullabilityMessage(s"$name", specified.nullable, existing.nullable))
      val metadataDiffs =
        if (existing.metadata == specified.metadata) Nil
        else Seq(metadataDifferentMessage(s"$name", specified.metadata, existing.metadata))
      val typeDiffs =
        typeDifference(existing.dataType, specified.dataType, name)

      nullabilityDiffs ++ metadataDiffs ++ typeDiffs
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
        typeDifference(existing.elementType, specified.elementType, field)
      val nullabilityDiffs =
        if (existing.containsNull == specified.containsNull) Nil
        else Seq(arrayNullabilityMessage(field, specified.containsNull, existing.containsNull))

      elementDiffs ++ nullabilityDiffs
    }

    def mapDifference(existing: MapType, specified: MapType, field: String) : Seq[String] = {

      val keyDiffs =
        typeDifference(existing.keyType, specified.keyType, s"$field[key]")
      val valueDiffs =
        typeDifference(existing.valueType, specified.valueType, s"$field[value]")
      val nullabilityDiffs =
        if (existing.valueContainsNull == specified.valueContainsNull) Nil
        else Seq(
          valueNullabilityMessage(field, specified.valueContainsNull, existing.valueContainsNull))

      keyDiffs ++ valueDiffs ++ nullabilityDiffs
    }

    structDifference(existingSchema, specifiedSchema, "")
  }

  /**
   * Returns the given column's ordinal within the given `schema` and the size of the last schema
   * size. The length of the returned position will be as long as how nested the column is.
   *
   * For ArrayType: accessing the array's element adds a position 0 to the position list.
   * e.g. accessing a.element.y would have the result -> Seq(..., positionOfA, 0, positionOfY)
   *
   * For MapType: accessing the map's key adds a position 0 to the position list.
   * e.g. accessing m.key.y would have the result -> Seq(..., positionOfM, 0, positionOfY)
   *
   * For MapType: accessing the map's value adds a position 1 to the position list.
   * e.g. accessing m.key.y would have the result -> Seq(..., positionOfM, 1, positionOfY)
   *
   * @param column The column to search for in the given struct. If the length of `column` is
   *               greater than 1, we expect to enter a nested field.
   * @param schema The current struct we are looking at.
   * @param resolver The resolver to find the column.
   */
  def findColumnPosition(
      column: Seq[String],
      schema: StructType,
      resolver: Resolver = DELTA_COL_RESOLVER): (Seq[Int], Int) = {
    def find(column: Seq[String], schema: StructType, stack: Seq[String]): (Seq[Int], Int) = {
      if (column.isEmpty) return (Nil, schema.size)
      val thisCol = column.head
      lazy val columnPath = UnresolvedAttribute(stack :+ thisCol).name
      val pos = schema.indexWhere(f => resolver(f.name, thisCol))
      if (pos == -1) {
        throw new IndexOutOfBoundsException(columnPath)
      }
      val colTail = column.tail
      val (children, lastSize) = (colTail, schema(pos).dataType) match {
        case (_, s: StructType) =>
          find(colTail, s, stack :+ thisCol)
        case (Seq("element", _ @ _*), ArrayType(s: StructType, _)) =>
          val (child, size) = find(colTail.tail, s, stack :+ thisCol)
          (ARRAY_ELEMENT_INDEX +: child, size)
        case (Seq(), ArrayType(s: StructType, _)) =>
          find(colTail, s, stack :+ thisCol)
        case (Seq(), ArrayType(_, _)) =>
          (Seq(0), 0)
        case (_, ArrayType(_, _)) =>
          throw new AnalysisException(
            s"""An ArrayType was found. In order to access elements of an ArrayType, specify
               |${prettyFieldName(stack ++ Seq(thisCol, "element"))}
               |Instead of ${prettyFieldName(stack ++ Seq(thisCol))}
               """.stripMargin
          )
        case (Seq(), MapType(_, _, _)) =>
          (Nil, 2)
        case (Seq("key", _ @ _*), MapType(keyType: StructType, _, _)) =>
          val (child, size) = find(colTail.tail, keyType, stack :+ thisCol)
          (MAP_KEY_INDEX +: child, size)
        case (Seq("key"), MapType(_, _, _)) =>
          (Seq(MAP_KEY_INDEX), 0)
        case (Seq("value", _ @ _*), MapType(_, valueType: StructType, _)) =>
          val (child, size) = find(colTail.tail, valueType, stack :+ thisCol)
          (MAP_VALUE_INDEX +: child, size)
        case (Seq("value"), MapType(_, _, _)) =>
          (Seq(MAP_VALUE_INDEX), 0)
        case (_, MapType(_, _, _)) =>
          throw new AnalysisException(
            s"""A MapType was found. In order to access the key or value of a MapType, specify one
               |of:
               |${prettyFieldName(stack ++ Seq(thisCol, "key"))} or
               |${prettyFieldName(stack ++ Seq(thisCol, "value"))}
               |followed by the name of the column (only if that column is a struct type).
               |e.g. mymap.key.mykey
               |If the column is a basic type, mymap.key or mymap.value is sufficient.
              """.stripMargin
          )
        case (_, o) =>
          if (column.length > 1) {
            throw new AnalysisException(
              s"""Expected $columnPath to be a nested data type, but found $o. Was looking for the
                 |index of ${prettyFieldName(column)} in a nested field
              """.stripMargin)
          }
          (Nil, 0)
      }
      (Seq(pos) ++ children, lastSize)
    }

    try {
      find(column, schema, Nil)
    } catch {
      case i: IndexOutOfBoundsException =>
        throw DeltaErrors.columnNotInSchemaException(i.getMessage, schema)
      case e: AnalysisException =>
        throw new AnalysisException(e.getMessage + s":\n${schema.treeString}")
    }
  }

  /**
   * Pretty print the column path passed in.
   */
  def prettyFieldName(columnPath: Seq[String]): String = {
    UnresolvedAttribute(columnPath).name
  }

  /**
   * Add `column` to the specified `position` in `schema`.
   * @param position A Seq of ordinals on where this column should go. It is a Seq to denote
   *                 positions in nested columns (0-based). For example:
   *
   *                 tableSchema: <a:STRUCT<a1,a2,a3>, b,c:STRUCT<c1,c3>>
   *                 column: c2
   *                 position: Seq(2, 1)
   *                 will return
   *                 result: <a:STRUCT<a1,a2,a3>, b,c:STRUCT<c1,**c2**,c3>>
   */
  def addColumn(schema: StructType, column: StructField, position: Seq[Int]): StructType = {
    require(position.nonEmpty, s"Don't know where to add the column $column")
    val slicePosition = position.head
    if (slicePosition < 0) {
      throw new AnalysisException(s"Index $slicePosition to add column $column is lower than 0")
    }
    val length = schema.length
    if (slicePosition > length) {
      throw new AnalysisException(
        s"Index $slicePosition to add column $column is larger than struct length: $length")
    }
    if (slicePosition == length) {
      if (position.length > 1) {
        throw new AnalysisException(s"Struct not found at position $slicePosition")
      }
      return StructType(schema :+ column)
    }
    val pre = schema.take(slicePosition)
    if (position.length > 1) {
      val posTail = position.tail
      val mid = schema(slicePosition) match {
        case StructField(name, f: StructType, nullable, metadata) =>
          if (!column.nullable && nullable) {
            throw new AnalysisException(
              "A non-nullable nested field can't be added to a nullable parent. Please set the " +
              "nullability of the parent column accordingly.")
          }
          StructField(
            name,
            addColumn(f, column, posTail),
            nullable,
            metadata)
        case StructField(name, ArrayType(f: StructType, containsNull), nullable, metadata) =>
          if (!column.nullable && nullable) {
            throw new AnalysisException(
              "A non-nullable nested field can't be added to a nullable parent. Please set the " +
                "nullability of the parent column accordingly.")
          }

          if (posTail.head != ARRAY_ELEMENT_INDEX) {
            throw new AnalysisException(
              s"""Incorrectly accessing an ArrayType. Use arrayname.element.elementname position to
                 |add to an array.
               """.stripMargin)
          }

          StructField(
            name,
            ArrayType(addColumn(f, column, posTail.tail), containsNull),
            nullable,
            metadata)
        case StructField(name, map @ MapType(_, _, _), nullable, metadata) =>
          if (!column.nullable && nullable) {
            throw new AnalysisException(
              "A non-nullable nested field can't be added to a nullable parent. Please set the " +
                "nullability of the parent column accordingly.")
          }

          val addedMap = (posTail.head, map) match {
            case (MAP_KEY_INDEX, MapType(key: StructType, v, nullability)) =>
              MapType(addColumn(key, column, posTail.tail), v, nullability)
            case (MAP_VALUE_INDEX, MapType(k, value: StructType, nullability)) =>
              MapType(k, addColumn(value, column, posTail.tail), nullability)
            case _ =>
              throw new AnalysisException(
                s"""
                  |Cannot add ${column.name} because its parent is not a StructType.
                """.stripMargin)
          }
          StructField(name, addedMap, nullable, metadata)
        case o =>
          throw new AnalysisException(s"Cannot add ${column.name} because its parent is not a " +
            s"StructType. Found ${o.dataType}")
      }
      StructType(pre ++ Seq(mid) ++ schema.slice(slicePosition + 1, length))
    } else {
      StructType(pre ++ Seq(column) ++ schema.slice(slicePosition, length))
    }
  }

  /**
   * Drop from the specified `position` in `schema` and return with the original column.
   * @param position A Seq of ordinals on where this column should go. It is a Seq to denote
   *                 positions in nested columns (0-based). For example:
   *
   *                 tableSchema: <a:STRUCT<a1,a2,a3>, b,c:STRUCT<c1,c2,c3>>
   *                 position: Seq(2, 1)
   *                 will return
   *                 result: <a:STRUCT<a1,a2,a3>, b,c:STRUCT<c1,c3>>
   */
  def dropColumn(schema: StructType, position: Seq[Int]): (StructType, StructField) = {
    require(position.nonEmpty, "Don't know where to drop the column")
    val slicePosition = position.head
    if (slicePosition < 0) {
      throw new AnalysisException(s"Index $slicePosition to drop column is lower than 0")
    }
    val length = schema.length
    if (slicePosition >= length) {
      throw new AnalysisException(
        s"Index $slicePosition to drop column equals to or is larger than struct length: $length")
    }
    val pre = schema.take(slicePosition)
    if (position.length > 1) {
      val (mid, original) = schema(slicePosition) match {
        case StructField(name, f: StructType, nullable, metadata) =>
          val (dropped, original) = dropColumn(f, position.tail)
          (StructField(name, dropped, nullable, metadata), original)
        case o =>
          throw new AnalysisException(s"Can only drop nested columns from StructType. Found: $o")
      }
      (StructType(pre ++ Seq(mid) ++ schema.slice(slicePosition + 1, length)), original)
    } else {
      (StructType(pre ++ schema.slice(slicePosition + 1, length)), schema(slicePosition))
    }
  }

  /**
   * Check if the two data types can be changed.
   *
   * @return None if the data types can be changed, otherwise Some(err) containing the reason.
   */
  def canChangeDataType(
      from: DataType,
      to: DataType,
      resolver: Resolver,
      columnPath: Seq[String] = Seq.empty): Option[String] = {
    def verify(cond: Boolean, err: => String): Unit = {
      if (!cond) {
        throw new AnalysisException(err)
      }
    }

    def verifyNullability(fn: Boolean, tn: Boolean, columnPath: Seq[String]): Unit = {
      verify(tn || !fn, s"tightening nullability of ${UnresolvedAttribute(columnPath).name}")
    }

    def check(fromDt: DataType, toDt: DataType, columnPath: Seq[String]): Unit = {
      (fromDt, toDt) match {
        case (ArrayType(fromElement, fn), ArrayType(toElement, tn)) =>
          verifyNullability(fn, tn, columnPath)
          check(fromElement, toElement, columnPath)

        case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
          verifyNullability(fn, tn, columnPath)
          check(fromKey, toKey, columnPath :+ "key")
          check(fromValue, toValue, columnPath :+ "value")

        case (StructType(fromFields), StructType(toFields)) =>
          val remainingFields = fromFields.to[mutable.Set]
          toFields.foreach { toField =>
            fromFields.find(field => resolver(field.name, toField.name)) match {
              case Some(fromField) =>
                remainingFields -= fromField

                val newPath = columnPath :+ fromField.name
                verifyNullability(fromField.nullable, toField.nullable, newPath)
                check(fromField.dataType, toField.dataType, newPath)
              case None =>
                verify(toField.nullable,
                  "adding non-nullable column " +
                  UnresolvedAttribute(columnPath :+ toField.name).name)
            }
          }
          verify(remainingFields.isEmpty,
            s"dropping column(s) [${remainingFields.map(_.name).mkString(", ")}]" +
            (if (columnPath.nonEmpty) s" from ${UnresolvedAttribute(columnPath).name}" else ""))

        case (fromDataType, toDataType) =>
          verify(fromDataType == toDataType,
            s"changing data type of ${UnresolvedAttribute(columnPath).name} " +
            s"from $fromDataType to $toDataType")
      }
    }

    try {
      check(from, to, columnPath)
      None
    } catch {
      case e: AnalysisException =>
        Some(e.message)
    }
  }

  /**
   * Copy the nested data type between two data types.
   */
  def changeDataType(from: DataType, to: DataType, resolver: Resolver): DataType = {
    (from, to) match {
      case (ArrayType(fromElement, fn), ArrayType(toElement, _)) =>
        ArrayType(changeDataType(fromElement, toElement, resolver), fn)

      case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, _)) =>
        MapType(
          changeDataType(fromKey, toKey, resolver),
          changeDataType(fromValue, toValue, resolver),
          fn)

      case (StructType(fromFields), StructType(toFields)) =>
        StructType(
          toFields.map { toField =>
            fromFields.find(field => resolver(field.name, toField.name)).map { fromField =>
              toField.getComment().map(fromField.withComment).getOrElse(fromField)
                .copy(
                  dataType = changeDataType(fromField.dataType, toField.dataType, resolver),
                  nullable = toField.nullable)
            }.getOrElse(toField)
          }
        )

      case (_, toDataType) => toDataType
    }
  }

  /**
   * Check whether we can write to the Delta table, which has `tableSchema`, using a query that has
   * `dataSchema`. Our rules are that:
   *   - `dataSchema` may be missing columns or have additional columns
   *   - We don't trust the nullability in `dataSchema`. Assume fields are nullable.
   *   - We only allow nested StructType expansions. For all other complex types, we check for
   *     strict equality
   *   - `dataSchema` can't have duplicate column names. Columns that only differ by case are also
   *     not allowed.
   * The following merging strategy is
   * applied:
   *  - The name of the current field is used.
   *  - The data types are merged by calling this function.
   *  - We respect the current field's nullability.
   *  - The metadata is current field's metadata.
   *
   * Schema merging occurs in a case insensitive manner. Hence, column names that only differ
   * by case are not accepted in the `dataSchema`.
   *
   * @param tableSchema The current schema of the table.
   * @param dataSchema The schema of the new data being written.
   * @param allowImplicitConversions Whether to allow Spark SQL implicit conversions. By default,
   *                                 we merge according to Parquet write compatibility - for
   *                                 example, an integer type data field will throw when merged to a
   *                                 string type table field, because int and string aren't stored
   *                                 the same way in Parquet files. With this flag enabled, the
   *                                 merge will succeed, because once we get to write time Spark SQL
   *                                 will support implicitly converting the int to a string.
   * @param keepExistingType Whether to keep existing types instead of trying to merge types.
   * @param fixedTypeColumns The set of columns whose type should not be changed in any case.
   */
  def mergeSchemas(
      tableSchema: StructType,
      dataSchema: StructType,
      allowImplicitConversions: Boolean = false,
      keepExistingType: Boolean = false,
      fixedTypeColumns: Set[String] = Set.empty): StructType = {
    checkColumnNameDuplication(dataSchema, "in the data to save")
    def merge(
        current: DataType,
        update: DataType,
        fixedTypeColumnsSet: Set[String] = Set.empty): DataType = {
      (current, update) match {
        case (StructType(currentFields), StructType(updateFields)) =>
          // Merge existing fields.
          val updateFieldMap = toFieldMap(updateFields)
          val updatedCurrentFields = currentFields.map { currentField =>
            updateFieldMap.get(currentField.name) match {
              case Some(updateField) =>
                if (fixedTypeColumnsSet.contains(currentField.name.toLowerCase(Locale.ROOT)) &&
                    currentField.dataType != updateField.dataType) {
                  throw DeltaErrors.generatedColumnsUpdateColumnType(currentField, updateField)
                }
                try {
                  StructField(
                    currentField.name,
                    merge(currentField.dataType, updateField.dataType),
                    currentField.nullable,
                    currentField.metadata)
                } catch {
                  case NonFatal(e) =>
                    throw new AnalysisException(s"Failed to merge fields '${currentField.name}' " +
                        s"and '${updateField.name}'. " + e.getMessage)
                }
              case None =>
                // Retain the old field.
                currentField
            }
          }

          // Identify the newly added fields.
          val nameToFieldMap = toFieldMap(currentFields)
          val newFields = updateFields.filterNot(f => nameToFieldMap.contains(f.name))

          // Create the merged struct, the new fields are appended at the end of the struct.
          StructType(updatedCurrentFields ++ newFields)
        case (ArrayType(currentElementType, currentContainsNull),
              ArrayType(updateElementType, _)) =>
          ArrayType(
            merge(currentElementType, updateElementType),
            currentContainsNull)
        case (MapType(currentKeyType, currentElementType, currentContainsNull),
              MapType(updateKeyType, updateElementType, _)) =>
          MapType(
            merge(currentKeyType, updateKeyType),
            merge(currentElementType, updateElementType),
            currentContainsNull)

        // Simply keeps the existing type for primitive types
        case (current, update) if keepExistingType => current

        // If implicit conversions are allowed, that means we can use any valid implicit cast to
        // perform the merge.
        case (current, update)
            if allowImplicitConversions && typeForImplicitCast(update, current).isDefined =>
          typeForImplicitCast(update, current).get

        case (DecimalType.Fixed(leftPrecision, leftScale),
              DecimalType.Fixed(rightPrecision, rightScale)) =>
          if ((leftPrecision == rightPrecision) && (leftScale == rightScale)) {
            current
          } else if ((leftPrecision != rightPrecision) && (leftScale != rightScale)) {
            throw new AnalysisException("Failed to merge decimal types with incompatible " +
              s"precision $leftPrecision and $rightPrecision & scale $leftScale and $rightScale")
          } else if (leftPrecision != rightPrecision) {
            throw new AnalysisException("Failed to merge decimal types with incompatible " +
              s"precision $leftPrecision and $rightPrecision")
          } else {
            throw new AnalysisException("Failed to merge decimal types with incompatible " +
              s"scale $leftScale and $rightScale")
          }
        case _ if current == update =>
          current

        // Parquet physically stores ByteType, ShortType and IntType as IntType, so when a parquet
        // column is of one of these three types, you can read this column as any of these three
        // types. Since Parquet doesn't complain, we should also allow upcasting among these
        // three types when merging schemas.
        case (ByteType, ShortType) => ShortType
        case (ByteType, IntegerType) => IntegerType

        case (ShortType, ByteType) => ShortType
        case (ShortType, IntegerType) => IntegerType

        case (IntegerType, ShortType) => IntegerType
        case (IntegerType, ByteType) => IntegerType

        case (NullType, _) =>
          update
        case (_, NullType) =>
          current
        case _ =>
          throw new AnalysisException(
            s"Failed to merge incompatible data types $current and $update")
      }
    }
    merge(tableSchema, dataSchema, fixedTypeColumns.map(_.toLowerCase(Locale.ROOT)))
      .asInstanceOf[StructType]
  }

  /**
   * Try to cast the source data type to the target type, returning the final type or None if
   * there's no valid cast.
   */
  private def typeForImplicitCast(sourceType: DataType, targetType: DataType): Option[DataType] = {
    TypeCoercion.ImplicitTypeCasts.implicitCast(Literal.default(sourceType), targetType)
      .map(_.dataType)
  }

  private def toFieldMap(fields: Seq[StructField]): Map[String, StructField] = {
    CaseInsensitiveMap(fields.map(field => field.name -> field).toMap)
  }

  /**
   * Transform (nested) columns in a schema.
   *
   * @param schema to transform.
   * @param tf function to apply.
   * @return the transformed schema.
   */
  def transformColumns(
      schema: StructType)(
      tf: (Seq[String], StructField, Resolver) => StructField): StructType = {
    def transform[E <: DataType](path: Seq[String], dt: E): E = {
      val newDt = dt match {
        case StructType(fields) =>
          StructType(fields.map { field =>
            val newField = tf(path, field, DELTA_COL_RESOLVER)
            newField.copy(dataType = transform(path :+ newField.name, newField.dataType))
          })
        case ArrayType(elementType, containsNull) =>
          ArrayType(transform(path, elementType), containsNull)
        case MapType(keyType, valueType, valueContainsNull) =>
          MapType(
            transform(path :+ "key", keyType),
            transform(path :+ "value", valueType),
            valueContainsNull)
        case other => other
      }
      newDt.asInstanceOf[E]
    }
    transform(Seq.empty, schema)
  }

  /**
   * Transform (nested) columns in a schema. Runs the transform function on all nested StructTypes
   *
   * @param schema to transform.
   * @param tf function to apply on the StructType.
   * @return the transformed schema.
   */
  def transformColumnsStructs(
      schema: StructType,
      colName: String)(
      tf: (Seq[String], StructType, Resolver) => Seq[StructField]): StructType = {
    def transform[E <: DataType](path: Seq[String], dt: E): E = {
      val newDt = dt match {
        case struct @ StructType(fields) =>
          val newFields = if (fields.exists(_.name == colName)) {
            tf(path, struct, DELTA_COL_RESOLVER)
          } else {
            fields.toSeq
          }

          StructType(newFields.map { field =>
            field.copy(dataType = transform(path :+ field.name, field.dataType))
          })
        case ArrayType(elementType, containsNull) =>
          ArrayType(transform(path :+ "element", elementType), containsNull)
        case MapType(keyType, valueType, valueContainsNull) =>
          MapType(
            transform(path :+ "key", keyType),
            transform(path :+ "value", valueType),
            valueContainsNull)
        case other => other
      }
      newDt.asInstanceOf[E]
    }
    transform(Seq.empty, schema)
  }

  /**
   * Transform (nested) columns in a schema using the given path and parameter pairs. The transform
   * function is only invoked when a field's path matches one of the input paths.
   *
   * @param schema to transform
   * @param input paths and parameter pairs. The paths point to fields we want to transform. The
   *              parameters will be passed to the transform function for a matching field.
   * @param tf function to apply per matched field. This function takes the field path, the field
   *           itself and the input names and payload pairs that matched the field name. It should
   *           return a new field.
   * @tparam E the type of the payload used for transforming fields.
   * @return the transformed schema.
   */
  def transformColumns[E](
      schema: StructType,
      input: Seq[(Seq[String], E)])(
      tf: (Seq[String], StructField, Seq[(Seq[String], E)]) => StructField): StructType = {
    // scalastyle:off caselocale
    val inputLookup = input.groupBy(_._1.map(_.toLowerCase))
    SchemaUtils.transformColumns(schema) { (path, field, resolver) =>
      // Find the parameters that match this field name.
      val fullPath = path :+ field.name
      val normalizedFullPath = fullPath.map(_.toLowerCase)
      val matches = inputLookup.get(normalizedFullPath).toSeq.flatMap {
        // Keep only the input name(s) that actually match the field name(s). Note
        // that the Map guarantees that the zipped sequences have the same size.
        _.filter(_._1.zip(fullPath).forall(resolver.tupled))
      }
      if (matches.nonEmpty) {
        tf(path, field, matches)
      } else {
        field
      }
    }
    // scalastyle:on caselocale
  }

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
   * Go through the schema to look for unenforceable NOT NULL constraints. By default we'll throw
   * when they're encountered, but if this is suppressed through SQLConf they'll just be silently
   * removed.
   *
   * Note that this should only be applied to schemas created from explicit user DDL - in other
   * scenarios, the nullability information may be inaccurate and Delta should always coerce the
   * nullability flag to true.
   */
  def removeUnenforceableNotNullConstraints(schema: StructType, conf: SQLConf): StructType = {
    val allowUnenforceableNotNulls =
      conf.getConf(DeltaSQLConf.ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS)

    def checkField(path: Seq[String], f: StructField, r: Resolver): StructField = f match {
      case StructField(name, ArrayType(elementType, containsNull), nullable, metadata) =>
        val nullableElementType = SchemaUtils.typeAsNullable(elementType)
        if (elementType != nullableElementType && !allowUnenforceableNotNulls) {
          throw DeltaErrors.nestedNotNullConstraint(
            prettyFieldName(path :+ f.name), elementType, nestType = "element")
        }
        StructField(
          name, ArrayType(nullableElementType, containsNull), nullable, metadata)

      case f @ StructField(
          name, MapType(keyType, valueType, containsNull), nullable, metadata) =>
        val nullableKeyType = SchemaUtils.typeAsNullable(keyType)
        val nullableValueType = SchemaUtils.typeAsNullable(valueType)

        if (keyType != nullableKeyType && !allowUnenforceableNotNulls) {
          throw DeltaErrors.nestedNotNullConstraint(
            prettyFieldName(path :+ f.name), keyType, nestType = "key")
        }
        if (valueType != nullableValueType && !allowUnenforceableNotNulls) {
          throw DeltaErrors.nestedNotNullConstraint(
            prettyFieldName(path :+ f.name), valueType, nestType = "value")
        }

        StructField(
          name,
          MapType(nullableKeyType, nullableValueType, containsNull),
          nullable,
          metadata)

      case s: StructField => s
    }

    SchemaUtils.transformColumns(schema)(checkField)
  }

  def fieldToColumn(field: StructField): Column = {
    col(UnresolvedAttribute.quoted(field.name).name)
  }

  // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
  // identifier with back-ticks.
  def quoteIdentifier(part: String): String = s"`${part.replace("`", "``")}`"
}
