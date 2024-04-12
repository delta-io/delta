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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaColumnMappingMode, DeltaErrors, DeltaLog, GeneratedColumn, NoMapping, TypeWidening}
import org.apache.spark.sql.delta.RowId
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaMergingUtils._
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

object SchemaUtils extends DeltaLogging {
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
      case a: ArrayType if checkComplexTypes =>
        recurseIntoComplexTypes(a.elementType, columnStack :+ "element")
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

  def findAnyTypeRecursively(dt: DataType)(f: DataType => Boolean): Option[DataType] = dt match {
    case s: StructType =>
      Some(s).filter(f).orElse(s.fields
          .find(field => findAnyTypeRecursively(field.dataType)(f).nonEmpty).map(_.dataType))
    case a: ArrayType =>
      Some(a).filter(f).orElse(findAnyTypeRecursively(a.elementType)(f))
    case m: MapType =>
      Some(m).filter(f).orElse(findAnyTypeRecursively(m.keyType)(f))
        .orElse(findAnyTypeRecursively(m.valueType)(f))
    case other =>
      Some(other).filter(f)
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
        throw new DeltaAnalysisException(
          errorClass = "DELTA_COMPLEX_TYPE_COLUMN_CONTAINS_NULL_TYPE",
          messageParameters = Array(colName, "ArrayType"))
      case m: MapType if typeExistsRecursively(m)(_.isInstanceOf[NullType]) =>
        val colName = UnresolvedAttribute.apply(nameStack :+ sf.name).name
        throw new DeltaAnalysisException(
          errorClass = "DELTA_COMPLEX_TYPE_COLUMN_CONTAINS_NULL_TYPE",
          messageParameters = Array(colName, "NullType"))
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
   * Returns the name of the first column/field that has null type (void).
   */
  def findNullTypeColumn(schema: StructType): Option[String] = {
    // Helper method to recursively check nested structs.
    def findNullTypeColumnRec(s: StructType, nameStack: Seq[String]): Option[String] = {
      val nullFields = s.flatMap {
        case StructField(name, n: NullType, _, _) => Some((nameStack :+ name).mkString("."))
        case StructField(name, s: StructType, _, _) => findNullTypeColumnRec(s, nameStack :+ name)
        // Note that we don't recursively check Array and Map types because NullTypes are already
        // not allowed (see 'dropNullTypeColumns').
        case _ => None
      }
      return nullFields.headOption
    }

    if (typeExistsRecursively(schema)(_.isInstanceOf[NullType])) {
      findNullTypeColumnRec(schema, Seq.empty)
    } else {
      None
    }
  }

  /**
   * Recursively rewrite the query field names according to the table schema within nested
   * data types.
   *
   * The same assumptions as in [[normalizeColumnNames]] are made.
   *
   * @param sourceDataType The data type that needs normalizing.
   * @param tableDataType The normalization template from the table's schema.
   * @param sourceParentFields The path (starting from the top level) to the nested field
   *                           with `sourceDataType`.
   * @param tableSchema The entire schema of the table.
   *
   * @return A normalized version of `sourceDataType`.
   */
def normalizeColumnNamesInDataType(
      deltaLog: DeltaLog,
      sourceDataType: DataType,
      tableDataType: DataType,
      sourceParentFields: Seq[String],
      tableSchema: StructType
    ): DataType = {

    def getMatchingTableField(
        sourceField: StructField,
        tableFields: Map[String, StructField]): StructField = {
      tableFields.get(sourceField.name) match {
        case Some(tableField) => tableField
        case None =>
          val columnPath = (sourceParentFields ++ Seq(sourceField.name)).mkString(".")
          throw DeltaErrors.cannotResolveColumn(columnPath, tableSchema)
      }
    }

    (sourceDataType, tableDataType) match {
      case (sourceStruct: StructType, tableStruct: StructType) =>
        val tableFields = toFieldMap(tableStruct.fields, caseSensitive = false)
        val normalizedFields = sourceStruct.fields.map { sourceField =>
          val tableField = getMatchingTableField(sourceField, tableFields)
          val normalizedDataType =
            normalizeColumnNamesInDataType(deltaLog, sourceField.dataType, tableField.dataType,
              sourceParentFields :+ sourceField.name, tableSchema)
          val normalizedName = tableField.name
          sourceField.copy(
            name = normalizedName,
            dataType = normalizedDataType
          )
        }
        sourceStruct.copy(fields = normalizedFields)
      case (sourceArray: ArrayType, tableArray: ArrayType) =>
        val normalizedElementType = normalizeColumnNamesInDataType(deltaLog,
          sourceArray.elementType, tableArray.elementType, sourceParentFields, tableSchema)
        sourceArray.copy(elementType = normalizedElementType)
      case (sourceMap: MapType, tableMap: MapType) =>
        val normalizedKeyType = normalizeColumnNamesInDataType(deltaLog, sourceMap.keyType,
          tableMap.keyType, sourceParentFields, tableSchema)
        val normalizedValueType = normalizeColumnNamesInDataType(deltaLog, sourceMap.valueType,
          tableMap.valueType, sourceParentFields, tableSchema)
        sourceMap.copy(
          keyType = normalizedKeyType,
          valueType = normalizedValueType
        )
      case (_: NullType, _) =>
        // When schema evolution adds a new column during MERGE, it can be represented with
        // a NullType in the schema of the data written by the MERGE.
        sourceDataType
      case (_: IntegralType, _: IntegralType) =>
        // The integral types can be cast to each other later on.
        sourceDataType
      case _ =>
        if (Utils.isTesting) {
          assert(sourceDataType == tableDataType,
            s"Types without nesting should match but $sourceDataType != $tableDataType")
        } else if (sourceDataType != tableDataType) {
          recordDeltaEvent(
            deltaLog = deltaLog,
            opType = "delta.assertions.schemaNormalization.nonNestedTypeMismatch",
            tags = Map.empty,
            data = Map(
              "sourceDataType" -> sourceDataType.json,
              "tableDataType" -> tableDataType.json
            ),
            path = None)
        }
        // The data types are compatible.
        sourceDataType
    }
  }

  /**
   * Rewrite the query field names according to the table schema. This method assumes that all
   * schema validation checks have been made and this is the last operation before writing into
   * Delta.
   */
  def normalizeColumnNames(
      deltaLog: DeltaLog,
      baseSchema: StructType,
      data: Dataset[_]
    ): DataFrame = {
    val dataSchema = data.schema
    val dataFields = explodeNestedFieldNames(dataSchema).toSet
    val tableFields = explodeNestedFieldNames(baseSchema).toSet
    if (dataFields.subsetOf(tableFields)) {
      data.toDF()
    } else {
      // Allow the same shortcut logic (as the above `if` stmt) if the only extra fields are CDC
      // metadata fields.
      val nonCdcFields = dataFields.filterNot { f =>
        f == CDCReader.CDC_PARTITION_COL || f == CDCReader.CDC_TYPE_COLUMN_NAME
      }
      if (nonCdcFields.subsetOf(tableFields)) {
        return data.toDF()
      }

      val baseFields = toFieldMap(baseSchema, caseSensitive = false)
      val aliasExpressions = dataSchema.map { field =>
        val (originalCase, castDataType): (String, Option[DataType]) =
          baseFields.get(field.name) match {
            case Some(original) =>
              val normalizedDataType = normalizeColumnNamesInDataType(deltaLog,
                field.dataType, original.dataType, Seq(field.name), baseSchema)
              (original.name, Option.when(field.dataType != normalizedDataType)(normalizedDataType))
            // This is a virtual partition column used for doing CDC writes. It's not actually
            // in the table schema.
            case None if field.name == CDCReader.CDC_TYPE_COLUMN_NAME ||
              field.name == CDCReader.CDC_PARTITION_COL => (field.name, None)
            // Consider Row Id columns internal if Row Ids are enabled.
            case None if RowId.RowIdMetadataStructField.isRowIdColumn(field) =>
              (field.name, None)
            case None =>
              throw DeltaErrors.cannotResolveColumn(field.name, baseSchema)
          }
        var expression = fieldToColumn(field)
        castDataType.foreach { castType =>
          expression = expression.cast(castType)
        }
        if (originalCase != field.name) {
          expression = expression.as(originalCase)
        }
        expression
      }
      data.select(aliasExpressions: _*)
    }
  }

  /**
   * A helper function to check if partition columns are the same.
   * This function only checks for partition column names.
   * Please use with other schema check functions for detecting type change etc.
   */
  def isPartitionCompatible(
      newPartitionColumns: Seq[String] = Seq.empty,
      oldPartitionColumns: Seq[String] = Seq.empty): Boolean = {
    newPartitionColumns == oldPartitionColumns
  }

  /**
   * As the Delta snapshots update, the schema may change as well. This method defines whether the
   * new schema of a Delta table can be used with a previously analyzed LogicalPlan. Our
   * rules are to return false if:
   *   - Dropping any column that was present in the existing schema, if not allowMissingColumns
   *   - Any change of datatype, if not allowTypeWidening. Any non-widening change of datatype
   *     otherwise.
   *   - Change of partition columns. Although analyzed LogicalPlan is not changed,
   *     physical structure of data is changed and thus is considered not read compatible.
   *   - If `forbidTightenNullability` = true:
   *      - Forbids tightening the nullability (existing nullable=true -> read nullable=false)
   *      - Typically Used when the existing schema refers to the schema of written data, such as
   *        when a Delta streaming source reads a schema change (existingSchema) which
   *        has nullable=true, using the latest schema which has nullable=false, so we should not
   *        project nulls from the data into the non-nullable read schema.
   *   - Otherwise:
   *      - Forbids relaxing the nullability (existing nullable=false -> read nullable=true)
   *      - Typically Used when the read schema refers to the schema of written data, such as during
   *        Delta scan, the latest schema during execution (readSchema) has nullable=true but during
   *        analysis phase the schema (existingSchema) was nullable=false, so we should not project
   *        nulls from the later data onto a non-nullable schema analyzed in the past.
   */
  def isReadCompatible(
      existingSchema: StructType,
      readSchema: StructType,
      forbidTightenNullability: Boolean = false,
      allowMissingColumns: Boolean = false,
      allowTypeWidening: Boolean = false,
      newPartitionColumns: Seq[String] = Seq.empty,
      oldPartitionColumns: Seq[String] = Seq.empty): Boolean = {

    def isNullabilityCompatible(existingNullable: Boolean, readNullable: Boolean): Boolean = {
      if (forbidTightenNullability) {
        readNullable || !existingNullable
      } else {
        existingNullable || !readNullable
      }
    }

    def isDatatypeReadCompatible(existing: DataType, newtype: DataType): Boolean = {
      (existing, newtype) match {
        case (e: StructType, n: StructType) =>
          isReadCompatible(e, n, forbidTightenNullability, allowTypeWidening = allowTypeWidening)
        case (e: ArrayType, n: ArrayType) =>
          // if existing elements are non-nullable, so should be the new element
          isNullabilityCompatible(e.containsNull, n.containsNull) &&
            isDatatypeReadCompatible(e.elementType, n.elementType)
        case (e: MapType, n: MapType) =>
          // if existing value is non-nullable, so should be the new value
          isNullabilityCompatible(e.valueContainsNull, n.valueContainsNull) &&
            isDatatypeReadCompatible(e.keyType, n.keyType) &&
            isDatatypeReadCompatible(e.valueType, n.valueType)
        case (e: AtomicType, n: AtomicType) if allowTypeWidening =>
          TypeWidening.isTypeChangeSupportedForSchemaEvolution(e, n)
        case (a, b) => a == b
      }
    }

    def isStructReadCompatible(existing: StructType, newtype: StructType): Boolean = {
      val existingFields = toFieldMap(existing)
      // scalastyle:off caselocale
      val existingFieldNames = existing.fieldNames.map(_.toLowerCase).toSet
      assert(existingFieldNames.size == existing.length,
        "Delta tables don't allow field names that only differ by case")
      val newFields = newtype.fieldNames.map(_.toLowerCase).toSet
      assert(newFields.size == newtype.length,
        "Delta tables don't allow field names that only differ by case")
      // scalastyle:on caselocale

      if (!allowMissingColumns &&
        !(existingFieldNames.subsetOf(newFields) &&
          isPartitionCompatible(newPartitionColumns, oldPartitionColumns))) {
        // Dropped a column that was present in the DataFrame schema
        return false
      }
      newtype.forall { newField =>
        // new fields are fine, they just won't be returned
        existingFields.get(newField.name).forall { existingField =>
          // we know the name matches modulo case - now verify exact match
          (existingField.name == newField.name
            // if existing value is non-nullable, so should be the new value
            && isNullabilityCompatible(existingField.nullable, newField.nullable)
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

    structDifference(
      existingSchema,
      CharVarcharUtils.replaceCharVarcharWithStringInSchema(specifiedSchema),
      ""
    )
  }

  /**
   * Copied verbatim from Apache Spark.
   *
   * Returns a field in this struct and its child structs, case insensitively. This is slightly less
   * performant than the case sensitive version.
   *
   * If includeCollections is true, this will return fields that are nested in maps and arrays.
   *
   * @param fieldNames The path to the field, in order from the root. For example, the column
   *                   nested.a.b.c would be Seq("nested", "a", "b", "c").
   */
  def findNestedFieldIgnoreCase(
      schema: StructType,
      fieldNames: Seq[String],
      includeCollections: Boolean = false): Option[StructField] = {

    @scala.annotation.tailrec
    def findRecursively(
      dataType: DataType,
      fieldNames: Seq[String],
      includeCollections: Boolean): Option[StructField] = {

      (fieldNames, dataType, includeCollections) match {
        case (Seq(fieldName, names @ _*), struct: StructType, _) =>
          val field = struct.find(_.name.equalsIgnoreCase(fieldName))
          if (names.isEmpty || field.isEmpty) {
            field
          } else {
            findRecursively(field.get.dataType, names, includeCollections)
          }

        case (_, _, false) => None // types nested in maps and arrays are not used

        case (Seq("key"), MapType(keyType, _, _), true) =>
          // return the key type as a struct field to include nullability
          Some(StructField("key", keyType, nullable = false))

        case (Seq("key", names @ _*), MapType(keyType, _, _), true) =>
          findRecursively(keyType, names, includeCollections)

        case (Seq("value"), MapType(_, valueType, isNullable), true) =>
          // return the value type as a struct field to include nullability
          Some(StructField("value", valueType, nullable = isNullable))

        case (Seq("value", names @ _*), MapType(_, valueType, _), true) =>
          findRecursively(valueType, names, includeCollections)

        case (Seq("element"), ArrayType(elementType, isNullable), true) =>
          // return the element type as a struct field to include nullability
          Some(StructField("element", elementType, nullable = isNullable))

        case (Seq("element", names @ _*), ArrayType(elementType, _), true) =>
          findRecursively(elementType, names, includeCollections)

        case _ =>
          None
      }
    }

    findRecursively(schema, fieldNames, includeCollections)
  }

  /**
   * Returns the path of the given column in `schema` as a list of ordinals (0-based), each value
   * representing the position at the current nesting level starting from the root.
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
      resolver: Resolver = DELTA_COL_RESOLVER): Seq[Int] = {
    def findRecursively(
        searchPath: Seq[String],
        currentType: DataType,
        currentPath: Seq[String] = Nil): Seq[Int] = {
      if (searchPath.isEmpty) return Nil

      val currentFieldName = searchPath.head
      val currentPathWithNestedField = currentPath :+ currentFieldName
      (currentType, currentFieldName) match {
        case (struct: StructType, _) =>
          lazy val columnPath = UnresolvedAttribute(currentPathWithNestedField).name
          val pos = struct.indexWhere(f => resolver(f.name, currentFieldName))
          if (pos == -1) {
            throw DeltaErrors.columnNotInSchemaException(columnPath, schema)
          }
          val childPosition = findRecursively(
            searchPath = searchPath.tail,
            currentType = struct(pos).dataType,
            currentPath = currentPathWithNestedField)
          pos +: childPosition

        case (map: MapType, "key") =>
          val childPosition = findRecursively(
            searchPath = searchPath.tail,
            currentType = map.keyType,
            currentPath = currentPathWithNestedField)
          MAP_KEY_INDEX +: childPosition

        case (map: MapType, "value") =>
          val childPosition = findRecursively(
            searchPath = searchPath.tail,
            currentType = map.valueType,
            currentPath = currentPathWithNestedField)
          MAP_VALUE_INDEX +: childPosition

        case (_: MapType, _) =>
          throw DeltaErrors.foundMapTypeColumnException(
            prettyFieldName(currentPath :+ "key"),
            prettyFieldName(currentPath :+ "value"),
            schema)

        case (array: ArrayType, "element") =>
          val childPosition = findRecursively(
            searchPath = searchPath.tail,
            currentType = array.elementType,
            currentPath = currentPathWithNestedField)
          ARRAY_ELEMENT_INDEX +: childPosition

        case (_: ArrayType, _) =>
          throw DeltaErrors.incorrectArrayAccessByName(
            prettyFieldName(currentPath :+ "element"),
            prettyFieldName(currentPath),
            schema)
        case _ =>
          throw DeltaErrors.columnPathNotNested(currentFieldName, currentType, currentPath, schema)
      }
    }

    try {
      findRecursively(column, schema)
    } catch {
      case e: DeltaAnalysisException => throw e
      case e: AnalysisException =>
        throw DeltaErrors.errorFindingColumnPosition(column, schema, e.getMessage)
    }
  }

  /**
   * Returns the nested field at the given position in `parent`. See [[findColumnPosition]] for the
   * representation used for `position`.
   * @param parent The field used for the lookup.
   * @param position A list of ordinals (0-based) representing the path to the nested field in
   *                 `parent`.
   */
  def getNestedFieldFromPosition(parent: StructField, position: Seq[Int]): StructField = {
    if (position.isEmpty) return parent

    val fieldPos = position.head
    parent.dataType match {
      case struct: StructType if fieldPos >= 0 && fieldPos < struct.size =>
        getNestedFieldFromPosition(struct(fieldPos), position.tail)
      case map: MapType if fieldPos == MAP_KEY_INDEX =>
        getNestedFieldFromPosition(StructField("key", map.keyType), position.tail)
      case map: MapType if fieldPos == MAP_VALUE_INDEX =>
        getNestedFieldFromPosition(StructField("value", map.valueType), position.tail)
      case array: ArrayType if fieldPos == ARRAY_ELEMENT_INDEX =>
        getNestedFieldFromPosition(StructField("element", array.elementType), position.tail)
      case _: StructType | _: ArrayType | _: MapType =>
        throw new IllegalArgumentException(
          s"Invalid child position $fieldPos in ${parent.dataType}")
      case other =>
        throw new IllegalArgumentException(s"Invalid indexing into non-nested type $other")
    }
  }

  /**
   * Returns the nested type at the given position in `schema`. See [[findColumnPosition]] for the
   * representation used for `position`.
   * @param parent The root schema used for the lookup.
   * @param position A list of ordinals (0-based) representing the path to the nested field in
   *                 `parent`.
   */
  def getNestedTypeFromPosition(schema: StructType, position: Seq[Int]): DataType =
    getNestedFieldFromPosition(StructField("schema", schema), position).dataType

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
    def addColumnInChild(parent: DataType, column: StructField, position: Seq[Int]): DataType = {
      if (position.isEmpty) {
          throw DeltaErrors.addColumnParentNotStructException(column, parent)
      }
      parent match {
        case struct: StructType =>
          addColumn(struct, column, position)
        case map: MapType if position.head == MAP_KEY_INDEX =>
          map.copy(keyType = addColumnInChild(map.keyType, column, position.tail))
        case map: MapType if position.head == MAP_VALUE_INDEX =>
          map.copy(valueType = addColumnInChild(map.valueType, column, position.tail))
        case array: ArrayType if position.head == ARRAY_ELEMENT_INDEX =>
          array.copy(elementType = addColumnInChild(array.elementType, column, position.tail))
        case _: ArrayType =>
          throw DeltaErrors.incorrectArrayAccess()
        case other =>
          throw DeltaErrors.addColumnParentNotStructException(column, other)
      }
    }
    // If the proposed new column includes a default value, return a specific "not supported" error.
    // The rationale is that such operations require the data source scan operator to implement
    // support for filling in the specified default value when the corresponding field is not
    // present in storage. That is not implemented yet for Delta, so we return this error instead.
    // The error message is descriptive and provides an easy workaround for the user.
    if (column.metadata.contains("CURRENT_DEFAULT")) {
      throw new DeltaAnalysisException(
        errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED",
        messageParameters = Array.empty)
    }

    require(position.nonEmpty, s"Don't know where to add the column $column")
    val slicePosition = position.head
    if (slicePosition < 0) {
      throw DeltaErrors.addColumnAtIndexLessThanZeroException(
        slicePosition.toString, column.toString)
    }
    val length = schema.length
    if (slicePosition > length) {
      throw DeltaErrors.indexLargerThanStruct(slicePosition, column, length)
    }
    if (slicePosition == length) {
      if (position.length > 1) {
        throw DeltaErrors.addColumnStructNotFoundException(slicePosition.toString)
      }
      return StructType(schema :+ column)
    }
    val (pre, post) = schema.splitAt(slicePosition)
    if (position.length > 1) {
      val field = post.head
      if (!column.nullable && field.nullable) {
        throw DeltaErrors.nullableParentWithNotNullNestedField
      }
      val mid = field.copy(dataType = addColumnInChild(field.dataType, column, position.tail))
      StructType(pre ++ Seq(mid) ++ post.tail)
    } else {
      StructType(pre ++ Seq(column) ++ post)
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
    def dropColumnInChild(parent: DataType, position: Seq[Int]): (DataType, StructField) = {
      if (position.isEmpty) {
          throw DeltaErrors.dropNestedColumnsFromNonStructTypeException(parent)
      }
      parent match {
        case struct: StructType =>
          dropColumn(struct, position)
        case map: MapType if position.head == MAP_KEY_INDEX =>
          val (newKeyType, droppedColumn) = dropColumnInChild(map.keyType, position.tail)
          map.copy(keyType = newKeyType) -> droppedColumn
        case map: MapType if position.head == MAP_VALUE_INDEX =>
          val (newValueType, droppedColumn) = dropColumnInChild(map.valueType, position.tail)
          map.copy(valueType = newValueType) -> droppedColumn
        case array: ArrayType if position.head == ARRAY_ELEMENT_INDEX =>
          val (newElementType, droppedColumn) = dropColumnInChild(array.elementType, position.tail)
          array.copy(elementType = newElementType) -> droppedColumn
        case _: ArrayType =>
          throw DeltaErrors.incorrectArrayAccess()
        case other =>
          throw DeltaErrors.dropNestedColumnsFromNonStructTypeException(other)
      }
    }

    require(position.nonEmpty, "Don't know where to drop the column")
    val slicePosition = position.head
    if (slicePosition < 0) {
      throw DeltaErrors.dropColumnAtIndexLessThanZeroException(slicePosition)
    }
    val length = schema.length
    if (slicePosition >= length) {
      throw DeltaErrors.indexLargerOrEqualThanStruct(slicePosition, length)
    }
    val (pre, post) = schema.splitAt(slicePosition)
    val field = post.head
    if (position.length > 1) {
      val (newType, droppedColumn) = dropColumnInChild(field.dataType, position.tail)
      val mid = field.copy(dataType = newType)

      StructType(pre ++ Seq(mid) ++ post.tail) -> droppedColumn
    } else {
      if (length == 1) {
        throw DeltaErrors.dropColumnOnSingleFieldSchema(schema)
      }
      StructType(pre ++ post.tail) -> field
    }
  }

  /**
   * Check if the two data types can be changed.
   *
   * @param failOnAmbiguousChanges Throw an error if a StructField both has columns dropped and new
   *                               columns added. These are ambiguous changes, because we don't
   *                               know if a column needs to be renamed, dropped, or added.
   * @param allowTypeWidening      Whether widening type changes as defined in [[TypeWidening]]
   *                               can be applied.
   * @return None if the data types can be changed, otherwise Some(err) containing the reason.
   */
  def canChangeDataType(
      from: DataType,
      to: DataType,
      resolver: Resolver,
      columnMappingMode: DeltaColumnMappingMode,
      columnPath: Seq[String] = Nil,
      failOnAmbiguousChanges: Boolean = false,
      allowTypeWidening: Boolean = false): Option[String] = {
    def verify(cond: Boolean, err: => String): Unit = {
      if (!cond) {
        throw DeltaErrors.cannotChangeDataType(err)
      }
    }

    def verifyNullability(fn: Boolean, tn: Boolean, columnPath: Seq[String]): Unit = {
      verify(tn || !fn, s"tightening nullability of ${UnresolvedAttribute(columnPath).name}")
    }

    def check(fromDt: DataType, toDt: DataType, columnPath: Seq[String]): Unit = {
      (fromDt, toDt) match {
        case (ArrayType(fromElement, fn), ArrayType(toElement, tn)) =>
          verifyNullability(fn, tn, columnPath)
          check(fromElement, toElement, columnPath :+ "element")

        case (MapType(fromKey, fromValue, fn), MapType(toKey, toValue, tn)) =>
          verifyNullability(fn, tn, columnPath)
          check(fromKey, toKey, columnPath :+ "key")
          check(fromValue, toValue, columnPath :+ "value")

        case (f @ StructType(fromFields), t @ StructType(toFields)) =>
          val remainingFields = mutable.Set[StructField]()
          remainingFields ++= fromFields
          var addingColumns = false
          toFields.foreach { toField =>
            fromFields.find(field => resolver(field.name, toField.name)) match {
              case Some(fromField) =>
                remainingFields -= fromField

                val newPath = columnPath :+ fromField.name
                verifyNullability(fromField.nullable, toField.nullable, newPath)
                check(fromField.dataType, toField.dataType, newPath)
              case None =>
                addingColumns = true
                verify(toField.nullable,
                  "adding non-nullable column " +
                  UnresolvedAttribute(columnPath :+ toField.name).name)
            }
          }
          val columnName = UnresolvedAttribute(columnPath).name
          if (failOnAmbiguousChanges && remainingFields.nonEmpty && addingColumns) {
            throw DeltaErrors.ambiguousDataTypeChange(columnName, f, t)
          }
          if (columnMappingMode == NoMapping) {
            verify(remainingFields.isEmpty,
              s"dropping column(s) [${remainingFields.map(_.name).mkString(", ")}]" +
                (if (columnPath.nonEmpty) s" from $columnName" else ""))
          }

        case (fromDataType: AtomicType, toDataType: AtomicType) if allowTypeWidening =>
          verify(TypeWidening.isTypeChangeSupported(fromDataType, toDataType),
            s"changing data type of ${UnresolvedAttribute(columnPath).name} " +
              s"from $fromDataType to $toDataType")

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
   * Runs the transform function `tf` on all nested StructTypes, MapTypes and ArrayTypes in the
   * schema.
   * If `colName` is defined, the transform function is only applied to all the fields with the
   * given name. There may be multiple matches if nested fields with the same name exist in the
   * schema, it is the responsibility of the caller to check the full field path before transforming
   * a field.
   * @param schema to transform.
   * @param colName Optional name to match for
   * @param tf function to apply on the StructType.
   * @return the transformed schema.
   */
  def transformSchema(
      schema: StructType,
      colName: Option[String] = None)(
      tf: (Seq[String], DataType, Resolver) => DataType): StructType = {
    def transform[E <: DataType](path: Seq[String], dt: E): E = {
      val newDt = dt match {
        case struct @ StructType(fields) =>
          val newStruct = if (colName.isEmpty || fields.exists(f => colName.contains(f.name))) {
            tf(path, struct, DELTA_COL_RESOLVER).asInstanceOf[StructType]
          } else {
            struct
          }

          StructType(newStruct.fields.map { field =>
            field.copy(dataType = transform(path :+ field.name, field.dataType))
          })
        case array: ArrayType =>
          val newArray =
            if (colName.isEmpty || colName.contains("element")) {
              tf(path, array, DELTA_COL_RESOLVER).asInstanceOf[ArrayType]
            } else {
              array
            }
          newArray.copy(elementType = transform(path :+ "element", newArray.elementType))
        case map: MapType =>
          val newMap =
            if (colName.isEmpty || colName.contains("key") || colName.contains("value")) {
              tf(path, map, DELTA_COL_RESOLVER).asInstanceOf[MapType]
            } else {
              map
            }
          newMap.copy(
            keyType = transform(path :+ "key", newMap.keyType),
            valueType = transform(path :+ "value", newMap.valueType))
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
    SchemaMergingUtils.transformColumns(schema) { (path, field, resolver) =>
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
   * Check if the schema contains invalid char in the column names depending on the mode.
   */
  def checkSchemaFieldNames(schema: StructType, columnMappingMode: DeltaColumnMappingMode): Unit = {
    if (columnMappingMode != NoMapping) {
      return
    }
    val invalidColumnNames =
      findInvalidColumnNames(SchemaMergingUtils.explodeNestedFieldNames(schema))
    if (invalidColumnNames.nonEmpty) {
      throw DeltaErrors.foundInvalidCharsInColumnNames(invalidColumnNames)
    }
  }

  /**
   * Verifies that the column names are acceptable by Parquet and henceforth Delta. Parquet doesn't
   * accept the characters ' ,;{}()\n\t='. We ensure that neither the data columns nor the partition
   * columns have these characters.
   */
  def checkFieldNames(names: Seq[String]): Unit = {
    val invalidColumnNames = findInvalidColumnNames(names)
    if (invalidColumnNames.nonEmpty) {
      throw DeltaErrors.invalidColumnName(invalidColumnNames.head)
    }
  }

  /**
   * Finds columns with invalid names, i.e. names containing any of the ' ,;{}()\n\t=' characters.
   */
  def findInvalidColumnNamesInSchema(schema: StructType): Seq[String] = {
    findInvalidColumnNames(SchemaMergingUtils.explodeNestedFieldNames(schema))
  }

  private def findInvalidColumnNames(columnNames: Seq[String]): Seq[String] = {
    val badChars = Seq(' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=')
    columnNames.filter(colName => badChars.map(_.toString).exists(colName.contains))
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

    SchemaMergingUtils.transformColumns(schema)(checkField)
  }

  def fieldToColumn(field: StructField): Column = {
    new Column(UnresolvedAttribute.quoted(field.name))
  }

  /**  converting field name to column type with quoted back-ticks */
  def fieldNameToColumn(field: String): Column = {
    col(quoteIdentifier(field))
  }
  // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
  // identifier with back-ticks.
  def quoteIdentifier(part: String): String = s"`${part.replace("`", "``")}`"

  /**
   * Will a column change, e.g., rename, need to be populated to the expression. This is true when
   * the column to change itself or any of its descendent column is referenced by expression.
   * For example:
   *  - a, length(a) -> true
   *  - b, (b.c + 1) -> true, because renaming b1 will need to change the expr to (b1.c + 1).
   *  - b.c, (cast b as string) -> false, because you can change b.c to b.c1 without affecting b.
   */
  def containsDependentExpression(
      spark: SparkSession,
      columnToChange: Seq[String],
      exprString: String,
      resolver: Resolver): Boolean = {
    val expression = spark.sessionState.sqlParser.parseExpression(exprString)
    expression.foreach {
      case refCol: UnresolvedAttribute =>
        // columnToChange is the referenced column or its prefix
        val prefixMatched = columnToChange.size <= refCol.nameParts.size &&
          refCol.nameParts.zip(columnToChange).forall(pair => resolver(pair._1, pair._2))
        if (prefixMatched) return true
      case _ =>
    }
    false
  }

  /**
   * Find the unsupported data type in a table schema. Return all columns that are using unsupported
   * data types. For example,
   * `findUnsupportedDataType(struct&lt;a: struct&lt;b: unsupported_type&gt;&gt;)` will return
   * `Some(unsupported_type, Some("a.b"))`.
   */
  def findUnsupportedDataTypes(schema: StructType): Seq[UnsupportedDataTypeInfo] = {
    val unsupportedDataTypes = mutable.ArrayBuffer[UnsupportedDataTypeInfo]()
    findUnsupportedDataTypesRecursively(unsupportedDataTypes, schema)
    unsupportedDataTypes.toSeq
  }

  /**
   * Find VariantType columns in the table schema.
   */
  def checkForVariantTypeColumnsRecursively(schema: StructType): Boolean = {
    SchemaUtils.typeExistsRecursively(schema)(VariantShim.isTypeVariant(_))
  }

  /**
   * Find TimestampNTZ columns in the table schema.
   */
  def checkForTimestampNTZColumnsRecursively(schema: StructType): Boolean = {
    SchemaUtils.typeExistsRecursively(schema)(_.isInstanceOf[TimestampNTZType])
  }

  /**
   * Find the unsupported data types in a `DataType` recursively. Add the unsupported data types to
   * the provided `unsupportedDataTypes` buffer.
   *
   * @param unsupportedDataTypes the buffer to store the found unsupport data types and the column
   *                             paths.
   * @param dataType the data type to search.
   * @param columnPath the column path to access the given data type. The callder should make sure
   *                   `columnPath` is not empty when `dataType` is not `StructType`.
   */
  private def findUnsupportedDataTypesRecursively(
      unsupportedDataTypes: mutable.ArrayBuffer[UnsupportedDataTypeInfo],
      dataType: DataType,
      columnPath: Seq[String] = Nil): Unit = dataType match {
    case NullType =>
    case BooleanType =>
    case ByteType =>
    case ShortType =>
    case IntegerType =>
    case dt: YearMonthIntervalType =>
      assert(columnPath.nonEmpty, "'columnPath' must not be empty")
      unsupportedDataTypes += UnsupportedDataTypeInfo(prettyFieldName(columnPath), dt)
    case LongType =>
    case dt: DayTimeIntervalType =>
      assert(columnPath.nonEmpty, "'columnPath' must not be empty")
      unsupportedDataTypes += UnsupportedDataTypeInfo(prettyFieldName(columnPath), dt)
    case FloatType =>
    case DoubleType =>
    case StringType =>
    case DateType =>
    case TimestampType =>
    case TimestampNTZType =>
    case dt if VariantShim.isTypeVariant(dt) =>
    case BinaryType =>
    case _: DecimalType =>
    case a: ArrayType =>
      assert(columnPath.nonEmpty, "'columnPath' must not be empty")
      findUnsupportedDataTypesRecursively(
        unsupportedDataTypes,
        a.elementType,
        columnPath.dropRight(1) :+ columnPath.last + "[]")
    case m: MapType =>
      assert(columnPath.nonEmpty, "'columnPath' must not be empty")
      findUnsupportedDataTypesRecursively(
        unsupportedDataTypes,
        m.keyType,
        columnPath.dropRight(1) :+ columnPath.last + "[key]")
      findUnsupportedDataTypesRecursively(
        unsupportedDataTypes,
        m.valueType,
        columnPath.dropRight(1) :+ columnPath.last + "[value]")
    case s: StructType =>
      s.fields.foreach { f =>
        findUnsupportedDataTypesRecursively(
          unsupportedDataTypes,
          f.dataType,
          columnPath :+ f.name)
      }
    case udt: UserDefinedType[_] =>
      findUnsupportedDataTypesRecursively(unsupportedDataTypes, udt.sqlType, columnPath)
    case dt: DataType =>
      assert(columnPath.nonEmpty, "'columnPath' must not be empty")
      unsupportedDataTypes += UnsupportedDataTypeInfo(prettyFieldName(columnPath), dt)
  }

  /**
   * Find all the generated columns that depend on the given target column.
   */
  def findDependentGeneratedColumns(
      sparkSession: SparkSession,
      targetColumn: Seq[String],
      protocol: Protocol,
      schema: StructType): Seq[StructField] = {
    if (GeneratedColumn.satisfyGeneratedColumnProtocol(protocol) &&
        GeneratedColumn.hasGeneratedColumns(schema)) {

      val dependentGenCols = ArrayBuffer[StructField]()
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        GeneratedColumn.getGenerationExpressionStr(field.metadata).foreach { exprStr =>
          val needsToChangeExpr = SchemaUtils.containsDependentExpression(
            sparkSession, targetColumn, exprStr, sparkSession.sessionState.conf.resolver)
          if (needsToChangeExpr) dependentGenCols += field
        }
        field
      }
      dependentGenCols.toList
    } else {
      Seq.empty
    }
  }

  /** Recursively find all types not defined in Delta protocol but used in `dt` */
  def findUndefinedTypes(dt: DataType): Seq[DataType] = dt match {
    // Types defined in Delta protocol
    case NullType => Nil
    case BooleanType => Nil
    case ByteType | ShortType | IntegerType | LongType => Nil
    case FloatType | DoubleType | _: DecimalType => Nil
    case StringType | BinaryType => Nil
    case DateType | TimestampType => Nil
    // Recursively search complex data types
    case s: StructType => s.fields.flatMap(f => findUndefinedTypes(f.dataType))
    case a: ArrayType => findUndefinedTypes(a.elementType)
    case m: MapType => findUndefinedTypes(m.keyType) ++ findUndefinedTypes(m.valueType)
    // Other types are not defined in Delta protocol
    case undefinedType => Seq(undefinedType)
  }

  /** Record all types not defined in Delta protocol but used in the `schema`. */
  def recordUndefinedTypes(deltaLog: DeltaLog, schema: StructType): Unit = {
    try {
      findUndefinedTypes(schema).map(_.getClass.getName).toSet.foreach { className: String =>
        recordDeltaEvent(deltaLog, "delta.undefined.type", data = Map("className" -> className))
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to log undefined types for table ${deltaLog.logPath}", e)
    }
  }
}

/**
 * The information of unsupported data type returned by [[SchemaUtils.findUnsupportedDataTypes]].
 *
 * @param column the column path to access the column using an unsupported data type, such as `a.b`.
 * @param dataType the unsupported data type.
 */
case class UnsupportedDataTypeInfo(column: String, dataType: DataType)
