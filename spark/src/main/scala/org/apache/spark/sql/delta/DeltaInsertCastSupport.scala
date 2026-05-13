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

package org.apache.spark.sql.delta

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf.AllowAutomaticWideningMode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Shared casting and schema-adjustment utilities for Delta insert operations.
 * Mixed into both [[DeltaAnalysis]] (SQL + DF by-ordinal inserts) and [[DeltaImplicitCast]]
 * (DF by-name inserts).
 */
trait DeltaInsertCastSupport extends DeltaLogging {

  protected def session: SparkSession
  protected def conf: SQLConf


  type CastFunction = (Expression, DataType, String) => Expression

  /**
   * Conditionally wraps a struct expression with an IF expression to preserve NULL source values
   * when `DELTA_INSERT_PRESERVE_NULL_SOURCE_STRUCTS` is enabled:
   *   IF(sourceExpr IS NULL, NULL, createStructExpr)
   *
   * This prevents null expansion where a null struct would be incorrectly expanded to a struct
   * with all fields set to NULL during INSERT operations.
   *
   * @param sourceExpr The source struct expression
   * @param createStructExpr The generated CreateStruct expression
   * @return The potentially wrapped expression with null preservation logic
   */
  private def maybeWrapWithNullPreservationForInsert(
      sourceExpr: Expression,
      createStructExpr: Expression): Expression = {
    if (conf.getConf(DeltaSQLConf.DELTA_INSERT_PRESERVE_NULL_SOURCE_STRUCTS)) {
      val sourceNullCondition = IsNull(sourceExpr)
      val targetType = createStructExpr.dataType
      If(
        sourceNullCondition,
        Literal.create(null, targetType),
        createStructExpr
      )
    } else {
      createStructExpr
    }
  }

  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) and Aliases so that we
   * can check if the by-ordinal schema of the insert query matches our Delta table.
   * The schema adjustment also include string length check if it's written into a char/varchar
   * type column/field.
   */
  protected def resolveQueryColumnsByOrdinal(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String]): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        addCastToColumn(attr, targetAttr, deltaTable.name(),
          typeWideningMode = getTypeWideningMode(deltaTable, writeOptions)
        )
      } else {
        attr
      }
    }
    Project(project, query)
  }

  /**
   * Performs the schema adjustment by adding UpCasts (which are safe) so that we can insert into
   * the Delta table when the input data types doesn't match the table schema. Unlike
   * `resolveQueryColumnsByOrdinal` which ignores the names in `targetAttrs` and maps attributes
   * directly to query output, this method will use the names in the query output to find the
   * corresponding attribute to use. This method also allows users to not provide values for
   * generated columns. If values of any columns are not in the query output, they must be generated
   * columns.
   */
  protected def resolveQueryColumnsByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String],
      allowSchemaEvolution: Boolean = false,
      isDfByNameInsert: Boolean): LogicalPlan = {
    // Schema evolution is only effective when mergeSchema is enabled in write options AND
    // the feature is enabled via SQL conf. For dataframe by-name queries, the schema evolution
    // check is always deferred until after DeltaAnalysis, so avoid throwing here.
    val effectiveSchemaEvolution = allowSchemaEvolution && (isDfByNameInsert ||
      new DeltaOptions(deltaTable.options ++ writeOptions, conf).canMergeSchema &&
      session.conf.get(DeltaSQLConf.DELTA_INSERT_BY_NAME_SCHEMA_EVOLUTION_ENABLED))

    // DataFrame by-name inserts always allow missing columns; SQL BY NAME inserts only allow
    // them when USE_NULLS_FOR_DEFAULT_COLUMN_VALUES is set or the column has a default/generated
    // expression. isDfByNameInsert=false signals the SQL path, which needs the stricter check.
    if (!isDfByNameInsert) {
      insertIntoByNameMissingColumn(query, targetAttrs, deltaTable, effectiveSchemaEvolution)
    }

    // This is called before resolveOutputColumns in postHocResolutionRules, so we need to duplicate
    // the schema validation here.
    if (!effectiveSchemaEvolution && query.output.length > targetAttrs.length) {
      throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
        tableName = deltaTable.name(),
        expected = targetAttrs.map(_.name),
        queryOutput = query.output)
    }

    val project = query.output.map { attr =>
      val targetAttr = targetAttrs.find(t => session.sessionState.conf.resolver(t.name, attr.name))
        .getOrElse {
          if (effectiveSchemaEvolution) {
            attr
          } else {
            // Extra columns in the source are not allowed when schema evolution is disabled.
            throw DeltaErrors.missingColumn(attr, targetAttrs)
          }
        }
      addCastToColumn(attr, targetAttr, deltaTable.name(),
        typeWideningMode = getTypeWideningMode(deltaTable, writeOptions),
        byName = isDfByNameInsert
      )
    }
    Project(project, query)
  }

  protected def addCastToColumn(
      attr: NamedExpression,
      targetAttr: NamedExpression,
      tblName: String,
      typeWideningMode: TypeWideningMode,
      byName: Boolean = false): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        if (byName) {
          addCastsToStructsByName(tblName, attr, s, t, typeWideningMode)
        } else {
          addCastsToStructsByPosition(tblName, attr, s, t, typeWideningMode)
        }
      case (ArrayType(s: StructType, sNull: Boolean), ArrayType(t: StructType, tNull: Boolean))
        if s != t && sNull == tNull =>
        addCastsToArrayStructs(tblName, attr, s, t, sNull, typeWideningMode, byName)
      case (ArrayType(s, sNull: Boolean), ArrayType(t, tNull: Boolean)) if byName && s != t =>
        // General array element casting for non-struct elements currently only for by-name inserts.
        // Recursively applies addCastToColumn to each element via ArrayTransform.
        val elementVar = NamedLambdaVariable("element", s, sNull)
        val targetElementAttr = AttributeReference("element", t, tNull)()
        val castedElement = addCastToColumn(
          elementVar, targetElementAttr, tblName, typeWideningMode, byName)
        ArrayTransform(attr, LambdaFunction(castedElement, Seq(elementVar)))
      case (_, _: NullType) if byName =>
        attr
      case (s: AtomicType, t: AtomicType)
        if typeWideningMode.shouldWidenTo(fromType = t, toType = s) =>
        // Keep the type from the query, the target schema will be updated to widen the existing
        // type to match it.
        attr
      case (s: MapType, t: MapType)
        if !DataType.equalsStructurally(s, t, ignoreNullability = true) || (byName && s != t) =>
        // only trigger addCastsToMaps if exists differences like extra fields, renaming or type
        // differences. When by-name, always trigger as equalsStructurally is a positional check.
        addCastsToMaps(tblName, attr, s, t, typeWideningMode, byName)
      case _ =>
        getCastFunction(attr, targetAttr.dataType, targetAttr.name)
    }
    // Preserve source name when byName=true to let downstream handle normalization.
    val fieldName = if (byName) attr.name else targetAttr.name
    val metadata = if (byName) attr.metadata else targetAttr.metadata
    Alias(expr, fieldName)(explicitMetadata = Option(metadata))
  }

  /**
   * Returns the type widening mode to use for the given delta table. A type widening mode indicates
   * for (fromType, toType) tuples whether `fromType` is eligible to be automatically widened to
   * `toType` when ingesting data. If it is, the table schema is updated to `toType` before
   * ingestion and values are written using their original `toType` type. Otherwise, the table type
   * `fromType` is retained and values are downcasted on write.
   */
  protected def getTypeWideningMode(
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String]): TypeWideningMode = {
    val options = new DeltaOptions(deltaTable.options ++ writeOptions, conf)
    val snapshot = deltaTable.initialSnapshot
    val typeWideningEnabled = TypeWidening.isEnabled(snapshot.protocol, snapshot.metadata)
    val schemaEvolutionEnabled = options.canMergeSchema

    if (typeWideningEnabled && schemaEvolutionEnabled) {
      TypeWideningMode.TypeEvolution(
        uniformIcebergCompatibleOnly = UniversalFormat.icebergEnabled(snapshot.metadata),
        allowAutomaticWidening = AllowAutomaticWideningMode.fromConf(conf))
    } else {
      TypeWideningMode.NoTypeWidening
    }
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. This allows us to perform better schema enforcement/evolution. Since Spark
   * skips this step, we see if we need to perform any schema adjustment here.
   */
  protected def needsSchemaAdjustmentByOrdinal(
      deltaTable: DeltaTableV2,
      query: LogicalPlan,
      schema: StructType,
      writeOptions: Map[String, String]): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(deltaTable.name(), output.length, schema.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoDelta
    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      !SchemaUtils.isReadCompatible(schema.asNullable, existingSchemaOutput.toStructType,
        typeWideningMode = getTypeWideningMode(deltaTable, writeOptions))
  }

  protected def hasReplaceOnOrUsingOption(writeOptions: Map[String, String]): Boolean = {
    val caseInsensitiveWriteOptions = CaseInsensitiveMap(writeOptions)
    caseInsensitiveWriteOptions.contains(DeltaOptions.REPLACE_ON_OPTION) ||
      caseInsensitiveWriteOptions.contains(DeltaOptions.REPLACE_USING_OPTION)
  }

  /**
   * Checks for missing columns in a insert by name query and throws an exception if found.
   * Delta does not require users to provide values for generated columns, so any columns missing
   * from the query output must have a default expression.
   * See [[ColumnWithDefaultExprUtils.columnHasDefaultExpr]].
   */
  private def insertIntoByNameMissingColumn(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      allowSchemaEvolution: Boolean = false): Unit = {
    // When allowing the source schema to contain extra columns, it can still
    // be missing required columns from the target schema.
    //
    // The total column count alone is not sufficient for validation.
    // A source may have more columns overall but still omit specific columns
    // that are required by the target schema.
    //
    // Example:
    //   Target: [a, b]
    //   Source: [a, x, y]
    // Since the source has 3 columns vs target's 2, but is missing column b, this should be caught.
    if (allowSchemaEvolution || query.output.length < targetAttrs.length) {
      val userSpecifiedNames = if (session.sessionState.conf.caseSensitiveAnalysis) {
        query.output.map(a => (a.name, a)).toMap
      } else {
        CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
      }
      val tableSchema = deltaTable.initialSnapshot.metadata.schema
      if (tableSchema.length != targetAttrs.length) {
        // The target attributes may contain the metadata columns by design. Throwing an exception
        // here in case target attributes may have the metadata columns for Delta in future.
        throw DeltaErrors.schemaNotConsistentWithTarget(s"$tableSchema", s"$targetAttrs")
      }
      val nullAsDefault = deltaTable.spark.sessionState.conf.useNullsForMissingDefaultColumnValues
      deltaTable.initialSnapshot.metadata.schema.foreach { col =>
        if (!userSpecifiedNames.contains(col.name) &&
          !ColumnWithDefaultExprUtils.columnHasDefaultExpr(
            deltaTable.initialSnapshot.protocol, col, nullAsDefault)) {
          throw DeltaErrors.missingColumnsInInsertInto(col.name)
        }
      }
    }
  }

  /**
   * With Delta, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
   * of INSERT INTO. Here we check if we need to perform any schema adjustment for INSERT INTO by
   * name queries. We also check that any columns not in the list of user-specified columns must
   * have a default expression.
   *
   * @param isDfByNameInsert True for DataFrame by-name inserts, false for SQL by name inserts. On
   *                         the first analyzer pass, controls two behaviors: (1) whether missing
   *                         non-generated columns are an error, and (2) whether struct field
   *                         comparison respects session case-sensitivity. Subsequent passes always
   *                         see true, but it should be safe as we will have already thrown errors
   *                         for missing columns and renamed columns for SQL inserts.
   */
  protected def needsSchemaAdjustmentByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      deltaTable: DeltaTableV2,
      writeOptions: Map[String, String],
      isDfByNameInsert: Boolean = false): Boolean = {
    if (!isDfByNameInsert) insertIntoByNameMissingColumn(query, targetAttrs, deltaTable)
    val caseSensitive = session.sessionState.conf.caseSensitiveAnalysis
    val userSpecifiedNames = if (caseSensitive) {
      query.output.map(a => (a.name, a)).toMap
    } else {
      CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
    }
    val specifiedTargetAttrs = targetAttrs.filter(col => userSpecifiedNames.contains(col.name))
    !SchemaUtils.isReadCompatible(
      specifiedTargetAttrs.toStructType.asNullable,
      query.output.toStructType,
      typeWideningMode = getTypeWideningMode(deltaTable, writeOptions),
      // DataFrame by-name inserts allow missing nested columns; SQL inserts do not.
      allowMissingColumns = isDfByNameInsert,
      // DF by-name casting preserves source field names, so (correctly) respect case to avoid
      // infinite loops.
      caseSensitive = caseSensitive || !isDfByNameInsert,
      allowVoidTypeChange = isDfByNameInsert
    )
  }

  // Get cast operation for the level of strictness in the schema a user asked for
  private def getCastFunction: CastFunction = {
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        (input: Expression, dt: DataType, _) =>
          Cast(input, dt, Option(timeZone), ansiEnabled = false)
      case SQLConf.StoreAssignmentPolicy.ANSI =>
        (input: Expression, dt: DataType, name: String) => {
          val cast = Cast(input, dt, Option(timeZone), ansiEnabled = true)
          cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
          TableOutputResolver.checkCastOverflowInTableInsert(cast, name)
        }
      case SQLConf.StoreAssignmentPolicy.STRICT =>
        (input: Expression, dt: DataType, _) =>
          UpCast(input, dt)
    }
  }

  /**
   * Recursively casts struct data types positionally in case the source/target type differs.
   */
  private def addCastsToStructsByPosition(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      typeWideningMode: TypeWideningMode): NamedExpression = {
    if (source.length < target.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(
        tableName, source.length, target.length, Some(parent.qualifiedName))
    }
    // Extracts the field at a given index in the target schema. Only matches if the index is valid.
    object TargetIndex {
      def unapply(index: Int): Option[StructField] = target.lift(index)
    }

    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i @ TargetIndex(targetField)) =>
        targetField.dataType match {
          case t: StructType =>
            val subField = Alias(GetStructField(parent, i, Option(name)), targetField.name)(
              explicitMetadata = Option(metadata))
            addCastsToStructsByPosition(tableName, subField, nested, t, typeWideningMode)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + targetField.name
            throw DeltaErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
        }

      case (StructField(name, sourceType: AtomicType, _, _),
            i @ TargetIndex(StructField(targetName, targetType: AtomicType, _, targetMetadata)))
          if typeWideningMode.shouldWidenTo(fromType = targetType, toType = sourceType) =>
        Alias(
          GetStructField(parent, i, Option(name)),
          targetName)(explicitMetadata = Option(targetMetadata))
      case (sourceField, i @ TargetIndex(targetField)) =>
        Alias(
          getCastFunction(GetStructField(parent, i, Option(sourceField.name)),
            targetField.dataType, targetField.name),
          targetField.name)(explicitMetadata = Option(targetField.metadata))

      case (sourceField, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(
          GetStructField(parent, i, Option(sourceField.name)),
          sourceField.name)(explicitMetadata = Option(sourceField.metadata))
    }

    // Fix for null expansion caused by struct type cast by preserving NULL source structs.
    //
    // Problem: When inserting a struct column, if the source struct is NULL, the casting logic
    // will expand the NULL into a non-null struct with all fields set to NULL:
    //   NULL -> struct(field1: null, field2: null, ..., newField: null)
    //
    // Expected: The target struct should remain NULL when the source struct is NULL:
    //   NULL -> NULL
    //
    // Solution: Wrap the CreateStruct expression in an IF expression that preserves NULL:
    //   IF(source_struct IS NULL, NULL, CreateStruct(...))
    //
    // This is controlled by the DELTA_INSERT_PRESERVE_NULL_SOURCE_STRUCTS config.
    val createStructExpr = CreateStruct(fields)
    val wrappedWithNullPreservation =
      maybeWrapWithNullPreservationForInsert(
        sourceExpr = parent,
        createStructExpr = createStructExpr)
    Alias(wrappedWithNullPreservation, parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
  }

  /**
   * Recursively casts struct data types by matching fields by name.
   * Unlike addCastsToStructsByPosition, this preserves source field names (i.e. capitalization)
   * and lets downstream handle normalization.
   */
  private def addCastsToStructsByName(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      typeWideningMode: TypeWideningMode): NamedExpression = {
    val resolver = session.sessionState.conf.resolver
    val fields = source.map { sourceField =>
      val sourceIndex = source.fieldIndex(sourceField.name)
      val targetFieldOpt = target.find(t => resolver(t.name, sourceField.name))
      targetFieldOpt match {
        case Some(targetField) =>
          val expr = (sourceField.dataType, targetField.dataType) match {
            // Delegate nested complex types to addCastToColumn which dispatches
            // to the appropriate by-name handler (structs, arrays, maps).
            case (_: StructType, _: StructType)
                | (_: ArrayType, _: ArrayType) | (_: MapType, _: MapType)
                if sourceField.dataType != targetField.dataType =>
              val subField = Alias(
                GetStructField(parent, sourceIndex, Option(sourceField.name)),
                sourceField.name
              )()
              val targetAttr = AttributeReference(
                targetField.name, targetField.dataType, targetField.nullable)()
              addCastToColumn(subField, targetAttr, tableName, typeWideningMode, byName = true)

            case (_, _: NullType) =>
              GetStructField(parent, sourceIndex, Option(sourceField.name))
            case (sourceType: AtomicType, targetType: AtomicType)
              if typeWideningMode.shouldWidenTo(fromType = targetType, toType = sourceType) =>
              GetStructField(parent, sourceIndex, Option(sourceField.name))

            case _ =>
              getCastFunction(
                GetStructField(parent, sourceIndex, Option(sourceField.name)),
                targetField.dataType,
                targetField.name
              )
          }
          Alias(expr, sourceField.name)(explicitMetadata = Option(sourceField.metadata))

        case None =>
          // Source field doesn't exist in target - pass through as-is.
          Alias(
            GetStructField(parent, sourceIndex, Option(sourceField.name)),
            sourceField.name
          )(explicitMetadata = Option(sourceField.metadata))
      }
    }

    // Wrap the struct in an if statement to make sure that when a null is passed in, the struct
    // field is null instead of a non-null struct with a null for each nested field.
    val resultStruct = CreateStruct(fields)
    val createStructExpr = If(IsNull(parent), Literal(null, resultStruct.dataType), resultStruct)
    Alias(createStructExpr, parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
  }

  private def addCastsToArrayStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      sourceNullable: Boolean,
      typeWideningMode: TypeWideningMode,
      byName: Boolean = false): Expression = {
    val structConverter: (Expression, Expression) => Expression = (_, i) => {
      if (byName) {
        addCastsToStructsByName(tableName, Alias(GetArrayItem(parent, i), i.toString)(),
          source, target, typeWideningMode)
      } else {
        addCastsToStructsByPosition(tableName, Alias(GetArrayItem(parent, i), i.toString)(),
          source, target, typeWideningMode)
      }
    }
    val transformLambdaFunc = {
      val elementVar = NamedLambdaVariable("elementVar", source, sourceNullable)
      val indexVar = NamedLambdaVariable("indexVar", IntegerType, false)
      LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
    }
    ArrayTransform(parent, transformLambdaFunc)
  }

  /**
   * Recursively casts map data types in case the key/value type differs.
   */
  private def addCastsToMaps(
      tableName: String,
      parent: NamedExpression,
      sourceMapType: MapType,
      targetMapType: MapType,
      typeWideningMode: TypeWideningMode,
      byName: Boolean = false): Expression = {
    val transformedKeys =
      if (sourceMapType.keyType != targetMapType.keyType) {
        // Create a transformation for the keys
        ArrayTransform(MapKeys(parent), {
          val key = NamedLambdaVariable(
            "key", sourceMapType.keyType, nullable = false)

          val keyAttr = AttributeReference(
            "key", targetMapType.keyType, nullable = false)()

          val castedKey =
            addCastToColumn(
              key,
              keyAttr,
              tableName,
              typeWideningMode,
              byName
            )
          LambdaFunction(castedKey, Seq(key))
        })
      } else {
        MapKeys(parent)
      }

    val transformedValues =
      if (sourceMapType.valueType != targetMapType.valueType) {
        // Create a transformation for the values
        ArrayTransform(MapValues(parent), {
          val value = NamedLambdaVariable(
            "value", sourceMapType.valueType, sourceMapType.valueContainsNull)

          val valueAttr = AttributeReference(
            "value", targetMapType.valueType, sourceMapType.valueContainsNull)()

          val castedValue =
            addCastToColumn(
              value,
              valueAttr,
              tableName,
              typeWideningMode,
              byName
            )
          LambdaFunction(castedValue, Seq(value))
        })
      } else {
        MapValues(parent)
      }
    // Create new map from transformed keys and values
    MapFromArrays(transformedKeys, transformedValues)
  }
}
