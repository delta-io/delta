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

import scala.language.implicitConversions

import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructType}

/** Trait containing common utility methods for struct evolution nullness tests. */
trait MergeIntoStructEvolutionNullnessTestUtils extends MergeHelpers {

  /** Whether to preserve null source structs for struct evolution tests. */
  protected def preserveNullSourceStructs: Boolean

  /** Whether to preserve null source structs for UPDATE * specifically. */
  protected def preserveNullSourceStructsUpdateStar: Boolean

  /** Configurations for preserving null source structs. */
  protected val preserveNullStructsConfs: Seq[(String, String)] = Seq(
    DeltaSQLConf.DELTA_MERGE_PRESERVE_NULL_SOURCE_STRUCTS.key -> preserveNullSourceStructs.toString,
    DeltaSQLConf.DELTA_MERGE_PRESERVE_NULL_SOURCE_STRUCTS_UPDATE_STAR.key ->
      preserveNullSourceStructsUpdateStar.toString)

  // `SourceType`, `TargetType` and `ActionType` assume that the source and target tables
  // have a single top-level column `col` of struct type.
  protected object SourceType extends Enumeration {
    case class Val(displayName: String) extends super.Val
    val NonNullLeaves: Val = Val("non-null source leaves")
    val NullLeaves: Val = Val("null source leaves")
    val NullNestedStruct: Val = Val("null source nested struct")
    val NullNestedArray: Val = Val("null source nested array")
    val NullNestedMap: Val = Val("null source nested map")
    val NullCol: Val = Val("null source col")

    def getName(sourceType: Value): String =
      sourceType.asInstanceOf[Val].displayName
  }

  protected object TargetType extends Enumeration {
    case class Val(displayName: String) extends super.Val
    val NonNullLeaves: Val = Val("non-null target leaves")
    val NullLeaves: Val = Val("null target leaves")
    val NullTargetOnlyFields: Val = Val("null target-only fields")
    val NullCol: Val = Val("null target col")
    val Empty: Val = Val("empty target")

    def getName(targetType: Value): String =
      targetType.asInstanceOf[Val].displayName
  }

  protected object ActionType extends Enumeration {
    case class Val(displayName: String, clause: MergeClause) extends super.Val
    val UpdateStar: Val = Val("UPDATE *", update("*"))
    val UpdateCol: Val = Val("UPDATE t.col = s.col", update("t.col = s.col"))
    val UpdateColY: Val = Val("UPDATE t.col.y = s.col.y", update("t.col.y = s.col.y"))
    val InsertStar: Val = Val("INSERT *", insert("*"))
    val InsertCol: Val = Val("INSERT col", insert("(key, col) VALUES (s.key, s.col)"))

    implicit def toVal(v: Value): Val = v.asInstanceOf[Val]

    def getName(actionType: Value): String =
      actionType.asInstanceOf[Val].displayName

    def getClause(actionType: Value): MergeClause =
      actionType.asInstanceOf[Val].clause

    def isWholeStructAssignment(actionType: Value): Boolean =
      Seq(ActionType.UpdateCol, ActionType.InsertStar, ActionType.InsertCol).contains(actionType)
  }

  /** Casts Any to Map[String, Any]. Returns null if `value` is null. */
  protected def castToMap(value: Any): Map[String, Any] =
    if (value == null) null else value.asInstanceOf[Map[String, Any]]

  /** Gets a value from a map. Returns null if the map is null. */
  protected def getNestedValue(map: Map[String, Any], key: String): Any = {
    if (map == null) null else map(key)
  }

  /**
   * JSON mapper that preserves null values during serialization.
   * This is necessary because the default JsonUtils uses Include.NON_ABSENT which filters out
   * null values, which is not what we want for the nullness tests.
   * Uses ClassTagExtensions instead of deprecated ScalaObjectMapper for Scala 3 compatibility.
   */
  private val jsonMapper = {
    val mapper = new ObjectMapper() with ClassTagExtensions
    mapper.setSerializationInclusion(Include.ALWAYS)  // Preserve null values
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  /**
   * Converts a Scala object to JSON string, preserving null values.
   * Unlike org.apache.spark.sql.util.JsonUtils.toJson(), this method uses Include.ALWAYS
   * to ensure null values are preserved in the JSON output.
   * Uses ClassTag instead of deprecated Manifest for Scala 3 compatibility.
   */
  protected def toJsonWithNulls[T: scala.reflect.ClassTag](obj: T): String = {
    jsonMapper.writeValueAsString(obj)
  }

  /** Parses a JSON string to a Map[String, Any]. */
  protected def fromJsonToMap(jsonStr: String): Map[String, Any] = {
    jsonMapper.readValue[Map[String, Any]](jsonStr)
  }

  /**
   * Determines whether the target struct should be overwritten with null.
   *
   * @param sourceCol The source column value (can be null)
   * @param targetColOpt Optional target column value corresponding to sourceCol
   * @param actionType The action type
   * @param targetOnlyFieldKey The key of the target-only field to check (e.g., "z")
   * @return true if target should be overwritten with null, false otherwise
   */
  protected def shouldOverwriteWithNull(
      sourceCol: Map[String, Any],
      targetColOpt: Option[Map[String, Any]],
      actionType: ActionType.Value,
      targetOnlyFieldKey: String): Boolean = {
    sourceCol == null && preserveNullSourceStructs && (
      // `targetColOpt` being None means it's an INSERT
      targetColOpt.isEmpty ||
      ActionType.isWholeStructAssignment(actionType) ||
      (actionType == ActionType.UpdateStar && preserveNullSourceStructsUpdateStar &&
       getNestedValue(targetColOpt.get, targetOnlyFieldKey) == null)
    )
  }

  /** Represents a struct evolution nullness test case. */
  protected case class StructEvolutionNullnessTestCase(
    testName: String,
    sourceSchema: StructType,
    targetSchema: StructType,
    sourceData: String,
    targetData: Seq[String],
    actionClause: MergeClause,
    resultSchema: StructType,
    expectedResult: String,
    confs: Seq[(String, String)])

  /**
   * Generates test cases for struct evolution nullness tests.
   *
   * @param testNamePrefix Prefix for test names
   * @param sourceSchema Source table schema
   * @param targetSchema Target table schema
   * @param sourceTypes Source types to test
   * @param updateTargetTypes Target types to use for UPDATE operations
   * @param actionTypes Action types to test
   * @param generateResultSchemaFn Function to determine result schema based on action type
   * @param generateSourceRowFn Function to generate source row
   * @param generateTargetRowFn Function to generate target row
   * @param generateExpectedResultFn Function to generate expected result
   */
  protected def generateStructEvolutionNullnessTests(
      testNamePrefix: String,
      sourceSchema: StructType,
      targetSchema: StructType,
      sourceTypes: Seq[SourceType.Value],
      updateTargetTypes: Seq[TargetType.Value],
      actionTypes: Seq[ActionType.Value],
      generateResultSchemaFn: ActionType.Value => StructType,
      generateSourceRowFn: SourceType.Value => String,
      generateTargetRowFn: TargetType.Value => Option[String],
      generateExpectedResultFn: (String, Option[String], ActionType.Value) => String)
  : Seq[StructEvolutionNullnessTestCase] = {
    for {
      actionType <- actionTypes
      sourceType <- sourceTypes
      // For INSERT, only use Empty target; for UPDATE, use specified target types.
      targetType <- {
        if (actionType == ActionType.InsertStar || actionType == ActionType.InsertCol) {
          Seq(TargetType.Empty)
        } else {
          updateTargetTypes
        }
      }
    } yield {
      val sourceRowJson = generateSourceRowFn(sourceType)
      val targetRowJsonOpt = generateTargetRowFn(targetType)
      val expectedResultJson = generateExpectedResultFn(sourceRowJson, targetRowJsonOpt, actionType)

      StructEvolutionNullnessTestCase(
        testName =
          s"$testNamePrefix${SourceType.getName(sourceType)}, " +
          s"${TargetType.getName(targetType)}, " +
          s"${ActionType.getName(actionType)}",
        sourceSchema = sourceSchema,
        targetSchema = targetSchema,
        sourceData = sourceRowJson,
        targetData = targetRowJsonOpt.toSeq,
        actionClause = ActionType.getClause(actionType),
        resultSchema = generateResultSchemaFn(actionType),
        expectedResult = expectedResultJson,
        confs = preserveNullStructsConfs
      )
    }
  }
}

/**
 * Trait collecting tests verifying the nullness of the results for top-level struct evolution.
 */
trait MergeIntoTopLevelStructEvolutionNullnessTests
    extends MergeIntoSchemaEvolutionMixin
    with MergeIntoStructEvolutionNullnessTestUtils {
  self: MergeIntoTestUtils with SharedSparkSession =>

  private val testNamePrefix = "top-level struct - "

  private val topLevelStructTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType))

  private val topLevelStructSourceSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("x", IntegerType)
      .add("y", IntegerType))

  private val topLevelStructResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)
      .add("x", IntegerType))

  private def generateTopLevelStructSourceRow(sourceType: SourceType.Value): String = {
    sourceType match {
      case SourceType.NonNullLeaves =>
        """{"key":1,"col":{"x":1,"y":1}}"""
      case SourceType.NullLeaves =>
        """{"key":1,"col":{"x":null,"y":null}}"""
      case SourceType.NullCol =>
        """{"key":1,"col":null}"""
    }
  }

  private def generateTopLevelStructTargetRow(
      targetType: TargetType.Value): Option[String] = {
    targetType match {
      case TargetType.NonNullLeaves =>
        Some("""{"key":1,"col":{"y":2,"z":2}}""")
      case TargetType.NullTargetOnlyFields =>
        Some("""{"key":1,"col":{"y":2,"z":null}}""")
      case TargetType.NullCol =>
        Some("""{"key":1,"col":null}""")
      case TargetType.Empty =>
        None
    }
  }

  /**
   * Generates the expected result based on `sourceRowJson`, `targetRowJsonOpt`, and `actionType`.
   * Semantics:
   * - UPDATE *: field-level merge, preserves target-only fields (col.z).
   * - UPDATE t.col = s.col, INSERT: whole-struct assignment, nulls target-only fields.
   */
  private def generateTopLevelStructExpectedResult(
      sourceRowJson: String,
      targetRowJsonOpt: Option[String],
      actionType: ActionType.Value): String = {
    val sourceRow = fromJsonToMap(sourceRowJson)
    val targetRowOpt = targetRowJsonOpt.map(fromJsonToMap)

    val sourceCol = castToMap(sourceRow("col"))
    val targetColOpt = targetRowOpt.map(row => castToMap(row("col")))

    if (shouldOverwriteWithNull(sourceCol, targetColOpt, actionType, targetOnlyFieldKey = "z")) {
      return """{"key":1,"col":null}"""
    }

    val sourceX = getNestedValue(sourceCol, "x")
    val sourceY = getNestedValue(sourceCol, "y")

    val (resultX, resultY, resultZ) = actionType match {
      case ActionType.UpdateStar =>
        // UPDATE SET * preserves target-only field (col.z).
        val targetZ = getNestedValue(targetColOpt.get, "z")
        (sourceX, sourceY, targetZ)

      case ActionType.UpdateCol | ActionType.InsertStar | ActionType.InsertCol =>
        // Whole-struct assignments null out target-only field (col.z).
        (sourceX, sourceY, null)
    }

    val resultMap = Map(
      "key" -> 1,
      "col" -> Map("y" -> resultY, "z" -> resultZ, "x" -> resultX))

    toJsonWithNulls(resultMap)
  }

  private def generateTopLevelStructNullnessTests(): Seq[StructEvolutionNullnessTestCase] = {
    generateStructEvolutionNullnessTests(
      testNamePrefix = testNamePrefix,
      sourceSchema = topLevelStructSourceSchema,
      targetSchema = topLevelStructTargetSchema,
      sourceTypes = Seq(SourceType.NonNullLeaves, SourceType.NullLeaves, SourceType.NullCol),
      updateTargetTypes =
        Seq(TargetType.NonNullLeaves, TargetType.NullTargetOnlyFields, TargetType.NullCol),
      actionTypes =
        Seq(
          ActionType.UpdateStar, ActionType.UpdateCol, ActionType.InsertStar, ActionType.InsertCol),
      generateResultSchemaFn = _ => topLevelStructResultSchema,
      generateSourceRowFn = generateTopLevelStructSourceRow,
      generateTargetRowFn = generateTopLevelStructTargetRow,
      generateExpectedResultFn = generateTopLevelStructExpectedResult
    )
  }

  generateTopLevelStructNullnessTests().foreach { testCase =>
    testNestedStructsEvolution(testCase.testName)(
      target = testCase.targetData,
      source = Seq(testCase.sourceData),
      targetSchema = testCase.targetSchema,
      sourceSchema = testCase.sourceSchema,
      clauses = testCase.actionClause :: Nil,
      result = Seq(testCase.expectedResult),
      resultSchema = testCase.resultSchema,
      expectErrorWithoutEvolutionContains = "Cannot cast",
      confs = testCase.confs)
  }

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with null target-only field")(
    target = Seq("""{"key":1,"col":{"y":2,"z":null}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = topLevelStructTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
        """{"key":1,"col":null}"""
      } else {
        """{"key":1,"col":{"y":null,"z":null,"x":null}}"""
      }
    ),
    resultSchema = topLevelStructResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with non-null target-only field")(
    target = Seq("""{"key":1,"col":{"y":2,"z":2}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = topLevelStructTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq("""{"key":1,"col":{"y":null,"z":2,"x":null}}"""),
    resultSchema = topLevelStructResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  private val expectedResult = if (preserveNullSourceStructs) {
    """{"key":1,"col":null}"""
  } else {
    """{"key":1,"col":{"y":null,"z":null,"x":null}}"""
  }

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - UPDATE t.col = s.col")(
    target = Seq("""{"key":1,"col":{"y":2,"z":2}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = topLevelStructTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelStructResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT *")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = topLevelStructTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = insert("*") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelStructResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT (key, col)")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = topLevelStructTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = insert("(key, col) VALUES (s.key, s.col)") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelStructResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(
    s"${testNamePrefix}non-nullable target struct becomes nullable")(
    target = Seq("""{"key":1,"col":{"y":2,"z":2}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    // Target schema has non-nullable struct
    targetSchema = new StructType()
      .add("key", IntegerType)
      .add("col", new StructType()
        .add("y", IntegerType)
        .add("z", IntegerType), nullable = false),
    sourceSchema = topLevelStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(if (preserveNullSourceStructs) {
      """{"key":1,"col":null}"""
    } else {
      """{"key":1,"col":{"y":null,"z":null,"x":null}}"""
    }),
    // Result schema has nullable struct
    resultSchema = new StructType()
      .add("key", IntegerType)
      .add("col", new StructType()
        .add("y", IntegerType)
        .add("z", IntegerType)
        .add("x", IntegerType), nullable = true),
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  // Tests for multiple target-only fields
  private val multiTargetOnlyFieldTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)
      .add("w", IntegerType))

  private val multiTargetOnlyFieldResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)
      .add("w", IntegerType)
      .add("x", IntegerType))

  testNestedStructsEvolution(
      s"${testNamePrefix}multiple target-only fields - UPDATE * with all target-only fields null")(
    target = Seq("""{"key":1,"col":{"y":2,"z":null,"w":null}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = multiTargetOnlyFieldTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
        """{"key":1,"col":null}"""
      } else {
        """{"key":1,"col":{"y":null,"z":null,"w":null,"x":null}}"""
      }
    ),
    resultSchema = multiTargetOnlyFieldResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(
      s"${testNamePrefix}multiple target-only fields - UPDATE * with a non-null target-only field")(
    target = Seq("""{"key":1,"col":{"y":2,"z":5,"w":null}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = multiTargetOnlyFieldTargetSchema,
    sourceSchema = topLevelStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq("""{"key":1,"col":{"y":null,"z":5,"w":null,"x":null}}"""),
    resultSchema = multiTargetOnlyFieldResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
}

/**
 * Trait collecting tests verifying the nullness of the results for nested struct evolution.
 */
trait MergeIntoNestedStructEvolutionNullnessTests
    extends MergeIntoSchemaEvolutionMixin
    with MergeIntoStructEvolutionNullnessTestUtils {
  self: MergeIntoTestUtils with SharedSparkSession =>

  private val testNamePrefix = "nested struct - "

  private val nestedStructTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType))
      .add("z", new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType)))

  private val nestedStructSourceSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))
      .add("y", new StructType()
        .add("c", IntegerType)
        .add("d", IntegerType)))

  // Result schema for UPDATE * and UPDATE t.col = s.col: adds both col.x and col.y.c
  private val nestedStructColEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)
        .add("c", IntegerType))
      .add("z", new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType))
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)))

  // Result schema for UPDATE t.col.y = s.col.y: only adds col.y.c (no col.x)
  private val nestedStructColYEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)
        .add("c", IntegerType))
      .add("z", new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType)))

  private def generateNestedStructSourceRow(sourceType: SourceType.Value): String = {
    sourceType match {
      case SourceType.NonNullLeaves =>
        """{"key":1,"col":{"x":{"a":10,"b":20},"y":{"c":30,"d":40}}}"""
      case SourceType.NullLeaves =>
        """{"key":1,"col":{"x":{"a":null,"b":null},"y":{"c":null,"d":null}}}"""
      case SourceType.NullNestedStruct =>
        """{"key":1,"col":{"x":null,"y":null}}"""
      case SourceType.NullCol =>
        """{"key":1,"col":null}"""
    }
  }

  private def generateNestedStructTargetRow(
      targetType: TargetType.Value): Option[String] = {
    targetType match {
      case TargetType.NonNullLeaves =>
        Some("""{"key":1,"col":{"y":{"d":4,"e":5},"z":{"f":6,"g":7}}}""")
      case TargetType.NullLeaves =>
        Some("""{"key":1,"col":{"y":{"d":null,"e":null},"z":{"f":null,"g":null}}}""")
      case TargetType.NullTargetOnlyFields =>
        Some("""{"key":1,"col":{"y":{"d":4,"e":null},"z":null}}""")
      case TargetType.NullCol =>
        Some("""{"key":1,"col":null}""")
      case TargetType.Empty =>
        None
    }
  }

  /**
   * Generates the expected result based on `sourceRowJson`, `targetRowJsonOpt`, and `actionType`.
   * Semantics:
   * - UPDATE *: field-level merge, preserves target-only fields.
   * - UPDATE t.col = s.col, INSERT: whole-struct assignment, nulls target-only fields.
   * - UPDATE t.col.y = s.col.y: whole-struct assignment, nulls target-only fields.
   */
  private def generateNestedStructExpectedResult(
      sourceRowJson: String,
      targetRowJsonOpt: Option[String],
      actionType: ActionType.Value): String = {
    val sourceRow = fromJsonToMap(sourceRowJson)
    val targetRowOpt = targetRowJsonOpt.map(fromJsonToMap)

    val sourceCol = castToMap(sourceRow("col"))
    val targetColOpt = targetRowOpt.map(row => castToMap(row("col")))

    if (shouldOverwriteWithNull(sourceCol, targetColOpt, actionType, targetOnlyFieldKey = "z")) {
      return """{"key":1,"col":null}"""
    }

    val sourceX = castToMap(getNestedValue(sourceCol, "x"))

    // col.x is source-only.
    val resultXOpt: Option[Any] = actionType match {
      case ActionType.UpdateStar =>
        if (sourceX == null) {
          if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
            Some(null)
          } else {
            Some(Map("a" -> null, "b" -> null))
          }
        } else {
          // Keep struct as is.
          Some(sourceX)
        }

      case ActionType.UpdateCol | ActionType.InsertStar | ActionType.InsertCol =>
        // Whole-struct assignment: null stays null, struct stays struct.
        Some(sourceX)

      case ActionType.UpdateColY =>
        // col.x is not added in result.
        None
    }

    // col.y exists in both the source and target.
    val sourceY = castToMap(getNestedValue(sourceCol, "y"))
    val targetY = targetColOpt.map(col => castToMap(getNestedValue(col, "y")))
    val resultY: Any = {
      if (shouldOverwriteWithNull(sourceY, targetY, actionType, targetOnlyFieldKey = "e")) {
        null
      } else {
        val sourceD = getNestedValue(sourceY, "d")
        val sourceC = getNestedValue(sourceY, "c")
        actionType match {
          case ActionType.UpdateStar =>
            // Update * preserve target-only field (col.y.e)
            val targetE = getNestedValue(targetY.get, "e")
            Map("d" -> sourceD, "e" -> targetE, "c" -> sourceC)

          case ActionType.UpdateCol | ActionType.UpdateColY |
               ActionType.InsertStar | ActionType.InsertCol =>
            // Whole-struct assignment nulls out target-only field (col.y.e).
            if (sourceY == null && preserveNullSourceStructs) {
              null
            } else {
              Map("d" -> sourceD, "e" -> null, "c" -> sourceC)
            }
        }
      }
    }

    // col.z is target-only.
    val resultZ: Any = actionType match {
      case ActionType.UpdateStar | ActionType.UpdateColY =>
        val targetCol = targetColOpt.get
        val targetZ = castToMap(getNestedValue(targetCol, "z"))
        // UPDATE * preserves target-only field (col.z);
        // UPDATE col.y = s.col.y does not change t.col.z.
        targetZ

      case ActionType.UpdateCol | ActionType.InsertStar | ActionType.InsertCol =>
        // Whole-struct assignment nulls out target-only field (col.z).
        null
    }

    val colMap = resultXOpt match {
      case Some(resultX) =>
        Map("y" -> resultY, "z" -> resultZ, "x" -> resultX)
      case None =>
        Map("y" -> resultY, "z" -> resultZ)
    }

    val resultMap = Map("key" -> 1, "col" -> colMap)
    toJsonWithNulls(resultMap)
  }

  private def getNestedStructResultSchema(actionType: ActionType.Value): StructType = {
    actionType match {
      case ActionType.UpdateStar | ActionType.UpdateCol |
           ActionType.InsertStar | ActionType.InsertCol =>
        nestedStructColEvolutionResultSchema
      case ActionType.UpdateColY =>
        nestedStructColYEvolutionResultSchema
    }
  }

  private def generateNestedStructNullnessTests(): Seq[StructEvolutionNullnessTestCase] = {
    generateStructEvolutionNullnessTests(
      testNamePrefix = testNamePrefix,
      sourceSchema = nestedStructSourceSchema,
      targetSchema = nestedStructTargetSchema,
      sourceTypes =
        Seq(
          SourceType.NonNullLeaves,
          SourceType.NullLeaves,
          SourceType.NullNestedStruct,
          SourceType.NullCol),
      updateTargetTypes =
        Seq(
          TargetType.NonNullLeaves,
          TargetType.NullLeaves,
          TargetType.NullTargetOnlyFields,
          TargetType.NullCol),
      actionTypes =
        Seq(
          ActionType.UpdateStar,
          ActionType.UpdateCol,
          ActionType.UpdateColY,
          ActionType.InsertStar,
          ActionType.InsertCol),
      generateResultSchemaFn = getNestedStructResultSchema,
      generateSourceRowFn = generateNestedStructSourceRow,
      generateTargetRowFn = generateNestedStructTargetRow,
      generateExpectedResultFn = generateNestedStructExpectedResult
    )
  }

  generateNestedStructNullnessTests().foreach { testCase =>
    testNestedStructsEvolution(testCase.testName)(
      target = testCase.targetData,
      source = Seq(testCase.sourceData),
      targetSchema = testCase.targetSchema,
      sourceSchema = testCase.sourceSchema,
      clauses = testCase.actionClause :: Nil,
      result = Seq(testCase.expectedResult),
      resultSchema = testCase.resultSchema,
      expectErrorWithoutEvolutionContains = "Cannot cast",
      confs = testCase.confs)
  }

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with null target-only field")(
    target = Seq("""{"key":1,"col":{"y":{"d":2,"e":null},"z":null}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedStructTargetSchema,
    sourceSchema = nestedStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
        """{"key":1,"col":null}"""
      } else {
        """{"key":1,"col":{"y":{"d":null,"e":null,"c":null},"z":null,"x":{"a":null,"b":null}}}"""
      }
    ),
    resultSchema = nestedStructColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  // scalastyle:off line.size.limit
  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with non-null target-only field")(
    target = Seq("""{"key":1,"col":{"y":{"d":2,"e":2},"z":{"f":2,"g":2}}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedStructTargetSchema,
    sourceSchema = nestedStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
        """{"key":1,"col":{"y":{"d":null,"e":2,"c":null},"z":{"f":2,"g":2},"x":null}}"""
      } else {
        """{"key":1,"col":{"y":{"d":null,"e":2,"c":null},"z":{"f":2,"g":2},"x":{"a":null,"b":null}}}"""
      }
    ),
    resultSchema = nestedStructColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
  // scalastyle:on line.size.limit

  private val expectedResult = if (preserveNullSourceStructs) {
    """{"key":1,"col":null}"""
  } else {
    """{"key":1,"col":{"y":{"d":null,"e":null,"c":null},"z":null,"x":null}}"""
  }

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - UPDATE t.col = s.col")(
    target = Seq("""{"key":1,"col":{"y":{"d":2,"e":2},"z":{"f":2,"g":2}}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedStructTargetSchema,
    sourceSchema = nestedStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedStructColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT *")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedStructTargetSchema,
    sourceSchema = nestedStructSourceSchema,
    clauses = insert("*") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedStructColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT (key, col)")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedStructTargetSchema,
    sourceSchema = nestedStructSourceSchema,
    clauses = insert("(key, col) VALUES (s.key, s.col)") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedStructColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
}

/**
 * Trait collecting tests verifying the nullness of the results for array-of-struct evolution.
 */
trait MergeIntoTopLevelArrayStructEvolutionNullnessTests
    extends MergeIntoSchemaEvolutionMixin
    with MergeIntoStructEvolutionNullnessTestUtils {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import org.apache.spark.sql.types.ArrayType

  private val testNamePrefix = "top-level array-of-struct - "

  private val topLevelArrayStructTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", ArrayType(new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)))

  private val topLevelArrayStructSourceSchema = new StructType()
    .add("key", IntegerType)
    .add("col", ArrayType(new StructType()
      .add("x", IntegerType)
      .add("y", IntegerType)))

  // Result schema: adds col[].x
  private val topLevelArrayStructEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", ArrayType(new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)
      .add("x", IntegerType)))

  private def generateTopLevelArrayStructSourceRow(
      sourceType: SourceType.Value): String = {
    sourceType match {
      case SourceType.NonNullLeaves =>
        """{"key":1,"col":[{"x":10,"y":20},{"x":30,"y":40}]}"""
      case SourceType.NullLeaves =>
        """{"key":1,"col":[{"x":null,"y":null},{"x":null,"y":null}]}"""
      case SourceType.NullNestedStruct =>
        """{"key":1,"col":[null,null]}"""
      case SourceType.NullCol =>
        """{"key":1,"col":null}"""
    }
  }

  private def generateTopLevelArrayStructTargetRow(
      targetType: TargetType.Value): Option[String] = {
    targetType match {
      case TargetType.NonNullLeaves =>
        Some("""{"key":1,"col":[{"y":2,"z":3},{"y":4,"z":5}]}""")
      case TargetType.Empty =>
        None
    }
  }

  /**
   * Generates the expected result based on `sourceRowJson`, `targetRowJsonOpt`, and `actionType`.
   * Semantics: Entire arrays are overwritten, and structs within the array evolve.
   * Note: `targetRowJsonOpt` and `actionType` are not used since arrays are always overwritten.
   *       They are added to match the data type of
   *       `generateStructEvolutionNullnessTests.generateExpectedResultFn`.
   *
   */
  private def generateTopLevelArrayStructExpectedResult(
      sourceRowJson: String,
      targetRowJsonOpt: Option[String],
      actionType: ActionType.Value): String = {
    val sourceRow = fromJsonToMap(sourceRowJson)

    val sourceCol = sourceRow("col")
    val resultCol = if (sourceCol == null) {
      null
    } else {
      val sourceArray = sourceCol.asInstanceOf[Seq[Any]]
      sourceArray.map { elem =>
        if (elem == null) {
          if (preserveNullSourceStructs) {
            null
          } else {
            Map("y" -> null, "z" -> null, "x" -> null)
          }
        } else {
          val sourceStruct = elem.asInstanceOf[Map[String, Any]]
          Map(
            "y" -> sourceStruct("y"),
            // Target-only field `z` in array element structs is added as null.
            "z" -> null,
            "x" -> sourceStruct("x")
          )
        }
      }
    }

    val resultMap = Map("key" -> 1, "col" -> resultCol)
    toJsonWithNulls(resultMap)
  }

  /**
   * Generates test cases for combinations of source type, target type, and action type.
   */
  private def generateTopLevelArrayStructNullnessTests(): Seq[StructEvolutionNullnessTestCase] = {
    generateStructEvolutionNullnessTests(
      testNamePrefix = testNamePrefix,
      sourceSchema = topLevelArrayStructSourceSchema,
      targetSchema = topLevelArrayStructTargetSchema,
      sourceTypes = Seq(
        SourceType.NonNullLeaves,
        SourceType.NullLeaves,
        SourceType.NullNestedStruct,
        SourceType.NullCol),
      updateTargetTypes = Seq(TargetType.NonNullLeaves),
      actionTypes = Seq(
        ActionType.UpdateStar,
        ActionType.UpdateCol,
        ActionType.InsertStar,
        ActionType.InsertCol),
      generateResultSchemaFn = _ => topLevelArrayStructEvolutionResultSchema,
      generateSourceRowFn = generateTopLevelArrayStructSourceRow,
      generateTargetRowFn = generateTopLevelArrayStructTargetRow,
      generateExpectedResultFn = generateTopLevelArrayStructExpectedResult
    )
  }

  generateTopLevelArrayStructNullnessTests().foreach { testCase =>
    testNestedStructsEvolution(testCase.testName)(
      target = testCase.targetData,
      source = Seq(testCase.sourceData),
      targetSchema = testCase.targetSchema,
      sourceSchema = testCase.sourceSchema,
      clauses = testCase.actionClause :: Nil,
      result = Seq(testCase.expectedResult),
      resultSchema = testCase.resultSchema,
      expectErrorWithoutEvolutionContains = "Cannot cast",
      confs = testCase.confs)
  }

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE *")(
    target = Seq("""{"key":1,"col":[{"y":2,"z":2}]}"""),
    source = Seq("""{"key":1,"col":[null]}"""),
    targetSchema = topLevelArrayStructTargetSchema,
    sourceSchema = topLevelArrayStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs) {
        """{"key":1,"col":[null]}"""
      } else {
        """{"key":1,"col":[{"y":null,"z":null,"x":null}]}"""
      }
    ),
    resultSchema = topLevelArrayStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  private val expectedResult = if (preserveNullSourceStructs) {
    """{"key":1,"col":[null]}"""
  } else {
    """{"key":1,"col":[{"y":null,"z":null,"x":null}]}"""
  }

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - UPDATE t.col = s.col")(
    target = Seq("""{"key":1,"col":[{"y":2,"z":2}]}"""),
    source = Seq("""{"key":1,"col":[null]}"""),
    targetSchema = topLevelArrayStructTargetSchema,
    sourceSchema = topLevelArrayStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelArrayStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT *")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":[null]}"""),
    targetSchema = topLevelArrayStructTargetSchema,
    sourceSchema = topLevelArrayStructSourceSchema,
    clauses = insert("*") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelArrayStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT (key, col)")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":[null]}"""),
    targetSchema = topLevelArrayStructTargetSchema,
    sourceSchema = topLevelArrayStructSourceSchema,
    clauses = insert("(key, col) VALUES (s.key, s.col)") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelArrayStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
}

/**
 * Trait collecting tests verifying the nullness of the results for nested array-of-struct
 * evolution.
 */
trait MergeIntoNestedArrayStructEvolutionNullnessTests
    extends MergeIntoSchemaEvolutionMixin
    with MergeIntoStructEvolutionNullnessTestUtils {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import org.apache.spark.sql.types.ArrayType

  private val testNamePrefix = "nested array-of-struct - "

  // Nested arrays: col is a struct containing multiple array-of-struct fields.
  private val nestedArrayStructTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", ArrayType(new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)))
      .add("z", ArrayType(new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType))))

  private val nestedArrayStructSourceSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("x", ArrayType(new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)))
      .add("y", ArrayType(new StructType()
        .add("c", IntegerType)
        .add("d", IntegerType))))

  // Result schema for UPDATE * and UPDATE t.col = s.col: adds both col.x and col.y[].c.
  private val nestedArrayColEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", ArrayType(new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)
        .add("c", IntegerType)))
      .add("z", ArrayType(new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType)))
      .add("x", ArrayType(new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))))

  // Result schema for UPDATE t.col.y = s.col.y: only adds col.y[].c (no col.x).
  private val nestedArrayColYEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", ArrayType(new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)
        .add("c", IntegerType)))
      .add("z", ArrayType(new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType))))

  private def generateNestedArraySourceRow(sourceType: SourceType.Value): String = {
    sourceType match {
      case SourceType.NonNullLeaves =>
        """{"key":1,"col":{"x":[{"a":10,"b":20}],"y":[{"c":30,"d":40}]}}"""
      case SourceType.NullLeaves =>
        """{"key":1,"col":{"x":[{"a":null,"b":null}],"y":[{"c":null,"d":null}]}}"""
      case SourceType.NullNestedStruct =>
        """{"key":1,"col":{"x":[null],"y":[null]}}"""
      case SourceType.NullNestedArray =>
        """{"key":1,"col":{"x":null,"y":null}}"""
      case SourceType.NullCol =>
        """{"key":1,"col":null}"""
    }
  }

  private def generateNestedArrayTargetRow(
      targetType: TargetType.Value): Option[String] = {
    targetType match {
      case TargetType.NonNullLeaves =>
        Some("""{"key":1,"col":{"y":[{"d":4,"e":5}],"z":[{"f":6,"g":7}]}}""")
      case TargetType.NullLeaves =>
        Some("""{"key":1,"col":{"y":[{"d":null,"e":null}],"z":[{"f":null,"g":null}]}}""")
      case TargetType.NullTargetOnlyFields =>
        Some("""{"key":1,"col":{"y":[{"d":4,"e":null}],"z":null}}""")
      case TargetType.NullCol =>
        Some("""{"key":1,"col":null}""")
      case TargetType.Empty =>
        None
    }
  }

  /**
   * Generates the expected result based on `sourceRowJson`, `targetRowJsonOpt`, and `actionType`.
   * Semantics: col struct evolves, nested arrays are overwritten, and structs within the
   * array evolve.
   */
  private def generateNestedArrayExpectedResult(
      sourceRowJson: String,
      targetRowJsonOpt: Option[String],
      actionType: ActionType.Value): String = {
    val sourceRow = fromJsonToMap(sourceRowJson)
    val targetRowOpt = targetRowJsonOpt.map(fromJsonToMap)

    val sourceCol = castToMap(sourceRow("col"))
    val targetColOpt = targetRowOpt.map(row => castToMap(row("col")))

    if (shouldOverwriteWithNull(sourceCol, targetColOpt, actionType, targetOnlyFieldKey = "z")) {
      return """{"key":1,"col":null}"""
    }

    // col.x is source-only.
    val sourceX = getNestedValue(sourceCol, "x")
    val resultXOpt: Option[Any] = actionType match {
      case ActionType.UpdateStar | ActionType.UpdateCol |
           ActionType.InsertStar | ActionType.InsertCol =>
        // col.x is kept as is.
        Some(sourceX)

      case ActionType.UpdateColY =>
        // col.x is not added in the result schema.
        None
    }

    val sourceY = getNestedValue(sourceCol, "y")
    // col.y exists in both the source and target.
    // Semantics: replace entire array and evolve struct within the array.
    val resultY: Any = if (sourceY == null) {
      null
    } else {
      val sourceYArray = sourceY.asInstanceOf[Seq[Any]]
      sourceYArray.map { elem =>
        if (elem == null) {
          if (preserveNullSourceStructs) {
            null
          } else {
            Map("d" -> null, "e" -> null, "c" -> null)
          }
        } else {
          val sourceYStruct = elem.asInstanceOf[Map[String, Any]]
          Map(
            "d" -> sourceYStruct("d"),
            // Target-only field `e` in array element structs is added as null.
            "e" -> null,
            "c" -> sourceYStruct("c")
          )
        }
      }
    }

    // col.z is target-only.
    val resultZ: Any = actionType match {
      case ActionType.UpdateStar | ActionType.UpdateColY =>
        val targetCol = targetColOpt.get
        val targetZ = getNestedValue(targetCol, "z")
        // UPDATE * preserves target-only field (col.z).
        // UPDATE col.y = s.col.y preserves fields not in assignment (col.z).
        targetZ

      case ActionType.UpdateCol | ActionType.InsertStar | ActionType.InsertCol =>
        // Whole-struct assignment nulls target-only field (col.z).
        null
    }

    val colMap = resultXOpt match {
      case Some(resultX) =>
        Map("y" -> resultY, "z" -> resultZ, "x" -> resultX)
      case None =>
        Map("y" -> resultY, "z" -> resultZ)
    }

    val resultMap = Map("key" -> 1, "col" -> colMap)
    toJsonWithNulls(resultMap)
  }

  private def getNestedArrayResultSchema(actionType: ActionType.Value): StructType = {
    actionType match {
      case ActionType.UpdateStar | ActionType.UpdateCol |
           ActionType.InsertStar | ActionType.InsertCol =>
        nestedArrayColEvolutionResultSchema
      case ActionType.UpdateColY =>
        nestedArrayColYEvolutionResultSchema
    }
  }

  /**
   * Generates test cases for combinations of source type, target type, and action type.
   */
  private def generateNestedArrayStructNullnessTests(): Seq[StructEvolutionNullnessTestCase] = {
    generateStructEvolutionNullnessTests(
      testNamePrefix = testNamePrefix,
      sourceSchema = nestedArrayStructSourceSchema,
      targetSchema = nestedArrayStructTargetSchema,
      sourceTypes = Seq(
        SourceType.NonNullLeaves,
        SourceType.NullLeaves,
        SourceType.NullNestedStruct,
        SourceType.NullNestedArray,
        SourceType.NullCol),
      updateTargetTypes = Seq(
        TargetType.NonNullLeaves,
        TargetType.NullLeaves,
        TargetType.NullTargetOnlyFields,
        TargetType.NullCol),
      actionTypes = Seq(
        ActionType.UpdateStar,
        ActionType.UpdateCol,
        ActionType.UpdateColY,
        ActionType.InsertStar,
        ActionType.InsertCol),
      generateResultSchemaFn = getNestedArrayResultSchema,
      generateSourceRowFn = generateNestedArraySourceRow,
      generateTargetRowFn = generateNestedArrayTargetRow,
      generateExpectedResultFn = generateNestedArrayExpectedResult
    )
  }

  generateNestedArrayStructNullnessTests().foreach { testCase =>
    testNestedStructsEvolution(testCase.testName)(
      target = testCase.targetData,
      source = Seq(testCase.sourceData),
      targetSchema = testCase.targetSchema,
      sourceSchema = testCase.sourceSchema,
      clauses = testCase.actionClause :: Nil,
      result = Seq(testCase.expectedResult),
      resultSchema = testCase.resultSchema,
      expectErrorWithoutEvolutionContains = "Cannot cast",
      confs = testCase.confs)
  }

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with null target-only field")(
    target = Seq("""{"key":1,"col":{"y":[{"d":2,"e":2}],"z":null}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedArrayStructTargetSchema,
    sourceSchema = nestedArrayStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
        """{"key":1,"col":null}"""
      } else {
        """{"key":1,"col":{"y":null,"z":null,"x":null}}"""
      }
    ),
    resultSchema = nestedArrayColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with non-null target-only field")(
    target = Seq("""{"key":1,"col":{"y":[{"d":2,"e":2}],"z":[{"f":2,"g":2}]}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedArrayStructTargetSchema,
    sourceSchema = nestedArrayStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq("""{"key":1,"col":{"y":null,"z":[{"f":2,"g":2}],"x":null}}"""),
    resultSchema = nestedArrayColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  private val expectedResult = if (preserveNullSourceStructs) {
    """{"key":1,"col":null}"""
  } else {
    """{"key":1,"col":{"y":null,"z":null,"x":null}}"""
  }

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - UPDATE t.col = s.col")(
    target = Seq("""{"key":1,"col":{"y":[{"d":2,"e":2}],"z":[{"f":2,"g":2}]}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedArrayStructTargetSchema,
    sourceSchema = nestedArrayStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedArrayColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT *")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedArrayStructTargetSchema,
    sourceSchema = nestedArrayStructSourceSchema,
    clauses = insert("*") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedArrayColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT (key, col)")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedArrayStructTargetSchema,
    sourceSchema = nestedArrayStructSourceSchema,
    clauses = insert("(key, col) VALUES (s.key, s.col)") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedArrayColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
}

/**
 * Trait collecting tests verifying the nullness of the results for map-of-struct evolution.
 */
trait MergeIntoTopLevelMapStructEvolutionNullnessTests
    extends MergeIntoSchemaEvolutionMixin
    with MergeIntoStructEvolutionNullnessTestUtils {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import org.apache.spark.sql.types.{MapType, StringType}

  private val testNamePrefix = "top-level map-of-struct - "

  private val topLevelMapStructTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", MapType(StringType, new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)))

  private val topLevelMapStructSourceSchema = new StructType()
    .add("key", IntegerType)
    .add("col", MapType(StringType, new StructType()
      .add("x", IntegerType)
      .add("y", IntegerType)))

  // Result schema: adds col{}.x
  private val topLevelMapStructEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", MapType(StringType, new StructType()
      .add("y", IntegerType)
      .add("z", IntegerType)
      .add("x", IntegerType)))

  private def generateTopLevelMapStructSourceRow(
      sourceType: SourceType.Value): String = {
    sourceType match {
      case SourceType.NonNullLeaves =>
        """{"key":1,"col":{"k1":{"x":10,"y":20},"k2":{"x":30,"y":40}}}"""
      case SourceType.NullLeaves =>
        """{"key":1,"col":{"k1":{"x":null,"y":null},"k2":{"x":null,"y":null}}}"""
      case SourceType.NullNestedStruct =>
        """{"key":1,"col":{"k1":null,"k2":null}}"""
      case SourceType.NullCol =>
        """{"key":1,"col":null}"""
    }
  }

  private def generateTopLevelMapStructTargetRow(
      targetType: TargetType.Value): Option[String] = {
    targetType match {
      case TargetType.NonNullLeaves =>
        Some("""{"key":1,"col":{"k2":{"y":2,"z":3},"k3":{"y":4,"z":5}}}""")
      case TargetType.Empty =>
        None
    }
  }

  /**
   * Generates the expected result based on `sourceRowJson`, `targetRowJsonOpt`, and `actionType`.
   * Semantics: Entire maps are overwritten, and structs within the map evolve.
   * Note: `targetRowJsonOpt` and `actionType` are not used since maps are always overwritten.
   *       They are added to match the data type of
   *       `generateStructEvolutionNullnessTests.generateExpectedResultFn`.
   *
   */
  private def generateTopLevelMapStructExpectedResult(
      sourceRowJson: String,
      targetRowJsonOpt: Option[String],
      actionType: ActionType.Value): String = {
    val sourceRow = fromJsonToMap(sourceRowJson)

    val sourceCol = sourceRow("col")
    val resultCol = if (sourceCol == null) {
      null
    } else {
      val sourceMap = sourceCol.asInstanceOf[Map[String, Any]]
      sourceMap.map { case (key, value) =>
        val resultValue = if (value == null) {
          if (preserveNullSourceStructs) {
            null
          } else {
            Map("y" -> null, "z" -> null, "x" -> null)
          }
        } else {
          val sourceStruct = value.asInstanceOf[Map[String, Any]]
          Map(
            "y" -> sourceStruct("y"),
            // Target-only field `z` in map value structs is added as null.
            "z" -> null,
            "x" -> sourceStruct("x")
          )
        }
        key -> resultValue
      }
    }

    val resultMap = Map("key" -> 1, "col" -> resultCol)
    toJsonWithNulls(resultMap)
  }

  /**
   * Generates test cases for combinations of source type, target type, and action type.
   */
  private def generateTopLevelMapStructNullnessTests(): Seq[StructEvolutionNullnessTestCase] = {
    generateStructEvolutionNullnessTests(
      testNamePrefix = testNamePrefix,
      sourceSchema = topLevelMapStructSourceSchema,
      targetSchema = topLevelMapStructTargetSchema,
      sourceTypes = Seq(
        SourceType.NonNullLeaves,
        SourceType.NullLeaves,
        SourceType.NullNestedStruct,
        SourceType.NullCol),
      updateTargetTypes = Seq(TargetType.NonNullLeaves),
      actionTypes = Seq(
        ActionType.UpdateStar,
        ActionType.UpdateCol,
        ActionType.InsertStar,
        ActionType.InsertCol),
      generateResultSchemaFn = _ => topLevelMapStructEvolutionResultSchema,
      generateSourceRowFn = generateTopLevelMapStructSourceRow,
      generateTargetRowFn = generateTopLevelMapStructTargetRow,
      generateExpectedResultFn = generateTopLevelMapStructExpectedResult
    )
  }

  generateTopLevelMapStructNullnessTests().foreach { testCase =>
    testNestedStructsEvolution(testCase.testName)(
      target = testCase.targetData,
      source = Seq(testCase.sourceData),
      targetSchema = testCase.targetSchema,
      sourceSchema = testCase.sourceSchema,
      clauses = testCase.actionClause :: Nil,
      result = Seq(testCase.expectedResult),
      resultSchema = testCase.resultSchema,
      expectErrorWithoutEvolutionContains = "Cannot cast",
      confs = testCase.confs)
  }

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE *")(
    target = Seq("""{"key":1,"col":{"k1":{"y":2,"z":2}}}"""),
    source = Seq("""{"key":1,"col":{"k1":null}}"""),
    targetSchema = topLevelMapStructTargetSchema,
    sourceSchema = topLevelMapStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs) {
        """{"key":1,"col":{"k1":null}}"""
      } else {
        """{"key":1,"col":{"k1":{"y":null,"z":null,"x":null}}}"""
      }
    ),
    resultSchema = topLevelMapStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  private val expectedResult = if (preserveNullSourceStructs) {
    """{"key":1,"col":{"k1":null}}"""
  } else {
    """{"key":1,"col":{"k1":{"y":null,"z":null,"x":null}}}"""
  }

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - UPDATE t.col = s.col")(
    target = Seq("""{"key":1,"col":{"k1":{"y":2,"z":2}}}"""),
    source = Seq("""{"key":1,"col":{"k1":null}}"""),
    targetSchema = topLevelMapStructTargetSchema,
    sourceSchema = topLevelMapStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelMapStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT *")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":{"k1":null}}"""),
    targetSchema = topLevelMapStructTargetSchema,
    sourceSchema = topLevelMapStructSourceSchema,
    clauses = insert("*") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelMapStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT (key, col)")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":{"k1":null}}"""),
    targetSchema = topLevelMapStructTargetSchema,
    sourceSchema = topLevelMapStructSourceSchema,
    clauses = insert("(key, col) VALUES (s.key, s.col)") :: Nil,
    result = Seq(expectedResult),
    resultSchema = topLevelMapStructEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
}

/**
 * Trait collecting tests verifying the nullness of the results for nested map-of-struct
 * evolution.
 */
trait MergeIntoNestedMapStructEvolutionNullnessTests
    extends MergeIntoSchemaEvolutionMixin
    with MergeIntoStructEvolutionNullnessTestUtils {
  self: MergeIntoTestUtils with SharedSparkSession =>

  import org.apache.spark.sql.types.{MapType, StringType}

  private val testNamePrefix = "nested map-of-struct - "

  // Nested maps: col is a struct containing multiple map-of-struct fields.
  private val nestedMapStructTargetSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", MapType(StringType, new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)))
      .add("z", MapType(StringType, new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType))))

  private val nestedMapStructSourceSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("x", MapType(StringType, new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType)))
      .add("y", MapType(StringType, new StructType()
        .add("c", IntegerType)
        .add("d", IntegerType))))

  // Result schema for UPDATE * and UPDATE t.col = s.col: adds both col.x and col.y{}.c.
  private val nestedMapColEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", MapType(StringType, new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)
        .add("c", IntegerType)))
      .add("z", MapType(StringType, new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType)))
      .add("x", MapType(StringType, new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))))

  // Result schema for UPDATE t.col.y = s.col.y: only adds col.y{}.c (no col.x).
  private val nestedMapColYEvolutionResultSchema = new StructType()
    .add("key", IntegerType)
    .add("col", new StructType()
      .add("y", MapType(StringType, new StructType()
        .add("d", IntegerType)
        .add("e", IntegerType)
        .add("c", IntegerType)))
      .add("z", MapType(StringType, new StructType()
        .add("f", IntegerType)
        .add("g", IntegerType))))

  private def generateNestedMapSourceRow(sourceType: SourceType.Value): String = {
    sourceType match {
      case SourceType.NonNullLeaves =>
        """{"key":1,"col":{"x":{"k1":{"a":10,"b":20}},"y":{"k1":{"c":30,"d":40}}}}"""
      case SourceType.NullLeaves =>
        """{"key":1,"col":{"x":{"k1":{"a":null,"b":null}},"y":{"k1":{"c":null,"d":null}}}}"""
      case SourceType.NullNestedStruct =>
        """{"key":1,"col":{"x":{"k1":null},"y":{"k1":null}}}"""
      case SourceType.NullNestedMap =>
        """{"key":1,"col":{"x":null,"y":null}}"""
      case SourceType.NullCol =>
        """{"key":1,"col":null}"""
    }
  }

  private def generateNestedMapTargetRow(
      targetType: TargetType.Value): Option[String] = {
    targetType match {
      case TargetType.NonNullLeaves =>
        Some("""{"key":1,"col":{"y":{"k2":{"d":4,"e":5}},"z":{"k2":{"f":6,"g":7}}}}""")
      case TargetType.NullLeaves =>
        Some("""{"key":1,"col":{"y":{"k2":{"d":null,"e":null}},"z":{"k2":{"f":null,"g":null}}}}""")
      case TargetType.NullTargetOnlyFields =>
        Some("""{"key":1,"col":{"y":{"k2":{"d":4,"e":null}},"z":null}}""")
      case TargetType.NullCol =>
        Some("""{"key":1,"col":null}""")
      case TargetType.Empty =>
        None
    }
  }

  /**
   * Generates the expected result based on `sourceRowJson`, `targetRowJsonOpt`, and `actionType`.
   * Semantics: col struct evolves, nested maps are overwritten, and structs within the
   * map evolve.
   */
  private def generateNestedMapExpectedResult(
      sourceRowJson: String,
      targetRowJsonOpt: Option[String],
      actionType: ActionType.Value): String = {
    val sourceRow = fromJsonToMap(sourceRowJson)
    val targetRowOpt = targetRowJsonOpt.map(fromJsonToMap)

    val sourceCol = castToMap(sourceRow("col"))
    val targetColOpt = targetRowOpt.map(row => castToMap(row("col")))

    if (shouldOverwriteWithNull(sourceCol, targetColOpt, actionType, targetOnlyFieldKey = "z")) {
      return """{"key":1,"col":null}"""
    }

    // col.x is source-only.
    val sourceX = getNestedValue(sourceCol, "x")
    val resultXOpt: Option[Any] = actionType match {
      case ActionType.UpdateStar | ActionType.UpdateCol |
           ActionType.InsertStar | ActionType.InsertCol =>
        // col.x is kept as is.
        Some(sourceX)

      case ActionType.UpdateColY =>
        // col.x is not added in the result schema.
        None
    }

    val sourceY = getNestedValue(sourceCol, "y")
    // col.y exists in both the source and target.
    // Semantics: replace entire map and evolve struct within the map.
    val resultY: Any = if (sourceY == null) {
      null
    } else {
      val sourceYMap = sourceY.asInstanceOf[Map[String, Any]]
      sourceYMap.map { case (key, value) =>
        val resultValue = if (value == null) {
          if (preserveNullSourceStructs) {
            null
          } else {
            Map("d" -> null, "e" -> null, "c" -> null)
          }
        } else {
          val sourceYStruct = value.asInstanceOf[Map[String, Any]]
          Map(
            "d" -> sourceYStruct("d"),
            // Target-only field `e` in map value structs is added as null.
            "e" -> null,
            "c" -> sourceYStruct("c")
          )
        }
        key -> resultValue
      }
    }

    // col.z is target-only.
    val resultZ: Any = actionType match {
      case ActionType.UpdateStar | ActionType.UpdateColY =>
        val targetCol = targetColOpt.get
        val targetZ = getNestedValue(targetCol, "z")
        // UPDATE * preserves target-only field (col.z).
        // UPDATE col.y = s.col.y preserves fields not in assignment (col.z).
        targetZ

      case ActionType.UpdateCol | ActionType.InsertStar | ActionType.InsertCol =>
        // Whole-struct assignment nulls target-only field (col.z).
        null
    }

    val colMap = resultXOpt match {
      case Some(resultX) =>
        Map("y" -> resultY, "z" -> resultZ, "x" -> resultX)
      case None =>
        Map("y" -> resultY, "z" -> resultZ)
    }

    val resultMap = Map("key" -> 1, "col" -> colMap)
    toJsonWithNulls(resultMap)
  }

  private def getNestedMapResultSchema(actionType: ActionType.Value): StructType = {
    actionType match {
      case ActionType.UpdateStar | ActionType.UpdateCol |
           ActionType.InsertStar | ActionType.InsertCol =>
        nestedMapColEvolutionResultSchema
      case ActionType.UpdateColY =>
        nestedMapColYEvolutionResultSchema
    }
  }

  /**
   * Generates test cases for combinations of source type, target type, and action type.
   */
  private def generateNestedMapStructNullnessTests(): Seq[StructEvolutionNullnessTestCase] = {
    generateStructEvolutionNullnessTests(
      testNamePrefix = testNamePrefix,
      sourceSchema = nestedMapStructSourceSchema,
      targetSchema = nestedMapStructTargetSchema,
      sourceTypes = Seq(
        SourceType.NonNullLeaves,
        SourceType.NullLeaves,
        SourceType.NullNestedStruct,
        SourceType.NullNestedMap,
        SourceType.NullCol),
      updateTargetTypes = Seq(
        TargetType.NonNullLeaves,
        TargetType.NullLeaves,
        TargetType.NullTargetOnlyFields,
        TargetType.NullCol),
      actionTypes = Seq(
        ActionType.UpdateStar,
        ActionType.UpdateCol,
        ActionType.UpdateColY,
        ActionType.InsertStar,
        ActionType.InsertCol),
      generateResultSchemaFn = getNestedMapResultSchema,
      generateSourceRowFn = generateNestedMapSourceRow,
      generateTargetRowFn = generateNestedMapTargetRow,
      generateExpectedResultFn = generateNestedMapExpectedResult
    )
  }

  generateNestedMapStructNullnessTests().foreach { testCase =>
    testNestedStructsEvolution(testCase.testName)(
      target = testCase.targetData,
      source = Seq(testCase.sourceData),
      targetSchema = testCase.targetSchema,
      sourceSchema = testCase.sourceSchema,
      clauses = testCase.actionClause :: Nil,
      result = Seq(testCase.expectedResult),
      resultSchema = testCase.resultSchema,
      expectErrorWithoutEvolutionContains = "Cannot cast",
      confs = testCase.confs)
  }

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with null target-only field")(
    target = Seq("""{"key":1,"col":{"y":{"k1":{"d":2,"e":2}},"z":null}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedMapStructTargetSchema,
    sourceSchema = nestedMapStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq(
      if (preserveNullSourceStructs && preserveNullSourceStructsUpdateStar) {
        """{"key":1,"col":null}"""
      } else {
        """{"key":1,"col":{"y": null,"z": null, "x": null}}"""
      }
    ),
    resultSchema = nestedMapColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(
      s"${testNamePrefix}null expansion - UPDATE * with non-null target-only field")(
    target = Seq("""{"key":1,"col":{"y":{"k1":{"d":2,"e":2}},"z":{"k1":{"f":2,"g":2}}}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedMapStructTargetSchema,
    sourceSchema = nestedMapStructSourceSchema,
    clauses = update("*") :: Nil,
    result = Seq("""{"key":1,"col":{"y":null,"z":{"k1":{"f":2,"g":2}},"x":null}}"""),
    resultSchema = nestedMapColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  private val expectedResult = if (preserveNullSourceStructs) {
    """{"key":1,"col":null}"""
  } else {
    """{"key":1,"col":{"y": null,"z": null, "x": null}}"""
  }

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - UPDATE t.col = s.col")(
    target = Seq("""{"key":1,"col":{"y":{"k1":{"d":2,"e":2}},"z":{"k1":{"f":2,"g":2}}}}"""),
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedMapStructTargetSchema,
    sourceSchema = nestedMapStructSourceSchema,
    clauses = update("t.col = s.col") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedMapColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT *")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedMapStructTargetSchema,
    sourceSchema = nestedMapStructSourceSchema,
    clauses = insert("*") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedMapColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)

  testNestedStructsEvolution(s"${testNamePrefix}null expansion - INSERT (key, col)")(
    target = Seq.empty,
    source = Seq("""{"key":1,"col":null}"""),
    targetSchema = nestedMapStructTargetSchema,
    sourceSchema = nestedMapStructSourceSchema,
    clauses = insert("(key, col) VALUES (s.key, s.col)") :: Nil,
    result = Seq(expectedResult),
    resultSchema = nestedMapColEvolutionResultSchema,
    expectErrorWithoutEvolutionContains = "Cannot cast",
    confs = preserveNullStructsConfs)
}

trait StructEvolutionPreserveNullSourceEnabled extends MergeIntoStructEvolutionNullnessTestUtils {
  override protected def preserveNullSourceStructs: Boolean = true
}

trait StructEvolutionPreserveNullSourceDisabled extends MergeIntoStructEvolutionNullnessTestUtils {
  override protected def preserveNullSourceStructs: Boolean = false
}

trait StructEvolutionPreserveNullSourceUpdateStarEnabled
    extends MergeIntoStructEvolutionNullnessTestUtils {
  override protected def preserveNullSourceStructsUpdateStar: Boolean = true
}

trait StructEvolutionPreserveNullSourceUpdateStarDisabled
    extends MergeIntoStructEvolutionNullnessTestUtils {
  override protected def preserveNullSourceStructsUpdateStar: Boolean = false
}
