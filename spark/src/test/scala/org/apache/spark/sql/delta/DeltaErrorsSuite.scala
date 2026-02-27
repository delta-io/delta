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

import java.io.{FileNotFoundException, PrintWriter, StringWriter}
import java.net.URI
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import scala.sys.process.Process

// scalastyle:off import.ordering.noEmptyLine
// scalastyle:off line.size.limit
import org.apache.spark.sql.delta.DeltaErrors.generateDocsLink
import org.apache.spark.sql.delta.actions.{Action, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.constraints.CharVarcharConstraint
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.hooks.{AutoCompactType, PostCommitHook}
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, InvariantViolationException, SchemaMergingUtils, SchemaUtils, UnsupportedDataTypeInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path
import org.json4s.JString
import org.scalatest.GivenWhenThen

import org.apache.spark.{SparkContext, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ExprId, Length, LessThanOrEqual, Literal, SparkVersion}
import org.apache.spark.sql.catalyst.expressions.Uuid
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

trait DeltaErrorsSuiteBase
    extends QueryTest
    with SharedSparkSession
    with GivenWhenThen
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with QueryErrorsBase {

  val MAX_URL_ACCESS_RETRIES = 3
  val path = "/sample/path"

  // Map of error function to the error
  // When adding a function...
  // (a) if the function is just a message: add the name of the message/function as the key, and an
  // error that uses that message as the value
  // (b) if the function is an error function: add the name of the function as the key, and the
  // value as the error being thrown
  def errorsToTest: Map[String, Throwable] = Map(
    "createExternalTableWithoutLogException" ->
      DeltaErrors.createExternalTableWithoutLogException(new Path(path), "tableName", spark),
    "createExternalTableWithoutSchemaException" ->
      DeltaErrors.createExternalTableWithoutSchemaException(new Path(path), "tableName", spark),
    "createManagedTableWithoutSchemaException" ->
      DeltaErrors.createManagedTableWithoutSchemaException("tableName", spark),
    "multipleSourceRowMatchingTargetRowInMergeException" ->
      DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark),
    "concurrentModificationExceptionMsg" -> new ConcurrentWriteException(None),
    "incorrectLogStoreImplementationException" ->
      DeltaErrors.incorrectLogStoreImplementationException(sparkConf, new Throwable()),
    "sourceNotDeterministicInMergeException" ->
      DeltaErrors.sourceNotDeterministicInMergeException(spark),
    "columnMappingAdviceMessage" ->
      DeltaErrors.columnRenameNotSupported,
    "icebergClassMissing" -> DeltaErrors.icebergClassMissing(sparkConf, new Throwable()),
    "tableFeatureReadRequiresWriteException" ->
      DeltaErrors.tableFeatureReadRequiresWriteException(requiredWriterVersion = 7),
    "tableFeatureRequiresHigherReaderProtocolVersion" ->
      DeltaErrors.tableFeatureRequiresHigherReaderProtocolVersion(
        feature = "feature",
        currentVersion = 1,
        requiredVersion = 7),
    "tableFeatureRequiresHigherWriterProtocolVersion" ->
      DeltaErrors.tableFeatureRequiresHigherReaderProtocolVersion(
        feature = "feature",
        currentVersion = 1,
        requiredVersion = 7),
    "blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges" ->
      DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
        spark,
        StructType.fromDDL("id int"),
        StructType.fromDDL("id2 int"),
        detectedDuringStreaming = true),
    "concurrentAppendException" ->
      DeltaErrors.concurrentAppendException(None, "t", -1, partitionOpt = None),
    "concurrentDeleteDeleteException" ->
      DeltaErrors.concurrentDeleteDeleteException(None, "t", -1, partitionOpt = None),
    "concurrentDeleteReadException" ->
      DeltaErrors.concurrentDeleteReadException(None, "t", -1, partitionOpt = None),
    "concurrentWriteException" ->
      DeltaErrors.concurrentWriteException(None),
    "concurrentTransactionException" ->
      DeltaErrors.concurrentTransactionException(None),
    "metadataChangedException" ->
      DeltaErrors.metadataChangedException(None),
    "protocolChangedException" ->
      DeltaErrors.protocolChangedException(None)
  )

  def otherMessagesToTest: Map[String, String] = Map(
    "ignoreStreamingUpdatesAndDeletesWarning" ->
      DeltaErrors.ignoreStreamingUpdatesAndDeletesWarning(spark)
  )

  def errorMessagesToTest: Map[String, String] =
    errorsToTest.mapValues(_.getMessage).toMap ++ otherMessagesToTest

  def checkIfValidResponse(url: String, response: String): Boolean = {
    response.contains("HTTP/1.1 200 OK") || response.contains("HTTP/2 200")
  }

  def getUrlsFromMessage(message: String): List[String] = {
    val regexToFindUrl = "https://[^\\s]+".r
    regexToFindUrl.findAllIn(message).toList
  }

  def testUrl(errName: String, url: String): Unit = {
    Given(s"*** Checking response for url: $url")
    val lastResponse = (1 to MAX_URL_ACCESS_RETRIES).map { attempt =>
      if (attempt > 1) Thread.sleep(1000)
      val response = try {
        Process("curl -I -L " + url).!!
      } catch {
        case e: RuntimeException =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          sw.toString
      }
      if (checkIfValidResponse(url, response)) {
        // The URL is correct. No need to retry.
        return
      }
      response
    }.last

    // None of the attempts resulted in a valid response. Fail the test.
    fail(
      s"""
         |A link to the URL: '$url' is broken in the error: $errName, accessing this URL
         |does not result in a valid response, received the following response: $lastResponse
       """.stripMargin)
  }

  def testUrls(): Unit = {
    errorMessagesToTest.foreach { case (errName, message) =>
      getUrlsFromMessage(message).foreach { url =>
        testUrl(errName, url)
      }
    }
  }

  def generateDocsLink(relativePath: String): String = DeltaErrors.generateDocsLink(
      spark.sparkContext.getConf, relativePath, skipValidation = true)

  test("Validate that links to docs in DeltaErrors are correct") {
    // verify DeltaErrors.errorsWithDocsLinks is consistent with DeltaErrorsSuite
    assert(errorsToTest.keySet ++ otherMessagesToTest.keySet ==
      DeltaErrors.errorsWithDocsLinks.toSet
    )
    testUrls()
  }

  protected def multipleSourceRowMatchingTargetRowInMergeUrl: String =
    "/delta-update.html#upsert-into-a-table-using-merge"

  test("test DeltaErrors methods -- part 1") {
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.tableAlreadyContainsCDCColumns(Seq("col1", "col2"))
      }
      checkError(e, "DELTA_TABLE_ALREADY_CONTAINS_CDC_COLUMNS", "42711",
        Map("columnList" -> "[col1,col2]"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cdcColumnsInData(Seq("col1", "col2"))
      }
      checkError(e, "RESERVED_CDC_COLUMNS_ON_WRITE", "42939",
        Map("columnList" -> "[col1,col2]", "config" -> "delta.enableChangeDataFeed"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.multipleCDCBoundaryException("starting")
      }
      checkError(e, "DELTA_MULTIPLE_CDC_BOUNDARY", "42614",
        Map("startingOrEnding" -> "starting"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnCheckpointRename(new Path("path-1"), new Path("path-2"))
      }
      checkError(e, "DELTA_CANNOT_RENAME_PATH", "22KD1",
        Map("currentPath" -> "path-1", "newPath" -> "path-2"))
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaErrors.notNullColumnMissingException(NotNull(Seq("c0", "c1")))
      }
      checkError(e, "DELTA_MISSING_NOT_NULL_COLUMN_VALUE", "23502",
        Map("columnName" -> "c0.c1"))
    }
    {
      val parent = "parent"
      val nested = IntegerType
      val nestType = "nestType"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedNotNullConstraint(parent, nested, nestType)
      }
      checkError(e, "DELTA_NESTED_NOT_NULL_CONSTRAINT", "0AKDC", Map(
        "parent" -> parent,
        "nestedPrettyJson" -> nested.prettyJson,
        "nestType" -> nestType,
        "configKey" -> DeltaSQLConf.ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS.key))
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(Constraints.NotNull(Seq("col1")))
      }
      checkError(e, "DELTA_NOT_NULL_CONSTRAINT_VIOLATED", "23502",
        Map("columnName" -> "col1"))
    }
    {
      val expr = UnresolvedAttribute("col")
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(
          Constraints.Check(CharVarcharConstraint.INVARIANT_NAME,
            LessThanOrEqual(Length(expr), Literal(5))),
          Map("col" -> "Hello World"))
      }
      checkError(e, "DELTA_EXCEED_CHAR_VARCHAR_LIMIT", "22001",
        Map("value" -> "Hello World", "expr" -> "(length(col) <= 5)"))
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(
          Constraints.Check("__dummy__",
            CatalystSqlParser.parseExpression("id < 0")),
          Map("a" -> "b"))
      }
      checkError(e, "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES", "23001", Map(
        "constraintName" -> "__dummy__",
        "expression" -> "(id < 0)",
        "values" -> " - a : b"))
    }
    {
      val tableIdentifier = DeltaTableIdentifier(Some("tableName"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException(tableIdentifier)
      }
      checkError(e, "DELTA_MISSING_DELTA_TABLE", "42P01",
        Map("tableName" -> tableIdentifier.toString))
    }
    {
      val tableIdentifier = DeltaTableIdentifier(Some("tableName"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException(
          operation = "delete", tableIdentifier)
      }
      checkError(e, "DELTA_TABLE_ONLY_OPERATION", "0AKDD",
        Map("tableName" -> tableIdentifier.toString, "operation" -> "delete"))
    }
    {
      val table = TableIdentifier("table")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotWriteIntoView(table)
      }
      checkError(e, "DELTA_CANNOT_WRITE_INTO_VIEW", "0A000",
        Map("table" -> table.toString))
    }
    {
      val sourceType = IntegerType
      val targetType = DateType
      val columnName = "column_name"
      val e = intercept[DeltaArithmeticException] {
        throw DeltaErrors.castingCauseOverflowErrorInTableWrite(sourceType, targetType, columnName)
      }
      checkError(e, "DELTA_CAST_OVERFLOW_IN_TABLE_WRITE", "22003", Map(
        "sourceType" -> toSQLType(sourceType),
        "targetType" -> toSQLType(targetType),
        "columnName" -> toSQLId(columnName),
        "storeAssignmentPolicyFlag" -> SQLConf.STORE_ASSIGNMENT_POLICY.key,
        "updateAndMergeCastingFollowsAnsiEnabledFlag" ->
          DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key,
        "ansiEnabledFlag" -> SQLConf.ANSI_ENABLED.key))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidColumnName(name = "col-1")
      }
      checkError(e, "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAME", "42K05",
        Map("columnName" -> "col-1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSetColumnNotFoundException(col = "c0", colList = Seq("c1", "c2"))
      }
      checkError(e, "DELTA_MISSING_SET_COLUMN", "42703",
        Map("columnName" -> "`c0`", "columnList" -> "[`c1`, `c2`]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSetConflictException(cols = Seq("c1", "c2"))
      }
      checkError(e, "DELTA_CONFLICT_SET_COLUMN", "42701",
        Map("columnList" -> "[`c1`, `c2`]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnNestedColumnNotSupportedException("c0")
      }
      checkError(e, "DELTA_UNSUPPORTED_NESTED_COLUMN_IN_BLOOM_FILTER", "0AKDC",
        Map("columnName" -> "c0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnPartitionColumnNotSupportedException("c0")
      }
      checkError(e, "DELTA_UNSUPPORTED_PARTITION_COLUMN_IN_BLOOM_FILTER", "0AKDC",
        Map("columnName" -> "c0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterDropOnNonIndexedColumnException("c0")
      }
      checkError(e, "DELTA_CANNOT_DROP_BLOOM_FILTER_ON_NON_INDEXED_COLUMN", "42703",
        Map("columnName" -> "c0"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotRenamePath("a", "b")
      }
      checkError(e, "DELTA_CANNOT_RENAME_PATH", "22KD1",
        Map("currentPath" -> "a", "newPath" -> "b"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cannotSpecifyBothFileListAndPatternString()
      }
      checkError(e, "DELTA_FILE_LIST_AND_PATTERN_STRING_CONFLICT", "42613",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateArrayField("t", "f")
      }
      checkError(e, "DELTA_CANNOT_UPDATE_ARRAY_FIELD", "429BQ",
        Map("tableName" -> "t", "fieldName" -> "f"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateMapField("t", "f")
      }
      checkError(e, "DELTA_CANNOT_UPDATE_MAP_FIELD", "429BQ",
        Map("tableName" -> "t", "fieldName" -> "f"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateStructField("t", "f")
      }
      checkError(e, "DELTA_CANNOT_UPDATE_STRUCT_FIELD", "429BQ",
        Map("tableName" -> "t", "fieldName" -> "f"))
    }
    {
      val tableName = "table"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateOtherField(tableName, IntegerType)
      }
      checkError(e, "DELTA_CANNOT_UPDATE_OTHER_FIELD", "429BQ",
        Map("tableName" -> tableName, "typeName" -> IntegerType.toString))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.duplicateColumnsOnUpdateTable(originalException = new Exception("123"))
      }
      checkError(e, "DELTA_DUPLICATE_COLUMNS_ON_UPDATE_TABLE", "42701",
        Map("message" -> "123"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.maxCommitRetriesExceededException(0, 1, 2, 3, 4)
      }
      checkError(e, "DELTA_MAX_COMMIT_RETRIES_EXCEEDED", "40000",
        Map("failVersion" -> "1", "startVersion" -> "2", "timeSpent" -> "4",
          "numActions" -> "3", "numAttempts" -> "0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingColumnsInInsertInto("c")
      }
      checkError(e, "DELTA_INSERT_COLUMN_MISMATCH", "42802",
        Map("columnName" -> "c"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidAutoCompactType("invalid")
      }
      val allowed = AutoCompactType.ALLOWED_VALUES.mkString("(", ",", ")")
      checkError(e, "DELTA_INVALID_AUTO_COMPACT_TYPE", "22023",
        Map("value" -> "invalid", "allowed" -> allowed))
    }
    {
      val table = DeltaTableIdentifier(Some("path"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentDeltaTable(table)
      }
      checkError(e, "DELTA_TABLE_NOT_FOUND", "42P01",
        Map("tableName" -> table.toString))
    }
    {
      val newTableId = "027fb01c-94aa-4cab-87cb-5aab6aec6d17"
      val oldTableId = "2edf2c02-bb63-44e9-a84c-517fad0db296"
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.differentDeltaTableReadByStreamingSource(
          newTableId = newTableId,
          oldTableId = oldTableId)
      }
      checkError(e, "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE", "55019", Map(
          "oldTableId" -> oldTableId, "newTableId" -> newTableId))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentColumnInSchema("c", "s")
      }
      checkError(e, "DELTA_COLUMN_NOT_FOUND_IN_SCHEMA", "42703", Map(
        "columnName" -> "c", "tableSchema" -> "s"))
    }
    {
      val ident = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaNoSuchTableException] {
        throw DeltaErrors.noRelationTable(ident)
      }
      checkError(e, "DELTA_NO_RELATION_TABLE", "42P01", Map("tableIdent" -> ident.quoted))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTable("t")
      }
      checkError(e, "DELTA_NOT_A_DELTA_TABLE", "0AKDD", Map("tableName" -> "t"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.notFoundFileToBeRewritten("f", Seq("a", "b"))
      }
      checkError(e, "DELTA_FILE_TO_OVERWRITE_NOT_FOUND", "42K03", Map("path" -> "f", "pathList" -> "a\nb"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsetNonExistentProperty("k", "t")
      }
      checkError(e, "DELTA_UNSET_NON_EXISTENT_PROPERTY", "42616",
        Map("property" -> "k", "tableName" -> "t"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsReferToWrongColumns(
          new AnalysisException(
            errorClass = "INTERNAL_ERROR",
            messageParameters = Map("message" -> "internal test error msg"))
        )
      }
      checkError(e, "DELTA_INVALID_GENERATED_COLUMN_REFERENCES", "42621", Map.empty[String, String])
      checkError(e.getCause.asInstanceOf[AnalysisException], "INTERNAL_ERROR", None,
        Map("message" -> "internal test error msg"))
    }
    {
      val current = StructField("c0", IntegerType)
      val update = StructField("c0", StringType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUpdateColumnType(current, update)
      }
      checkError(e, "DELTA_GENERATED_COLUMN_UPDATE_TYPE_MISMATCH", "42K09", Map(
        "currentName" -> current.name,
        "currentDataType" -> current.dataType.sql,
        "updateDataType" -> update.dataType.sql))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.changeColumnMappingModeNotSupported(oldMode = "old", newMode = "new")
      }
      checkError(e, "DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE", "0AKDC",
        Map("oldMode" -> "old", "newMode" -> "new"))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.generateManifestWithColumnMappingNotSupported
      }
      checkError(e, "DELTA_UNSUPPORTED_MANIFEST_GENERATION_WITH_COLUMN_MAPPING", "0AKDC",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertToDeltaNoPartitionFound("testTable")
      }
      checkError(e, "DELTA_CONVERSION_NO_PARTITION_FOUND", "42KD6",
        Map("tableName" -> "testTable"))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.convertToDeltaWithColumnMappingNotSupported(IdMapping)
      }
      checkError(e, "DELTA_CONVERSION_UNSUPPORTED_COLUMN_MAPPING", "0AKDC", Map(
        "config" -> DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey, "mode" -> "id"))
    }
    {
      val oldSchema = StructType(Seq(StructField("c0", IntegerType)))
      val newSchema = StructType(Seq(StructField("c1", IntegerType)))
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.schemaChangeDuringMappingModeChangeNotSupported(
          oldSchema = oldSchema, newSchema = newSchema)
      }
      checkError(e, "DELTA_UNSUPPORTED_COLUMN_MAPPING_SCHEMA_CHANGE", "0AKDC",
        Map("oldTableSchema" -> oldSchema.treeString, "newTableSchema" -> newSchema.treeString))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notEnoughColumnsInInsert(
          "table", 1, 2, Some("nestedField"))
      }
      checkError(e, "DELTA_INSERT_COLUMN_ARITY_MISMATCH", "42802",
        Map("tableName" -> "table", "columnName" -> "not enough nested fields in nestedField",
          "numColumns" -> "2", "insertColumns" -> "1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotInsertIntoColumn(
          "tableName", "source", "target", "targetType")
      }
      checkError(e, "DELTA_COLUMN_STRUCT_TYPE_MISMATCH", "2200G",
        Map("source" -> "source", "targetType" -> "targetType", "targetField" -> "target",
          "targetTable" -> "tableName"))
    }
    {
      val colName = "col1"
      val schema = Seq(UnresolvedAttribute("col2"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.partitionColumnNotFoundException(colName, schema)
      }
      checkError(e, "DELTA_PARTITION_COLUMN_NOT_FOUND", "42703",
        Map("columnName" -> DeltaErrors.formatColumn(colName),
          "schemaMap" -> schema.map(_.name).mkString(", ")))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.partitionPathParseException("fragment")
      }
      checkError(e, "DELTA_INVALID_PARTITION_PATH", "22KD1", Map("path" -> "fragment"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereMismatchException("replaceWhereArgValue",
          new InvariantViolationException("Invariant violated."))
      }
      checkError(e, "DELTA_REPLACE_WHERE_MISMATCH", "44000",
        Map("replaceWhere" -> "replaceWhereArgValue", "message" -> "Invariant violated."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereMismatchException("replaceWhere", "badPartitions")
      }
      checkError(e, "DELTA_REPLACE_WHERE_MISMATCH", "44000",
        Map("replaceWhere" -> "replaceWhere",
          "message" -> "Invalid data would be written to partitions badPartitions."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.actionNotFoundException("action", 0)
      }
      checkError(e, "DELTA_STATE_RECOVER_ERROR", "XXKDS",
        Map("operation" -> "action", "version" -> "0"))
    }
    {
      val oldSchema = StructType(Seq(StructField("c0", IntegerType)))
      val newSchema = StructType(Seq(StructField("c0", StringType)))
      for (retryable <- DeltaTestUtils.BOOLEAN_DOMAIN) {
        val expectedClass: Class[_] = classOf[DeltaIllegalStateException]

        var e = intercept[Exception with SparkThrowable] {
          throw DeltaErrors.schemaChangedException(oldSchema, newSchema, retryable, None, false)
        }
        assert(expectedClass.isAssignableFrom(e.getClass))
        checkError(e, "DELTA_SCHEMA_CHANGED", "KD007", Map(
          "readSchema" -> DeltaErrors.formatSchema(oldSchema),
          "dataSchema" -> DeltaErrors.formatSchema(newSchema)
        ))

        // Check the error message with version information
        e = intercept[Exception with SparkThrowable] {
          throw DeltaErrors.schemaChangedException(oldSchema, newSchema, retryable, Some(10), false)
        }
        assert(expectedClass.isAssignableFrom(e.getClass))
        checkError(e, "DELTA_SCHEMA_CHANGED_WITH_VERSION", "KD007", Map(
          "version" -> "10",
          "readSchema" -> DeltaErrors.formatSchema(oldSchema),
          "dataSchema" -> DeltaErrors.formatSchema(newSchema)
        ))

        // Check the error message with startingVersion/Timestamp error message
        e = intercept[Exception with SparkThrowable] {
          throw DeltaErrors.schemaChangedException(oldSchema, newSchema, retryable, Some(10), true)
        }
        assert(expectedClass.isAssignableFrom(e.getClass))
        checkError(e, "DELTA_SCHEMA_CHANGED_WITH_STARTING_OPTIONS", "KD007", Map(
          "version" -> "10",
          "readSchema" -> DeltaErrors.formatSchema(oldSchema),
          "dataSchema" -> DeltaErrors.formatSchema(newSchema)
        ))
      }
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreVersionNotExistException(1, 2, 3)
      }
      checkError(e, "DELTA_CANNOT_RESTORE_TABLE_VERSION", "22003",
        Map("version" -> "1", "startVersion" -> "2", "endVersion" -> "3"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedColumnMappingModeException("modeName")
      }
      checkError(e, "DELTA_MODE_NOT_SUPPORTED", "0AKDC", Map(
        "mode" -> "modeName",
        "supportedModes" -> DeltaColumnMapping.supportedModes.map(_.name).toSeq.mkString(", ")
      ))
    }
    {
      import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
      val supportedModes = DeltaGenerateCommand.modeNameToGenerationFunc.keys.toSeq.mkString(", ")
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedGenerateModeException("modeName")
      }
      checkError(e, "DELTA_MODE_NOT_SUPPORTED", "0AKDC", Map(
        "mode" -> "modeName", "supportedModes" -> supportedModes))
    }
    {
      import org.apache.spark.sql.delta.DeltaOptions.EXCLUDE_REGEX_OPTION
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.excludeRegexOptionException(EXCLUDE_REGEX_OPTION)
      }
      checkError(e, "DELTA_REGEX_OPT_SYNTAX_ERROR", "2201B", Map(
        "regExpOption" -> EXCLUDE_REGEX_OPTION
      ))
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileNotFoundException("somePath")
      }
      checkError(e, "DELTA_FILE_NOT_FOUND", "42K03", Map(
        "path" -> "somePath"
      ))
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.logFileNotFoundException(new Path("file://table"), None, 10)
      }
      checkError(e, "DELTA_LOG_FILE_NOT_FOUND", "42K03", Map(
        "version" -> "LATEST",
        "checkpointVersion" -> "10",
        "logPath" -> "file://table"
      ))
    }
    {
      val ex = new FileNotFoundException("reason")
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(ex)
      }
      checkError(e, "DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE", "42K03",
        parameters = Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidIsolationLevelException("level")
      }
      checkError(e, "DELTA_INVALID_ISOLATION_LEVEL", "25000", Map(
        "isolationLevel" -> "level"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnNameNotFoundException("a", "b")
      }
      checkError(e, "DELTA_COLUMN_NOT_FOUND", "42703", Map(
        "columnName" -> "a",
        "columnList" -> "b"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnAtIndexLessThanZeroException("1", "a")
      }
      checkError(e, "DELTA_ADD_COLUMN_AT_INDEX_LESS_THAN_ZERO", "42KD3", Map(
        "columnIndex" -> "1",
        "columnName" -> "a"
      ))
    }
    {
      val pos = -1
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropColumnAtIndexLessThanZeroException(pos)
      }
      checkError(e, "DELTA_DROP_COLUMN_AT_INDEX_LESS_THAN_ZERO", "42KD8",
        Map("columnIndex" -> pos.toString))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.incorrectArrayAccess()
      }
      checkError(e, "DELTA_INCORRECT_ARRAY_ACCESS", "KD003", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.partitionColumnCastFailed("Value", "Type", "Name")
      }
      checkError(e, "DELTA_PARTITION_COLUMN_CAST_FAILED", "22525",
        Map("value" -> "Value", "dataType" -> "Type", "columnName" -> "Name"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidTimestampFormat("ts", "someFormat")
      }
      checkError(e, "DELTA_INVALID_TIMESTAMP_FORMAT", "22007",
        Map("timestamp" -> "ts", "format" -> "someFormat"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotChangeDataType("example message")
      }
      checkError(e, "DELTA_CANNOT_CHANGE_DATA_TYPE", "429BQ", Map("dataType" -> "example message"))
    }
    {
      val table = CatalogTable(TableIdentifier("my table"), null, null, null)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableAlreadyExists(table)
      }
      checkError(e, "DELTA_TABLE_ALREADY_EXISTS", "42P07", Map("tableName" -> "`my table`"))
    }
    {
      val storage1 = CatalogStorageFormat(Option(new URI("loc1")), null, null, null, false, Map.empty)
      val storage2 = CatalogStorageFormat(Option(new URI("loc2")), null, null, null, false, Map.empty)
      val table = CatalogTable(TableIdentifier("table"), null, storage1, null)
      val existingTable = CatalogTable(TableIdentifier("existing table"), null, storage2, null)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableLocationMismatch(table, existingTable)
      }
      checkError(e, "DELTA_TABLE_LOCATION_MISMATCH", "42613", Map(
        "tableName" -> "`table`",
        "tableLocation" -> "`loc1`",
        "existingTableLocation" -> "`loc2`"
      ))
    }
    {
      val ident = "ident"
      val e = intercept[DeltaNoSuchTableException] {
        throw DeltaErrors.nonSinglePartNamespaceForCatalog(ident)
      }
      checkError(e, "DELTA_NON_SINGLE_PART_NAMESPACE_FOR_CATALOG", "42K05",
        Map("identifier" -> ident))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.targetTableFinalSchemaEmptyException()
      }
      checkError(e, "DELTA_TARGET_TABLE_FINAL_SCHEMA_EMPTY", "428GU", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonDeterministicNotSupportedException("op", Uuid())
      }
      checkError(e, "DELTA_NON_DETERMINISTIC_FUNCTION_NOT_SUPPORTED", "0AKDC",
        Map("operation" -> "op", "expression" -> "(condition = uuid())."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableNotSupportedException("someOp")
      }
      checkError(e, "DELTA_TABLE_NOT_SUPPORTED_IN_OP", "42809", Map("operation" -> "someOp"))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.postCommitHookFailedException(new PostCommitHook() {
          override val name: String = "DummyPostCommitHook"
          override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {}
        }, 0, "msg", null)
      }
      checkError(e, "DELTA_POST_COMMIT_HOOK_FAILED", "2DKD0",
        Map("version" -> "0", "name" -> "DummyPostCommitHook", "message" -> ": msg"))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.postCommitHookFailedException(new PostCommitHook() {
          override val name: String = "DummyPostCommitHook"
          override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {}
        }, 0, null, null)
      }
      checkError(e, "DELTA_POST_COMMIT_HOOK_FAILED", "2DKD0", Map(
        "version" -> "0", "name" -> "DummyPostCommitHook", "message" -> ""))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerThanStruct(1, StructField("col1", IntegerType), 1)
      }
      checkError(e, "DELTA_INDEX_LARGER_THAN_STRUCT", "42KD8", Map(
        "index" -> "1",
        "columnName" -> "StructField(col1,IntegerType,true)",
        "length" -> "1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerOrEqualThanStruct(pos = 1, len = 2)
      }
      checkError(e, "DELTA_INDEX_LARGER_OR_EQUAL_THAN_STRUCT", "42KD8",
        Map("index" -> "1", "length" -> "2") )
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidV1TableCall("v1Table", "DeltaTableV2")
      }
      checkError(e, "DELTA_INVALID_V1_TABLE_CALL", "XXKDS",
        Map("callVersion" -> "v1Table", "tableVersion" -> "DeltaTableV2"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotGenerateUpdateExpressions()
      }
      checkError(e, "DELTA_CANNOT_GENERATE_UPDATE_EXPRESSIONS", "XXKDS",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val s1 = StructType(Seq(StructField("c0", IntegerType)))
        val s2 = StructType(Seq(StructField("c0", StringType)))
        SchemaMergingUtils.mergeSchemas(s1, s2)
      }
      checkError(
        e,
        "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map("currentField" -> "c0", "updateField" -> "c0"))
      checkError(
        e.getCause.asInstanceOf[DeltaAnalysisException],
        "DELTA_MERGE_INCOMPATIBLE_DATATYPE",
        parameters = Map("currentDataType" -> "IntegerType", "updateDataType" -> "StringType"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.describeViewHistory
      }
      checkError(e, "DELTA_CANNOT_DESCRIBE_VIEW_HISTORY", "42809",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedInvariant()
      }
      checkError(e, "DELTA_UNRECOGNIZED_INVARIANT", "56038", Map.empty[String, String])
    }
    {
      val baseSchema = StructType(Seq(StructField("c0", StringType)))
      val field = StructField("id", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotResolveColumn(field.name, baseSchema)
      }
      checkError(e, "DELTA_CANNOT_RESOLVE_COLUMN", "42703",
        Map("schema" -> baseSchema.treeString, "columnName" -> "id"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.alterTableChangeColumnException(
          fieldPath = "a.b.c",
          oldField = StructField("c", IntegerType),
          newField = StructField("c", LongType))
      }
      checkError(
        e,
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "a.b.c",
          "oldField" -> "INT",
          "newField" -> "BIGINT"
        )
      )
    }
    {
      val s1 = StructType(Seq(StructField("c0", IntegerType)))
      val s2 = StructType(Seq(StructField("c0", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.alterTableReplaceColumnsException(s1, s2, "incompatible")
      }
      checkError(e, "DELTA_UNSUPPORTED_ALTER_TABLE_REPLACE_COL_OP", "0AKDC", Map(
        "details" -> "incompatible",
        "oldSchema" -> s1.treeString,
        "newSchema" -> s2.treeString))
    }
    {
      checkError(
        exception = intercept[DeltaUnsupportedOperationException] {
          throw DeltaErrors.unsupportedTypeChangeInPreview(
            fieldPath = Seq("origin", "country"),
            fromType = IntegerType,
            toType = LongType,
            feature = TypeWideningPreviewTableFeature
          )
        },
        "DELTA_UNSUPPORTED_TYPE_CHANGE_IN_PREVIEW",
        parameters = Map(
          "fieldPath" -> "origin.country",
          "fromType" -> "INT",
          "toType" -> "BIGINT",
          "typeWideningFeatureName" -> "typeWidening-preview"
        ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unsupportedTypeChangeInSchema(Seq("s", "a"), IntegerType, StringType)
      }
      checkError(e, "DELTA_UNSUPPORTED_TYPE_CHANGE_IN_SCHEMA", "0AKDC",
        Map("fieldName" -> "s.a", "fromType" -> "INT", "toType" -> "STRING"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val classConf = Seq(("classKey", "classVal"))
        val schemeConf = Seq(("schemeKey", "schemeVal"))
        throw DeltaErrors.logStoreConfConflicts(classConf, schemeConf)
      }
      checkError(e, "DELTA_INVALID_LOGSTORE_CONF", "F0000",
        Map("classConfig" -> "classKey", "schemeConfig" -> "schemeKey"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        val schemeConf = Seq(("key", "val"))
        throw DeltaErrors.inconsistentLogStoreConfs(
          Seq(("delta.key", "value1"), ("spark.delta.key", "value2")))
      }
      checkError(e, "DELTA_INCONSISTENT_LOGSTORE_CONFS", "F0000",
        Map("setKeys" -> "delta.key = value1, spark.delta.key = value2"))
    }
    {
      val e = intercept[DeltaSparkException] {
        throw DeltaErrors.failedMergeSchemaFile(
          file = "someFile",
          schema = "someSchema",
          cause = null)
      }
      checkError(e, "DELTA_FAILED_MERGE_SCHEMA_FILE", "42KDA",
        Map("file" -> "someFile", "schema" -> "someSchema"))
    }
    {
      val id = TableIdentifier("id")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("op", id)
      }
      checkError(e, "DELTA_OPERATION_NOT_ALLOWED_DETAIL", "0AKDC",
        Map("operation" -> "op", "tableName" -> "`id`"))
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileOrDirectoryNotFoundException("somePath")
      }
      checkError(e, "DELTA_FILE_OR_DIR_NOT_FOUND", "42K03",
        Map("path" -> "somePath"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidPartitionColumn("col", "tbl")
      }
      checkError(e, "DELTA_INVALID_PARTITION_COLUMN", "42996",
        Map("columnName" -> "col", "tableName" -> "tbl"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotFindSourceVersionException("someJson")
      }
      checkError(e, "DELTA_CANNOT_FIND_VERSION", "XXKDS",
        Map("json" -> "someJson"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unknownConfigurationKeyException("confKey")
      }
      checkError(e, "DELTA_UNKNOWN_CONFIGURATION", "F0000", Map(
        "config" -> "confKey",
        "disableCheckConfig" -> DeltaSQLConf.ALLOW_ARBITRARY_TABLE_PROPERTIES.key
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathNotExistsException("somePath")
      }
      checkError(e, "DELTA_PATH_DOES_NOT_EXIST", "42K03",
        Map("path" -> "somePath"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failRelativizePath("somePath")
      }
      checkError(e, "DELTA_FAIL_RELATIVIZE_PATH", "XXKDS", Map(
        "path" -> "somePath",
        "config" -> DeltaSQLConf.DELTA_VACUUM_RELATIVIZE_IGNORE_ERROR.key
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.illegalFilesFound("someFile")
      }
      checkError(e, "DELTA_ILLEGAL_FILE_FOUND", "XXKDS", Map("file" -> "someFile"))
    }
    {
      val name = "name"
      val input = "input"
      val explain = "explain"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.illegalDeltaOptionException(name, input, explain)
      }
      checkError(e, "DELTA_ILLEGAL_OPTION", "42616", Map("name" -> name, "input" -> input, "explain" -> explain))
    }
    {
      val version = "version"
      val timestamp = "timestamp"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.startingVersionAndTimestampBothSetException(version, timestamp)
      }
      checkError(e, "DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET", "42613",
        Map("version" -> version, "timestamp" -> timestamp))
    }
    {
      val path = new Path("parent", "child")
      val specifiedSchema = StructType(Seq(StructField("a", IntegerType)))
      val existingSchema = StructType(Seq(StructField("b", StringType)))
      val diffs = Seq("a", "b")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentSchemaException(path, specifiedSchema, existingSchema, diffs)
      }
      checkError(e, "DELTA_CREATE_TABLE_SCHEME_MISMATCH", "42KD7", Map(
        "path" -> path.toString,
        "specifiedSchema" -> specifiedSchema.treeString,
        "existingSchema" -> existingSchema.treeString,
        "schemaDifferences" -> "- a\n- b"))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noHistoryFound(path)
      }
      checkError(e, "DELTA_NO_COMMITS_FOUND", "KD006", Map("logPath" -> path.toString))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noRecreatableHistoryFound(path)
      }
      checkError(e, "DELTA_NO_RECREATABLE_HISTORY_FOUND", "KD006", Map("logPath" -> path.toString))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.castPartitionValueException("partitionValue", StringType)
      }
      checkError(e, "DELTA_FAILED_CAST_PARTITION_VALUE", "22018",
        Map("value" -> "partitionValue", "dataType" -> "StringType"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkSessionNotSetException()
      }
      checkError(e, "DELTA_SPARK_SESSION_NOT_SET", "XXKDS", Map.empty[String, String])
    }
    {
      val id = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotReplaceMissingTableException(id)
      }
      checkError(e, "DELTA_CANNOT_REPLACE_MISSING_TABLE", "42P01", Map("tableName" -> "namespace.name"))
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.cannotCreateLogPathException("logPath")
      }
      checkError(e, "DELTA_CANNOT_CREATE_LOG_PATH", "42KD5", Map("path" -> "logPath"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.protocolPropNotIntException(
          key = "someKey",
          value = "someValue")
      }
      checkError(e, "DELTA_PROTOCOL_PROPERTY_NOT_INT", "42K06",
        Map("key" -> "someKey", "value" -> "someValue"))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createExternalTableWithoutLogException(
          path = path,
          tableName = "someTableName",
          spark = spark)
      }
      checkError(e, "DELTA_CREATE_EXTERNAL_TABLE_WITHOUT_TXN_LOG", "42K03", Map(
        "path" -> path.toString,
        "logPath" -> new Path(path, "_delta_log").toString,
        "tableName" -> "someTableName",
        "docLink" -> generateDocsLink("/index.html")
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.ambiguousPathsInCreateTableException("id", "loc")
      }
      checkError(e, "DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE", "42613", Map(
        "identifier" -> "id",
        "location" -> "loc",
        "config" -> DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS.key))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.illegalUsageException("overwriteSchema", "replacing")
      }
      checkError(e, "DELTA_ILLEGAL_USAGE", "42601", Map(
        "option" -> "overwriteSchema",
        "operation" -> "replacing"
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.expressionsNotFoundInGeneratedColumn("col1")
      }
      checkError(e, "DELTA_EXPRESSIONS_NOT_FOUND_IN_GENERATED_COLUMN", "XXKDS", Map(
        "columnName" -> "col1"
      ))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.activeSparkSessionNotFound()
      }
      checkError(e, "DELTA_ACTIVE_SPARK_SESSION_NOT_FOUND", "08003", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationOnTempViewWithGenerateColsNotSupported("UPDATE")
      }
      checkError(e, "DELTA_OPERATION_ON_TEMP_VIEW_WITH_GENERATED_COLS_NOT_SUPPORTED", "0A000", Map(
        "operation" -> "UPDATE"
      ))
    }
    {
      val property = "prop"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.cannotModifyTableProperty(property)
      }
      checkError(e, "DELTA_CANNOT_MODIFY_TABLE_PROPERTY", "42939",
        Map("prop" -> property))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingProviderForConvertException("parquet_path")
      }
      checkError(e, "DELTA_MISSING_PROVIDER_FOR_CONVERT", "0AKDC", Map(
        "path" -> "parquet_path"
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.iteratorAlreadyClosed()
      }
      checkError(e, "DELTA_ITERATOR_ALREADY_CLOSED", "XXKDS", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.activeTransactionAlreadySet()
      }
      checkError(e, "DELTA_ACTIVE_TRANSACTION_ALREADY_SET", "0B000", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterMultipleConfForSingleColumnException("col1")
      }
      checkError(e, "DELTA_MULTIPLE_CONF_FOR_SINGLE_COLUMN_IN_BLOOM_FILTER", "42614",
        Map("columnName" -> "col1"))
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null)
      }
      checkError(e, "DELTA_INCORRECT_LOG_STORE_IMPLEMENTATION", "0AKDC", Map(
        "docLink" -> generateDocsLink("/delta-storage.html")
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidSourceVersion("xyz")
      }
      checkError(e, "DELTA_INVALID_SOURCE_VERSION", "XXKDS",
        Map("version" -> "xyz"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidSourceOffsetFormat()
      }
      checkError(e, "DELTA_INVALID_SOURCE_OFFSET_FORMAT", "XXKDS", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidCommittedVersion(1L, 2L)
      }
      checkError(e, "DELTA_INVALID_COMMITTED_VERSION", "XXKDS", Map(
        "committedVersion" -> "1",
        "currentVersion" -> "2"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnReference("col1", Seq("col2", "col3"))
      }
      checkError(e, "DELTA_NON_PARTITION_COLUMN_REFERENCE", "42P10",
        Map("columnName" -> "col1", "columnList" -> "col2, col3"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val attr = UnresolvedAttribute("col1")
        val attrs = Seq(UnresolvedAttribute("col2"), UnresolvedAttribute("col3"))
        throw DeltaErrors.missingColumn(attr, attrs)
      }
      checkError(e, "DELTA_MISSING_COLUMN", "42703",
        Map("columnName" -> "col1", "columnList" -> "col2, col3"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val schema = StructType(Seq(StructField("c0", IntegerType)))
        throw DeltaErrors.missingPartitionColumn("c1", schema.catalogString)
      }
      checkError(e, "DELTA_MISSING_PARTITION_COLUMN", "42KD6",
        Map("columnName" -> "c1", "columnList" -> "struct<c0:int>"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.aggsNotSupportedException("op", SparkVersion())
      }
      checkError(e, "DELTA_AGGREGATION_NOT_SUPPORTED", "42903",
        Map("operation" -> "op", "predicate" -> "(condition = version())"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotChangeProvider()
      }
      checkError(e, "DELTA_CANNOT_CHANGE_PROVIDER", "42939", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.noNewAttributeId(AttributeReference("attr1", IntegerType)())
      }
      checkError(e, "DELTA_NO_NEW_ATTRIBUTE_ID", "XXKDS", Map("columnName" -> "attr1"))
    }
    {
      val e = intercept[ProtocolDowngradeException] {
        val p1 = Protocol(1, 1)
        val p2 = Protocol(2, 2)
        throw new ProtocolDowngradeException(p1, p2)
      }
      checkError(e, "DELTA_INVALID_PROTOCOL_DOWNGRADE", "KD004",
        Map("oldProtocol" -> "1,1", "newProtocol" -> "2,2"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsExprTypeMismatch("col1", IntegerType, StringType)
      }
      checkError(e, "DELTA_GENERATED_COLUMNS_EXPR_TYPE_MISMATCH", "42K09",
        Map("columnName" -> "col1", "expressionType" -> "STRING", "columnType" -> "INT"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.nonGeneratedColumnMissingUpdateExpression("attr1")
      }
      checkError(e, "DELTA_NON_GENERATED_COLUMN_MISSING_UPDATE_EXPR", "XXKDS", Map("columnName" -> "attr1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.constraintDataTypeMismatch(
          columnPath = Seq("a", "x"),
          columnType = ByteType,
          dataType = IntegerType,
          constraints = Map("ck1" -> "a > 0", "ck2" -> "hash(b) > 0"))
      }
      checkError(e, "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH", "42K09", Map(
        "columnName" -> "a.x",
        "columnType" -> "TINYINT",
        "dataType" -> "INT",
        "constraints" -> "ck1 -> a > 0\nck2 -> hash(b) > 0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsDataTypeMismatch(
          columnPath = Seq("a", "x"),
          columnType = ByteType,
          dataType = IntegerType,
          generatedColumns = Map(
            "gen1" -> "a . x + 1",
            "gen2" -> "3 + a . x"
          ))
      }
      checkError(e, "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH", "42K09", Map(
        "columnName" -> "a.x",
        "columnType" -> "TINYINT",
        "dataType" -> "INT",
        "generatedColumns" -> "gen1 -> a . x + 1\ngen2 -> 3 + a . x"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.useSetLocation()
      }
      checkError(e, "DELTA_CANNOT_CHANGE_LOCATION", "42601", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnAbsentException(false)
      }
      checkError(e, "DELTA_NON_PARTITION_COLUMN_ABSENT", "KD005", Map("details" -> ""))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnAbsentException(true)
      }
      checkError(e, "DELTA_NON_PARTITION_COLUMN_ABSENT", "KD005",
        Map("details" -> " Columns which are of NullType have been dropped."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.constraintAlreadyExists("name", "oldExpr")
      }
      checkError(e, "DELTA_CONSTRAINT_ALREADY_EXISTS", "42710",
        Map("constraintName" -> "name", "oldConstraint" -> "oldExpr"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.timeTravelNotSupportedException
      }
      checkError(e, "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS", "0AKDC", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.addFilePartitioningMismatchException(Seq("col3"), Seq("col2"))
      }
      checkError(e, "DELTA_INVALID_PARTITIONING_SCHEMA", "XXKDS", Map(
        "neededPartitioning" -> "[`col2`]",
        "specifiedPartitioning" -> "[`col3`]",
        "config" -> DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.emptyCalendarInterval
      }
      checkError(e, "DELTA_INVALID_CALENDAR_INTERVAL_EMPTY", "2200P", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createManagedTableWithoutSchemaException("table-1", spark)
      }
      checkError(e, "DELTA_INVALID_MANAGED_TABLE_SYNTAX_NO_SCHEMA", "42000", Map(
        "tableName" -> "table-1",
        "docLink" -> generateDocsLink("/index.html")
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUnsupportedExpression("someExp".expr)
      }
      checkError(e, "DELTA_UNSUPPORTED_EXPRESSION_GENERATED_COLUMN", "42621",
        Map("expression" -> "'someExp'"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedExpression("Merge", DataTypes.DateType, Seq("Integer", "Long"))
      }
      checkError(e, "DELTA_UNSUPPORTED_EXPRESSION", "0A000", Map(
        "expType" -> "DateType",
        "causedBy" -> "Merge",
        "supportedTypes" -> "Integer,Long"
      ))
    }
    {
      val expr = "someExp"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUDF(expr.expr)
      }
      checkError(e, "DELTA_UDF_IN_GENERATED_COLUMN", "42621", Map("udfExpr" -> "'someExp'"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnColumnTypeNotSupportedException("col1", DateType)
      }
      checkError(e, "DELTA_UNSUPPORTED_COLUMN_TYPE_IN_BLOOM_FILTER", "0AKDC", Map(
        "columnName" -> "col1",
        "dataType" -> "date"
      ))
    }
    {
      val e = intercept[DeltaTableFeatureException] {
        throw DeltaErrors.tableFeatureDropHistoryTruncationNotAllowed()
      }
      checkError(e, "DELTA_FEATURE_DROP_HISTORY_TRUNCATION_NOT_ALLOWED", "42000", Map.empty[String, String])
    }
    {
      val logRetention = DeltaConfigs.LOG_RETENTION
      val e = intercept[DeltaTableFeatureException] {
        throw DeltaErrors.dropTableFeatureWaitForRetentionPeriod(
          "test_feature",
          Metadata(configuration = Map(logRetention.key -> "30 days"))
        )
      }
      checkError(e, "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD", "22KD0", Map(
        "feature" -> "test_feature",
          "logRetentionPeriodKey" -> "delta.logRetentionDuration",
          "logRetentionPeriod" -> "30 days",
          "truncateHistoryLogRetentionPeriod" -> "24 hours"))
    }
  }

  test("test DeltaErrors methods -- part 2") {
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedDataTypes(
          UnsupportedDataTypeInfo("foo", CalendarIntervalType),
          UnsupportedDataTypeInfo("bar", TimestampNTZType))
      }
      checkError(e, "DELTA_UNSUPPORTED_DATA_TYPES", "0AKDC", Map(
        "dataTypeList" -> "[foo: CalendarIntervalType, bar: TimestampNTZType]",
        "config" -> DeltaSQLConf.DELTA_SCHEMA_TYPE_CHECK.key
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnDataLossException(12, 10)
      }
      checkError(e, "DELTA_MISSING_FILES_UNEXPECTED_VERSION", "XXKDS",
        Map("startVersion" -> "12", "earliestVersion" -> "10", "option" -> "failOnDataLoss"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedFieldNotSupported("INSERT clause of MERGE operation", "col1")
      }
      checkError(e, "DELTA_UNSUPPORTED_NESTED_FIELD_IN_OPERATION", "0AKDC",
        Map("operation" -> "INSERT clause of MERGE operation", "fieldName" -> "col1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.newCheckConstraintViolated(10, "table-1", "sample")
      }
      checkError(e, "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION", "23512", Map(
        "checkConstraint" -> "sample",
        "tableName" -> "table-1",
        "numRows" -> "10"
      ))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.failedInferSchema
      }
      checkError(e, "DELTA_FAILED_INFER_SCHEMA", "42KD9", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartialScan(new Path("path-1"))
      }
      checkError(e, "DELTA_UNEXPECTED_PARTIAL_SCAN", "KD00A", Map("path" -> "path-1"))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedLogFile(new Path("path-1"))
      }
      checkError(e, "DELTA_UNRECOGNIZED_LOGFILE", "KD00B", Map("filename" -> "path-1"))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unsupportedAbsPathAddFile("path-1")
      }
      checkError(e, "DELTA_UNSUPPORTED_ABS_PATH_ADD_FILE", "0AKDC", Map("path" -> "path-1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.outputModeNotSupportedException("source1", "sample")
      }
      checkError(e, "DELTA_UNSUPPORTED_OUTPUT_MODE", "0AKDC",
        Map("dataSource" -> "source1", "mode" -> "sample"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US)
        throw DeltaErrors.timestampGreaterThanLatestCommit(
          new Timestamp(sdf.parse("2022-02-28 10:30:00").getTime),
          new Timestamp(sdf.parse("2022-02-28 10:00:00").getTime), "2022-02-28 10:00:00")
      }
      checkError(e, "DELTA_TIMESTAMP_GREATER_THAN_COMMIT", "42816", Map(
        "providedTimestamp" -> "2022-02-28 10:30:00.0",
        "tableName" -> "2022-02-28 10:00:00.0",
        "maximumTimestamp" -> "2022-02-28 10:00:00"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US)
        throw DeltaErrors.TimestampEarlierThanCommitRetentionException(
          new Timestamp(sdf.parse("2022-02-28 10:00:00").getTime),
          new Timestamp(sdf.parse("2022-02-28 11:00:00").getTime),
          "2022-02-28 11:00:00")
      }
      checkError(e, "DELTA_TIMESTAMP_EARLIER_THAN_COMMIT_RETENTION", "42816", Map(
        "userTimestamp" -> "2022-02-28 10:00:00.0",
        "commitTs" -> "2022-02-28 11:00:00.0",
        "timestampString" -> "2022-02-28 11:00:00"))
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.timestampInvalid(expr)
      }
      checkError(e, "DELTA_TIMESTAMP_INVALID", "42816", Map("expr" -> expr.sql))
    }
    {
      val version = "null"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.versionInvalid(version)
      }
      checkError(e, "DELTA_VERSION_INVALID", "42815", Map("version" -> version))
    }
    {
      val version = 2
      val earliest = 0
      val latest = 1
      val e = intercept[DeltaAnalysisException] {
        throw VersionNotFoundException(version, earliest, latest)
      }
      checkError(e, "DELTA_VERSION_NOT_FOUND", "22003", Map(
        "userVersion" -> version.toString,
        "earliest" -> earliest.toString,
        "latest" -> latest.toString))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaSourceException("sample")
      }
      checkError(e, "DELTA_UNSUPPORTED_SOURCE", "0AKDD",
        Map("operation" -> "sample", "plan" -> ""))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreTimestampGreaterThanLatestException("2022-02-02 12:12:12", "2022-02-02 12:12:10")
      }
      checkError(e, "DELTA_CANNOT_RESTORE_TIMESTAMP_GREATER", "22003", Map(
        "requestedTimestamp" -> "2022-02-02 12:12:12",
        "latestTimestamp" -> "2022-02-02 12:12:10"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnStructNotFoundException("pos1")
      }
      checkError(e, "DELTA_ADD_COLUMN_STRUCT_NOT_FOUND", "42KD3", Map(
        "position" -> "pos1"
      ))
    }
    {
      val column = StructField("c0", IntegerType)
      val other = IntegerType
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnParentNotStructException(column, other)
      }
      checkError(e, "DELTA_ADD_COLUMN_PARENT_NOT_STRUCT", "42KD3", Map(
        "columnName" -> column.name,
        "other" -> other.toString
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateNonStructTypeFieldNotSupportedException("col1", DataTypes.DateType)
      }
      checkError(e, "DELTA_UNSUPPORTED_FIELD_UPDATE_NON_STRUCT", "0AKDC",
        Map("columnName" -> "`col1`", "dataType" -> "DateType"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.extractReferencesFieldNotFound("struct1",
          DeltaErrors.updateSchemaMismatchExpression(
            StructType(Seq(StructField("c0", IntegerType))),
            StructType(Seq(StructField("c1", IntegerType)))
          ))
      }
      checkError(e, "DELTA_EXTRACT_REFERENCES_FIELD_NOT_FOUND", "XXKDS",
        Map("fieldName" -> "struct1"))
    }
    {
      val e = intercept[DeltaIndexOutOfBoundsException] {
        throw DeltaErrors.notNullColumnNotFoundInStruct("struct1")
      }
      checkError(e, "DELTA_NOT_NULL_COLUMN_NOT_FOUND_IN_STRUCT", "42K09",
        Map("struct" -> "struct1"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidIdempotentWritesOptionsException("someReason")
      }
      checkError(e, "DELTA_INVALID_IDEMPOTENT_WRITES_OPTIONS", "42616",
        Map("reason" -> "someReason"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("dummyOp")
      }
      checkError(e, "DELTA_OPERATION_NOT_ALLOWED", "0AKDC",
        Map("operation" -> "dummyOp"))
    }
    {
      val s1 = StructType(Seq(StructField("c0", IntegerType)))
      val s2 = StructType(Seq(StructField("c0", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.alterTableSetLocationSchemaMismatchException(s1, s2)
      }
      checkError(e, "DELTA_SET_LOCATION_SCHEMA_MISMATCH", "42KD7", Map(
        "original" -> s1.treeString,
        "destination" -> s2.treeString,
        "config" -> DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK.key))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundDuplicateColumnsException("integer", "col1")
      }
      checkError(e, "DELTA_DUPLICATE_COLUMNS_FOUND", "42711",
        Map("coltype" -> "integer", "duplicateCols" -> "col1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.subqueryNotSupportedException("dummyOp", "col1")
      }
      checkError(e, "DELTA_UNSUPPORTED_SUBQUERY", "0AKDC",
        Map("operation" -> "dummyOp", "cond" -> "'col1'"))
    }
    {
      val schema = StructType(Array(StructField("foo", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundMapTypeColumnException("dummyKey", "dummyVal", schema)
      }
      checkError(e, "DELTA_FOUND_MAP_TYPE_COLUMN", "KD003", Map(
        "key" -> "dummyKey",
        "value" -> "dummyVal",
        "schema" -> schema.treeString
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnOfTargetTableNotFoundInMergeException("target", "dummyCol")
      }
      checkError(e, "DELTA_COLUMN_NOT_FOUND_IN_MERGE", "42703",
        Map("targetCol" -> "target", "colNames" -> "dummyCol"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.multiColumnInPredicateNotSupportedException("dummyOp")
      }
      checkError(e, "DELTA_UNSUPPORTED_MULTI_COL_IN_PREDICATE", "0AKDC",
        Map("operation" -> "dummyOp"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.newNotNullViolated(10L, "table1", UnresolvedAttribute("col1"))
      }
      checkError(e, "DELTA_NEW_NOT_NULL_VIOLATION", "23512",
        Map("numRows" -> "10", "tableName" -> "table1", "colName" -> "col1"))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.modifyAppendOnlyTableException("dummyTable")
      }
      checkError(e, "DELTA_CANNOT_MODIFY_APPEND_ONLY", "42809",
        Map("table_name" -> "dummyTable", "config" -> DeltaConfigs.IS_APPEND_ONLY.key))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.schemaNotConsistentWithTarget("dummySchema", "targetAttr")
      }
      checkError(e, "DELTA_SCHEMA_NOT_CONSISTENT_WITH_TARGET", "XXKDS", Map(
        "tableSchema" -> "dummySchema",
        "targetAttrs" -> "targetAttr"
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkTaskThreadNotFound
      }
      checkError(e, "DELTA_SPARK_THREAD_NOT_FOUND", "XXKDS", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.staticPartitionsNotSupportedException
      }
      checkError(e, "DELTA_UNSUPPORTED_STATIC_PARTITIONS", "0AKDD", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedWriteStagedTable("table1")
      }
      checkError(e, "DELTA_UNSUPPORTED_WRITES_STAGED_TABLE", "42807", Map(
        "tableName" -> "table1"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.vacuumBasePathMissingException(new Path("path-1"))
      }
      checkError(e, "DELTA_UNSUPPORTED_VACUUM_SPECIFIC_PARTITION", "0AKDC",
        Map("baseDeltaPath" -> "path-1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterCreateOnNonExistingColumnsException(Seq("col1", "col2"))
      }
      checkError(e, "DELTA_CANNOT_CREATE_BLOOM_FILTER_NON_EXISTING_COL", "42703",
        Map("unknownCols" -> "col1, col2"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.zOrderingColumnDoesNotExistException("colName")
      }
      checkError(e, "DELTA_ZORDERING_COLUMN_DOES_NOT_EXIST", "42703", Map("columnName" -> "colName"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.zOrderingOnPartitionColumnException("column1")
      }
      checkError(e, "DELTA_ZORDERING_ON_PARTITION_COLUMN", "42P10", Map("colName" -> "column1"))
    }
    {
      val colNames = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.zOrderingOnColumnWithNoStatsException(colNames, spark)
      }
      checkError(e, "DELTA_ZORDERING_ON_COLUMN_WITHOUT_STATS", "KD00D", Map(
        "cols" -> "[col1, col2]",
        "zorderColStatKey" -> DeltaSQLConf.DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK.key
      ))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw MaterializedRowId.missingMetadataException("table_name")
      }
      checkError(e, "DELTA_MATERIALIZED_ROW_TRACKING_COLUMN_NAME_MISSING", "22000",
        Map("rowTrackingColumn" -> "Row ID", "tableName" -> "table_name"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw MaterializedRowCommitVersion.missingMetadataException("table_name")
      }
      checkError(e, "DELTA_MATERIALIZED_ROW_TRACKING_COLUMN_NAME_MISSING", "22000", Map(
        "rowTrackingColumn" -> "Row Commit Version",
        "tableName" -> "table_name"
      ))
    }
    {
      val path = new Path("a/b")
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.catalogManagedTablePathBasedAccessNotAllowed(path)
      }
      checkError(e, "DELTA_PATH_BASED_ACCESS_TO_CATALOG_MANAGED_TABLE_BLOCKED", "KD00G",
        Map("path" -> path.toString))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.operationBlockedOnCatalogManagedTable("OPTIMIZE")
      }
      checkError(e, "DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION", "0AKDC",
        Map("operation" -> "OPTIMIZE"))
    }
  }

  // The compiler complains the lambda function is too large if we put all tests in one lambda.
  test("test DeltaErrors OSS methods more") {
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaNotSetException
      }
      checkError(e, "DELTA_SCHEMA_NOT_SET", "KD008", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaNotProvidedException
      }
      checkError(e, "DELTA_SCHEMA_NOT_PROVIDED", "42908", Map.empty[String, String])
    }
    {
      val st1 = StructType(Seq(StructField("a0", IntegerType)))
      val st2 = StructType(Seq(StructField("b0", IntegerType)))
      val schemaDiff = SchemaUtils.reportDifferences(st1, st2)
        .map(_.replace("Specified", "Latest"))

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaChangedSinceAnalysis(st1, st2)
      }
      checkError(e, "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS", "KD007", Map(
        "schemaDiff" ->
          "Latest schema is missing field(s): a0\nLatest schema has additional field(s): b0",
        "legacyFlagMessage" -> ""))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsAggregateExpression("1".expr)
      }
      checkError(e, "DELTA_AGGREGATE_IN_GENERATED_COLUMN", "42621", Map("sqlExpr" -> "'1'"))
    }
    {
      val path = new Path("somePath")
      val specifiedColumns = Seq("col1", "col2")
      val existingColumns = Seq("col3", "col4")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentPartitioningException(path, specifiedColumns, existingColumns)
      }
      checkError(e, "DELTA_CREATE_TABLE_WITH_DIFFERENT_PARTITIONING", "42KD7", Map(
        "path" -> "somePath",
        "specifiedColumns" -> "col1, col2",
        "existingColumns" -> "col3, col4"
      ))
    }
    {
      val path = new Path("a/b")
      val smaps = Map("abc" -> "xyz")
      val emaps = Map("def" -> "hjk")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentPropertiesException(path, smaps, emaps)
      }

      checkError(e, "DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY", "42KD7", Map(
        "path" -> path.toString,
        "specifiedProperties" -> smaps.map { case (k, v) => s"$k=$v" }.mkString("\n"),
        "existingProperties" -> emaps.map { case (k, v) => s"$k=$v" }.mkString("\n")
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportSubqueryInPartitionPredicates()
      }
      checkError(e, "DELTA_UNSUPPORTED_SUBQUERY_IN_PARTITION_PREDICATES", "0AKDC",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.emptyDirectoryException("dir")
      }
      checkError(e, "DELTA_EMPTY_DIRECTORY", "42K03", Map("directory" -> "dir"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.replaceWhereUsedWithDynamicPartitionOverwrite()
      }
      checkError(e, "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE", "42613", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereUsedInOverwrite()
      }
      checkError(e, "DELTA_REPLACE_WHERE_IN_OVERWRITE", "42613", Map.empty[String, String])
    }
    {
      val schema = StructType(Array(StructField("foo", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.incorrectArrayAccessByName("right", "wrong", schema)
      }
      checkError(e, "DELTA_INCORRECT_ARRAY_ACCESS_BY_NAME", "KD003", Map(
        "rightName" -> "right",
        "wrongName" -> "wrong",
        "schema" -> schema.treeString))
    }
    {
      val columnPath = "colPath"
      val other = IntegerType
      val column = Seq("col1", "col2")
      val schema = StructType(Array(StructField("foo", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnPathNotNested(columnPath, other, column, schema)
      }
      checkError(e, "DELTA_COLUMN_PATH_NOT_NESTED", "42704", Map(
        "columnPath" -> columnPath,
        "other" -> other.toString,
        "column" -> column.mkString("."),
        "schema" -> schema.treeString))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
      }
      val docLink = generateDocsLink(multipleSourceRowMatchingTargetRowInMergeUrl)
      checkError(e, "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE", "21506",
        Map("usageReference" -> docLink))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.showPartitionInNotPartitionedTable("table")
      }
      checkError(e, "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_TABLE", "42809",
        Map("tableName" -> "table"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.showPartitionInNotPartitionedColumn(Set("col1", "col2"))
      }
      checkError(e, "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_COLUMN", "42P10",
        Map("badCols" -> "[col1, col2]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.duplicateColumnOnInsert()
      }
      checkError(e, "DELTA_DUPLICATE_COLUMNS_ON_INSERT", "42701", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.timeTravelInvalidBeginValue("key", new Throwable)
      }
      checkError(e, "DELTA_TIME_TRAVEL_INVALID_BEGIN_VALUE", "42604",
        Map("timeTravelKey" -> "key"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.metadataAbsentException()
      }
      checkError(e, "DELTA_METADATA_ABSENT", "XXKDS", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(errorClass = "DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION",
          Array.empty)
      }
      checkError(e, "DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION", "428FT",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.failedReadFileFooter("test.txt", null)
      }
      checkError(e, "DELTA_FAILED_READ_FILE_FOOTER", "KD001",
        Map("currentFile" -> "test.txt"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedScanWithHistoricalVersion(123)
      }
      checkError(e, "DELTA_FAILED_SCAN_WITH_HISTORICAL_VERSION", "KD002",
        Map("historicalVersion" -> "123"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedRecognizePredicate("select ALL", new Throwable())
      }
      checkError(e, "DELTA_FAILED_RECOGNIZE_PREDICATE", "42601",
        Map("predicate" -> "select ALL"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedFindAttributeInOutputColumns("col1",
          "col2,col3,col4")
      }
      checkError(e, "DELTA_FAILED_FIND_ATTRIBUTE_IN_OUTPUT_COLUMNS", "42703",
        Map("newAttributeName" -> "col1", "targetOutputColumns" -> "col2,col3,col4"))
    }
    {
      val col = "col1"
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failedFindPartitionColumnInOutputPlan(col)
      }
      checkError(e, "DELTA_FAILED_FIND_PARTITION_COLUMN_IN_OUTPUT_PLAN", "XXKDS",
        Map("partitionColumn" -> col))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.deltaTableFoundInExecutor()
      }
      checkError(e, "DELTA_TABLE_FOUND_IN_EXECUTOR", "XXKDS", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaFileAlreadyExistsException] {
        throw DeltaErrors.fileAlreadyExists("file.txt")
      }
      checkError(e, "DELTA_FILE_ALREADY_EXISTS", "42K04",
        Map("path" -> "file.txt"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.configureSparkSessionWithExtensionAndCatalog(Some(new Throwable()))
      }

      checkError(e, "DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG", "56038", Map(
        "sparkSessionExtensionName" -> classOf[DeltaSparkSessionExtension].getName,
        "catalogKey" -> SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        "catalogClassName" -> classOf[DeltaCatalog].getName
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cdcNotAllowedInThisVersion()
      }
      checkError(e, "DELTA_CDC_NOT_ALLOWED_IN_THIS_VERSION", "0AKDC", Map.empty[String, String])
    }
    {
      val ident = TableIdentifier("view1")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertNonParquetTablesException(ident, "source1")
      }
      checkError(e, "DELTA_CONVERT_NON_PARQUET_TABLE", "0AKDC",
        Map("sourceName" -> "source1", "tableId" -> "`view1`"))
    }
    {
      val from = StructType(Seq(StructField("c0", IntegerType)))
      val to = StructType(Seq(StructField("c1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSchemaMismatchExpression(from, to)
      }
      checkError(e, "DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION", "42846",
        Map("fromCatalog" -> "struct<c0:int>", "toCatalog" -> "struct<c1:int>"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.removeFileCDCMissingExtendedMetadata("someFile")
      }
      checkError(e, "DELTA_REMOVE_FILE_CDC_MISSING_EXTENDED_METADATA", "XXKDS",
        Map("file" -> "someFile"))
    }
    {
      val columnName = "c0"
      val colMatches = Seq(StructField("c0", IntegerType))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.ambiguousPartitionColumnException(columnName, colMatches)
      }
      checkError(e, "DELTA_AMBIGUOUS_PARTITION_COLUMN", "42702",
        Map("column" -> "`c0`", "colMatches" -> "[`c0`]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.truncateTablePartitionNotSupportedException
      }
      checkError(e, "DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED", "0AKDC",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidFormatFromSourceVersion(100, 10)
      }
      checkError(e, "DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION", "XXKDS",
        Map("expectedVersion" -> "10", "realVersion" -> "100"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.emptyDataException
      }
      checkError(e, "DELTA_EMPTY_DATA", "428GU", Map.empty[String, String])
    }
    {
      val path = "somePath"
      val parsedCol = "col1"
      val expectedCol = "col2"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(path, parsedCol, expectedCol)
      }
      checkError(e, "DELTA_UNEXPECTED_PARTITION_COLUMN_FROM_FILE_NAME", "KD009",
        Map("expectedCol" -> "`col2`", "parsedCol" -> "`col1`", "path" -> "somePath"))
    }
    {
      val path = "somePath"
      val parsedCols = Seq("col1", "col2")
      val expectedCols = Seq("col3", "col4")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(path, parsedCols, expectedCols)
      }
      checkError(e, "DELTA_UNEXPECTED_NUM_PARTITION_COLUMNS_FROM_FILE_NAME", "KD009", Map(
        "expectedCols" -> "[`col3`, `col4`]",
        "path" -> "somePath",
        "parsedCols" -> "[`col1`, `col2`]",
        "parsedColsSize" -> "2",
        "expectedColsSize" -> "2"))
    }
    {
      val version = 100L
      val removedFile = "file"
      val dataPath = "tablePath"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(version, removedFile, dataPath)
      }
      checkError(e, "DELTA_SOURCE_IGNORE_DELETE", "0A000",
        Map("removedFile" -> "file", "version" -> "100", "dataPath" -> "tablePath"))
    }
    {
      val tableId = "someTableId"
      val tableLocation = "path"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithNonEmptyLocation(tableId, tableLocation)
      }
      checkError(e, "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION", "42601",
        Map("tableId" -> "someTableId", "tableLocation" -> "path"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.maxArraySizeExceeded()
      }
      checkError(e, "DELTA_MAX_ARRAY_SIZE_EXCEEDED", "42000", Map.empty[String, String])
    }
    {
      val unknownColumns = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterDropOnNonExistingColumnsException(unknownColumns)
      }
      checkError(e, "DELTA_BLOOM_FILTER_DROP_ON_NON_EXISTING_COLUMNS", "42703",
        Map("unknownColumns" -> unknownColumns.mkString(", ")))
    }
    {
      val dataFilters = "filters"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters)
      }

      checkError(e, "DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET", "42613",
        Map("dataFilters" -> dataFilters))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingTableIdentifierException("read")
      }
      checkError(e, "DELTA_OPERATION_MISSING_PATH", "42601",
        Map("operation" -> "read"))
    }
    {
      val column = StructField("c0", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUseDataTypeForPartitionColumnError(column)
      }
      checkError(e, "DELTA_INVALID_PARTITION_COLUMN_TYPE", "42996",
        Map("name" -> "c0", "dataType" -> "IntegerType"))
    }
    {
      val catalogPartitionSchema = StructType(Seq(StructField("a", IntegerType)))
      val userPartitionSchema = StructType(Seq(StructField("b", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartitionSchemaFromUserException(catalogPartitionSchema,
          userPartitionSchema)
      }
      checkError(e, "DELTA_UNEXPECTED_PARTITION_SCHEMA_FROM_USER", "KD009", Map(
        "catalogPartitionSchema" -> catalogPartitionSchema.treeString,
        "userPartitionSchema" -> userPartitionSchema.treeString))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidInterval("interval1")
      }
      checkError(e, "DELTA_INVALID_INTERVAL", "22006", Map("interval" -> "interval1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cdcWriteNotAllowedInThisVersion
      }
      checkError(e, "DELTA_CHANGE_TABLE_FEED_DISABLED", "42807", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.specifySchemaAtReadTimeException
      }
      checkError(e, "DELTA_UNSUPPORTED_SCHEMA_DURING_READ", "0AKDC", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.readSourceSchemaConflictException
      }
      checkError(e, "DELTA_READ_SOURCE_SCHEMA_CONFLICT", "42K07", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedDataChangeException("operation1")
      }
      checkError(e, "DELTA_DATA_CHANGE_FALSE", "0AKDE", Map("op" -> "operation1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noStartVersionForCDC
      }
      checkError(e, "DELTA_NO_START_FOR_CDC_READ", "42601", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedColumnChange("change1")
      }
      checkError(e, "DELTA_UNRECOGNIZED_COLUMN_CHANGE", "42601", Map("otherClass" -> "change1"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.nullRangeBoundaryInCDCRead()
      }
      checkError(e, "DELTA_CDC_READ_NULL_RANGE_BOUNDARY", "22004", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.endBeforeStartVersionInCDC(2, 1)
      }
      checkError(e, "DELTA_INVALID_CDC_RANGE", "22003",
        Map("start" -> "2", "end" -> "1"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unexpectedChangeFilesFound("a.parquet")
      }
      checkError(e, "DELTA_UNEXPECTED_CHANGE_FILES_FOUND", "XXKDS",
        Map("fileList" -> "a.parquet"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.logFailedIntegrityCheck(2, "option1")
      }
      checkError(e, "DELTA_TXN_LOG_FAILED_INTEGRITY", "XXKDS", Map(
        "version" -> "2",
        "mismatchStringOpt" -> "option1"
      ))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.checkpointNonExistTable(path)
      }
      checkError(e, "DELTA_CHECKPOINT_NON_EXIST_TABLE", "42K03", Map("path" -> path.toString))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedDeepCloneException()
      }
      checkError(e, "DELTA_UNSUPPORTED_DEEP_CLONE", "0A000", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.viewInDescribeDetailException(TableIdentifier("customer"))
      }
      checkError(e, "DELTA_UNSUPPORTED_DESCRIBE_DETAIL_VIEW", "42809", Map("view" -> "`customer`"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathAlreadyExistsException(new Path(path))
      }
      checkError(e, "DELTA_PATH_EXISTS", "42K04", Map("path" -> "/sample/path"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "DELTA_MERGE_MISSING_WHEN",
          messageParameters = Array.empty
        )
      }
      checkError(e, "DELTA_MERGE_MISSING_WHEN", "42601", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unrecognizedFileAction("invalidAction", "invalidClass")
      }
      checkError(e, "DELTA_UNRECOGNIZED_FILE_ACTION", "XXKDS", Map(
        "action" -> "invalidAction",
        "actionClass" -> "invalidClass"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.streamWriteNullTypeException
      }
      checkError(e, "DELTA_NULL_SCHEMA_IN_STREAMING_WRITE", "42P18", Map.empty[String, String])
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaIllegalArgumentException] {
        throw new DeltaIllegalArgumentException(
          errorClass = "DELTA_UNEXPECTED_ACTION_EXPRESSION",
          messageParameters = Array(s"$expr"))
      }
      checkError(e, "DELTA_UNEXPECTED_ACTION_EXPRESSION", "42601",
        Map("expression" -> "1"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unexpectedAlias("alias1")
      }
      checkError(e, "DELTA_UNEXPECTED_ALIAS", "XXKDS", Map("alias" -> "alias1"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unexpectedProject("project1")
      }
      checkError(e, "DELTA_UNEXPECTED_PROJECT", "XXKDS", Map("project" -> "project1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nullableParentWithNotNullNestedField
      }
      checkError(e, "DELTA_NOT_NULL_NESTED_FIELD", "0A000", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.useAddConstraints
      }
      checkError(e, "DELTA_ADD_CONSTRAINTS", "0A000", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.deltaSourceIgnoreChangesError(10, "removedFile", "tablePath")
      }
      checkError(e, "DELTA_SOURCE_TABLE_IGNORE_CHANGES", "0A000", Map(
        "version" -> "10",
        "file" -> "removedFile",
        "dataPath" -> "tablePath"
      ))
    }
    {
      val limit = "someLimit"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unknownReadLimit(limit)
      }
      checkError(e, "DELTA_UNKNOWN_READ_LIMIT", "42601", Map("limit" -> limit))
    }
    {
      val privilege = "unknown"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unknownPrivilege(privilege)
      }
      checkError(e, "DELTA_UNKNOWN_PRIVILEGE", "42601", Map("privilege" -> privilege))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.deltaLogAlreadyExistsException("somePath")
      }
      checkError(e, "DELTA_LOG_ALREADY_EXISTS", "42K04", Map("path" -> "somePath"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingPartFilesException(10L, new FileNotFoundException("reason"))
      }
      checkError(e, "DELTA_MISSING_PART_FILES", "42KD6", Map("version" -> "10"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.checkConstraintNotBoolean("name1", "expr1")
      }
      checkError(e, "DELTA_NON_BOOLEAN_CHECK_CONSTRAINT", "42621", Map("name" -> "name1", "expr" -> "expr1"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.checkpointMismatchWithSnapshot
      }
      checkError(e, "DELTA_CHECKPOINT_SNAPSHOT_MISMATCH", "XXKDS", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException("operation1")
      }
      checkError(e, "DELTA_ONLY_OPERATION", "0AKDD", Map("operation" -> "operation1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropNestedColumnsFromNonStructTypeException(StringType)
      }
      checkError(e, "DELTA_UNSUPPORTED_DROP_NESTED_COLUMN_FROM_NON_STRUCT_TYPE", "0AKDC",
        Map("struct" -> "StringType"))
    }
    {
      val locations = Seq("location1", "location2")
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cannotSetLocationMultipleTimes(locations)
      }
      checkError(e, "DELTA_CANNOT_SET_LOCATION_MULTIPLE_TIMES", "XXKDS",
        Map("location" -> "List(location1, location2)"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.metadataAbsentForExistingCatalogTable("tblName", "file://path/to/table")
      }
      checkError(e, "DELTA_METADATA_ABSENT_EXISTING_CATALOG_TABLE", "XXKDS", Map(
        "tableName" -> "tblName",
        "tablePath" -> "file://path/to/table",
        "tableNameForDropCmd" -> "tblName"
      ))
    }
    {
      val e = intercept[DeltaStreamingNonAdditiveSchemaIncompatibleException] {
        throw DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
          spark,
          StructType.fromDDL("id int"),
          StructType.fromDDL("id2 int"),
          detectedDuringStreaming = true
        )
      }
      val docLink = generateDocsLink("/versioning.html#column-mapping")
      checkError(e, "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG", "42KD4", Map(
        "docLink" -> docLink,
        "readSchema" -> StructType.fromDDL("id int").json,
        "incompatibleSchema" -> StructType.fromDDL("id2 int").json
      ))
      assert(e.additionalProperties("detectedDuringStreaming").toBoolean)
    }
    {
      val e = intercept[DeltaStreamingNonAdditiveSchemaIncompatibleException] {
        throw DeltaErrors.blockStreamingReadsWithIncompatibleNonAdditiveSchemaChanges(
          spark,
          StructType.fromDDL("id int"),
          StructType.fromDDL("id2 int"),
          detectedDuringStreaming = false
        )
      }
      val docLink = generateDocsLink("/versioning.html#column-mapping")
      checkError(e, "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG", "42KD4", Map(
        "docLink" -> docLink,
        "readSchema" -> StructType.fromDDL("id int").json,
        "incompatibleSchema" -> StructType.fromDDL("id2 int").json
      ))
      assert(!e.additionalProperties("detectedDuringStreaming").toBoolean)
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.cannotContinueStreamingPostSchemaEvolution(
          nonAdditiveSchemaChangeOpType = "RENAME AND TYPE WIDENING",
          previousSchemaChangeVersion = 0,
          currentSchemaChangeVersion = 1,
          readerOptionsUnblock = Seq("allowSourceColumnRename", "allowSourceColumnTypeChange"),
          sqlConfsUnblock = Seq(
            "spark.databricks.delta.streaming.allowSourceColumnRename",
            "spark.databricks.delta.streaming.allowSourceColumnTypeChange"),
          checkpointHash = 15,
          prettyColumnChangeDetails = "some column details")
      }
      checkError(e,
        "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION",
        parameters = Map(
          "opType" -> "RENAME AND TYPE WIDENING",
          "previousSchemaChangeVersion" -> "0",
          "currentSchemaChangeVersion" -> "1",
          "columnChangeDetails" -> "some column details",
          "unblockChangeOptions" ->
            s"""  .option("allowSourceColumnRename", "1")
               |  .option("allowSourceColumnTypeChange", "1")""".stripMargin,
          "unblockStreamOptions" ->
            s"""  .option("allowSourceColumnRename", "always")
               |  .option("allowSourceColumnTypeChange", "always")""".stripMargin,
          "unblockChangeConfs" ->
            s"""  SET spark.databricks.delta.streaming.allowSourceColumnRename.ckpt_15 = 1;
               |  SET spark.databricks.delta.streaming.allowSourceColumnTypeChange.ckpt_15 = 1;""".stripMargin,
          "unblockStreamConfs" ->
            s"""  SET spark.databricks.delta.streaming.allowSourceColumnRename.ckpt_15 = "always";
               |  SET spark.databricks.delta.streaming.allowSourceColumnTypeChange.ckpt_15 = "always";""".stripMargin,
          "unblockAllConfs" ->
            s"""  SET spark.databricks.delta.streaming.allowSourceColumnRename = "always";
               |  SET spark.databricks.delta.streaming.allowSourceColumnTypeChange = "always";""".stripMargin
        )
      )
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.blockColumnMappingAndCdcOperation(DeltaOperations.ManualUpdate)
      }
      checkError(e, "DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION", "42KD4",
        Map("opName" -> "Manual Update"))
    }
    {
      val options = Map(
        "foo" -> "1",
        "bar" -> "2"
      )
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedDeltaTableForPathHadoopConf(options)
      }
      val prefixStr = DeltaTableUtils.validDeltaTableHadoopPrefixes.mkString("[", ",", "]")
      checkError(e, "DELTA_TABLE_FOR_PATH_UNSUPPORTED_HADOOP_CONF", "0AKDC",
        Map("allowedPrefixes" -> prefixStr, "unsupportedOptions" -> "foo -> 1,bar -> 2"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneOnRelativePath("somePath")
      }
      checkError(e, "DELTA_INVALID_CLONE_PATH", "22KD1", Map("path" -> "somePath"))
    }
    {
      val e = intercept[AnalysisException] {
        throw DeltaErrors.cloneFromUnsupportedSource( "table-0", "CSV")
      }
      checkError(e, "DELTA_CLONE_UNSUPPORTED_SOURCE", "0AKDC",
        Map("name" -> "table-0", "format" -> "CSV"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneReplaceUnsupported(TableIdentifier("customer"))
      }
      checkError(e, "DELTA_UNSUPPORTED_CLONE_REPLACE_SAME_TABLE", "0AKDC",
        Map("tableName" -> "`customer`"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneAmbiguousTarget("external-location", TableIdentifier("table1"))
      }
      checkError(e, "DELTA_CLONE_AMBIGUOUS_TARGET", "42613",
        Map("externalLocation" -> "external-location", "targetIdentifier" -> "`table1`"))
    }
    {
      DeltaTableValueFunctions.supportedFnNames.foreach { fnName =>
        {
          val fnCall = s"${fnName}()"
          val e = intercept[DeltaAnalysisException] {
            sql(s"SELECT * FROM $fnCall").collect()
          }
          checkError(e, "INCORRECT_NUMBER_OF_ARGUMENTS", "42605",
            Map("failure" -> "not enough args", "functionName" -> fnName, "minArgs" -> "2",
              "maxArgs" -> "3"),
            ExpectedContext(fragment = fnCall, start = 14, stop = 14 + fnCall.length - 1))
        }
        {
          val fnCall = s"${fnName}(1, 2, 3, 4, 5)"
          val e = intercept[DeltaAnalysisException] {
            sql(s"SELECT * FROM ${fnCall}").collect()
          }
          checkError(e, "INCORRECT_NUMBER_OF_ARGUMENTS", "42605",
            Map("failure" -> "too many args", "functionName" -> fnName, "minArgs" -> "2",
              "maxArgs" -> "3"),
            ExpectedContext(fragment = fnCall, start = 14, stop = 14 + fnCall.length - 1))
        }
      }
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidTableValueFunction("invalid1")
      }
      checkError(e, "DELTA_INVALID_TABLE_VALUE_FUNCTION", "22000",
        Map("function" -> "invalid1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED",
          messageParameters = Array("ALTER TABLE"))
      }
      checkError(e, "WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED", "0AKDE",
        Map("commandType" -> "ALTER TABLE"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED",
          messageParameters = Array.empty)
      }
      checkError(e, "WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED",
        "0AKDC", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingCommitInfo("featureName", "1225")
      }
      checkError(e, "DELTA_MISSING_COMMIT_INFO", "KD004",
        Map("featureName" -> "featureName", "version" -> "1225"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingCommitTimestamp("1225")
      }
      checkError(e, "DELTA_MISSING_COMMIT_TIMESTAMP", "KD004",
        Map("featureName" -> "inCommitTimestamp", "version" -> "1225"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidConstraintName("foo")
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0001", None, Map("name" -> "foo"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterInvalidParameterValueException("foo")
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0002", None, Map("message" -> "foo"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertMetastoreMetadataMismatchException(
          tableProperties = Map("delta.prop1" -> "foo"),
          deltaConfiguration = Map("delta.config1" -> "bar"))
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0003", None, Map(
        "tableProperties" -> "[delta.prop1=foo]",
        "configuration" -> "[delta.config1=bar]",
        "metadataCheckSqlConf" -> DeltaSQLConf.DELTA_CONVERT_METADATA_CHECK_ENABLED.key))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreTimestampBeforeEarliestException("2022-02-02 12:12:12",
          "2022-02-02 12:12:14")
      }
      checkError(e, "DELTA_CANNOT_RESTORE_TIMESTAMP_EARLIER", "22003", Map(
        "requestedTimestamp" -> "2022-02-02 12:12:12",
        "earliestTimestamp" -> "2022-02-02 12:12:14"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.viewNotSupported("FOO_OP")
      }
      checkError(e, "DELTA_OPERATION_ON_VIEW_NOT_ALLOWED", "0AKDC",
        Map("operation" -> "FOO_OP"))
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsNonDeterministicExpression(expr)
      }
      checkError(e, "DELTA_NON_DETERMINISTIC_EXPRESSION_IN_GENERATED_COLUMN", "42621",
        Map("expr" -> "'1'"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnBuilderMissingDataType("col1")
      }
      checkError(e, "DELTA_COLUMN_MISSING_DATA_TYPE", "42601", Map("colName" -> "`col1`"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundViolatingConstraintsForColumnChange(
          "col1", Map("foo" -> "bar"))
      }
      checkError(e, "DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE", "42K09",
        Map("columnName" -> "col1", "constraints" -> "foo -> bar"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableMissingTableNameOrLocation()
      }
      checkError(e, "DELTA_CREATE_TABLE_MISSING_TABLE_NAME_OR_LOCATION", "42601", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableIdentifierLocationMismatch("delta.`somePath1`", "somePath2")
      }
      checkError(e, "DELTA_CREATE_TABLE_IDENTIFIER_LOCATION_MISMATCH", "0AKDC",
        Map("identifier" -> "delta.`somePath1`", "location" -> "somePath2"))
    }
    {
      val schema = StructType(Seq(StructField("col1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropColumnOnSingleFieldSchema(schema)
      }
      checkError(e, "DELTA_DROP_COLUMN_ON_SINGLE_FIELD_SCHEMA", "0AKDC",
        Map("schema" -> schema.treeString))
    }
    {
      val schema = StructType(Seq(StructField("col1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.errorFindingColumnPosition(Seq("col2"), schema, "foo")
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0008", None, Map(
        "column" -> "col2",
        "schema" -> schema.treeString,
        "message" -> "foo"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundViolatingGeneratedColumnsForColumnChange(
          columnName = "col1",
          generatedColumns = Map("col2" -> "col1 + 1", "col3" -> "col1 + 2"))
      }
      checkError(e, "DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE", "42K09", Map(
        "columnName" -> "col1",
        "generatedColumns" -> "col2 -> col1 + 1\ncol3 -> col1 + 2"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.identityColumnInconsistentMetadata("col1", true, true, true)
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0006", None, Map(
        "colName" -> "col1", "hasStart" -> "true", "hasStep" -> "true", "hasInsert" -> "true"))
    }
    {
      // Test MetadataMismatchErrorBuilder with single sub-error (schema mismatch)
      val errorBuilder = new MetadataMismatchErrorBuilder()
      val schema1 = StructType(Seq(StructField("c0", IntegerType)))
      val schema2 = StructType(Seq(StructField("c0", StringType)))
      errorBuilder.addSchemaMismatch(schema1, schema2, "id")
      val e = intercept[DeltaAnalysisException] {
        errorBuilder.finalizeAndThrow(spark.sessionState.conf)
      }
      checkError(e, "DELTA_METADATA_MISMATCH", "42KDG", Map.empty[String, String])
      // Verify complete message format with main message + sub-error bullet
      val message = e.getMessage
      assert(message.contains(
        """[DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.
          |- A schema mismatch detected when writing to the Delta table (Table ID: id).
          |To enable schema migration using DataFrameWriter or DataStreamWriter, please set: '.option("mergeSchema", "true")'.
          |For other operations, set the session configuration spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation specific to the operation for details.
          |
          |Table schema:
          |root
          | |-- c0: integer (nullable = true)
          |
          |
          |Data schema:
          |root
          | |-- c0: string (nullable = true)
          |""".stripMargin))
    }
    // Test with multiple sub-errors
    {
      val errorBuilder = new MetadataMismatchErrorBuilder()
      val schema1 = StructType(Seq(StructField("c0", IntegerType)))
      val schema2 = StructType(Seq(StructField("c0", StringType)))
      errorBuilder.addSchemaMismatch(schema1, schema2, "test-id")
      errorBuilder.addPartitioningMismatch(Seq("part1"), Seq("part2"))
      errorBuilder.addOverwriteBit()
      val e = intercept[DeltaAnalysisException] {
        errorBuilder.finalizeAndThrow(spark.sessionState.conf)
      }
      checkError(e, "DELTA_METADATA_MISMATCH", "42KDG", Map.empty[String, String])
      // Verify complete message format with main message + three sub-error bullets
      val message = e.getMessage
      assert(message.contains(
        """[DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.
          |- A schema mismatch detected when writing to the Delta table (Table ID: test-id).
          |To enable schema migration using DataFrameWriter or DataStreamWriter, please set: '.option("mergeSchema", "true")'.
          |For other operations, set the session configuration spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation specific to the operation for details.
          |
          |Table schema:
          |root
          | |-- c0: integer (nullable = true)
          |
          |
          |Data schema:
          |root
          | |-- c0: string (nullable = true)
          |
          |
          |- Partition columns do not match the partition columns of the table.
          |Given: [`part2`]
          |Table: [`part1`]
          |
          |- To overwrite your schema or change partitioning, please set: '.option("overwriteSchema", "true")'.
          |Note that the schema can't be overwritten when using 'replaceWhere'.""".stripMargin))
    }
    // Test with partitioning mismatch only
    {
      val errorBuilder = new MetadataMismatchErrorBuilder()
      errorBuilder.addPartitioningMismatch(Seq("year", "month"), Seq("date"))
      val e = intercept[DeltaAnalysisException] {
        errorBuilder.finalizeAndThrow(spark.sessionState.conf)
      }
      checkError(e, "DELTA_METADATA_MISMATCH", "42KDG", Map.empty[String, String])
      // Verify complete message format with main message + one sub-error bullet
      val message = e.getMessage
      assert(message.contains(
        """[DELTA_METADATA_MISMATCH] A metadata mismatch was detected when writing to the Delta table.
          |- Partition columns do not match the partition columns of the table.
          |Given: [`date`]
          |Table: [`year`, `month`]
          |""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.mergeAddVoidColumn("fooCol")
      }
      checkError(e, "DELTA_MERGE_ADD_VOID_COLUMN", "42K09",
        Map("newColumn" -> "`fooCol`"))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentAppendException] {
        throw org.apache.spark.sql.delta.DeltaErrors
          .concurrentAppendException(None, "t", -1, partitionOpt = None)
      }
      checkError(e, "DELTA_CONCURRENT_APPEND.WITHOUT_HINT", "2D521",
        Map(
          "operation" -> "TRANSACTION", "tableName" -> "t", "version" -> "-1",
          "docLink" -> generateDocsLink("/concurrency-control.html")
        )
      )
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentAppendException] {
        throw org.apache.spark.sql.delta.DeltaErrors
          .concurrentAppendException(None, "t", -1, partitionOpt = Some("p1"))
      }
      checkError(e, "DELTA_CONCURRENT_APPEND.WITH_PARTITION_HINT", "2D521",
        Map("operation" -> "TRANSACTION", "tableName" -> "t", "version" -> "-1",
          "partitionValues" -> "p1",
          "docLink" -> generateDocsLink("/concurrency-control.html")))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentDeleteReadException] {
        throw org.apache.spark.sql.delta.DeltaErrors
          .concurrentDeleteReadException(None, "t", -1, partitionOpt = None)
      }
      checkError(e, "DELTA_CONCURRENT_DELETE_READ.WITHOUT_HINT", "2D521",
        Map("operation" -> "TRANSACTION", "tableName" -> "t", "version" -> "-1",
          "docLink" -> generateDocsLink("/concurrency-control.html")))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentDeleteReadException] {
        throw org.apache.spark.sql.delta.DeltaErrors
          .concurrentDeleteReadException(None, "t", -1, partitionOpt = Some("p1"))
      }
      checkError(e, "DELTA_CONCURRENT_DELETE_READ.WITH_PARTITION_HINT", "2D521",
        Map("operation" -> "TRANSACTION", "tableName" -> "t", "version" -> "-1",
          "partitionValues" -> "p1",
          "docLink" -> generateDocsLink("/concurrency-control.html")))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentDeleteDeleteException] {
        throw org.apache.spark.sql.delta.DeltaErrors
          .concurrentDeleteDeleteException(None, "t", -1, partitionOpt = None)
      }
      checkError(e, "DELTA_CONCURRENT_DELETE_DELETE.WITHOUT_HINT", "2D521",
        Map("operation" -> "TRANSACTION", "tableName" -> "t", "version" -> "-1",
          "docLink" -> generateDocsLink("/concurrency-control.html")))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentDeleteDeleteException] {
        throw org.apache.spark.sql.delta.DeltaErrors
          .concurrentDeleteDeleteException(None, "t", -1, partitionOpt = Some("p1"))
      }
      checkError(e, "DELTA_CONCURRENT_DELETE_DELETE.WITH_PARTITION_HINT", "2D521",
        Map("operation" -> "TRANSACTION", "tableName" -> "t", "version" -> "-1",
          "partitionValues" -> "p1",
          "docLink" -> generateDocsLink("/concurrency-control.html")))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentTransactionException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentTransactionException(None)
      }
      checkError(e, "DELTA_CONCURRENT_TRANSACTION", "2D521", Map.empty[String, String])
      assert(e.getMessage.contains("This error occurs when multiple streaming queries are using " +
        "the same checkpoint to write into this table. Did you run multiple instances of the " +
        "same streaming query at the same time?"))
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentWriteException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentWriteException(None)
      }
      checkError(e, "DELTA_CONCURRENT_WRITE", "2D521", Map.empty[String, String])
      assert(e.getMessage.contains("A concurrent transaction has written new data since the " +
        "current transaction read the table."))
    }
    {
      val e = intercept[io.delta.exceptions.ProtocolChangedException] {
        throw org.apache.spark.sql.delta.DeltaErrors.protocolChangedException(None)
      }
      checkError(e, "DELTA_PROTOCOL_CHANGED", "2D521", Map.empty[String, String])
      assert(e.getMessage.contains("The protocol version of the Delta table has been changed " +
        "by a concurrent update."))
    }
    {
      val e = intercept[io.delta.exceptions.MetadataChangedException] {
        throw org.apache.spark.sql.delta.DeltaErrors.metadataChangedException(None)
      }
      checkError(e, "DELTA_METADATA_CHANGED", "2D521", Map.empty[String, String])
      assert(e.getMessage.contains("The metadata of the Delta table has been changed by a " +
        "concurrent update."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0009",
          messageParameters = Array("prefixMsg - "))
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0009", None,
        Map("optionalPrefixMessage" -> "prefixMsg - "))
    }
    {
      val expr = "someExp".expr
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0010",
          messageParameters = Array("prefixMsg - ", expr.sql))
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0010", None,
        Map("optionalPrefixMessage" -> "prefixMsg - ", "expression" -> "'someExp'"))
    }
    {
      val exprs = Seq("1".expr, "2".expr)
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0012",
          messageParameters = Array(exprs.mkString(",")))
      }
      checkError(e, "_LEGACY_ERROR_TEMP_DELTA_0012", None,
        Map("expression" -> exprs.mkString(",")))
    }
    {
      val unsupportedDataType = IntegerType
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.identityColumnDataTypeNotSupported(unsupportedDataType)
      }
      checkError(e, "DELTA_IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE", "428H2",
        Map("dataType" -> "integer"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.identityColumnIllegalStep()
      }
      checkError(e, "DELTA_IDENTITY_COLUMNS_ILLEGAL_STEP", "42611", Map.empty[String, String])
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.identityColumnWithGenerationExpression()
      }
      checkError(e, "DELTA_IDENTITY_COLUMNS_WITH_GENERATED_EXPRESSION", "42613",
        Map.empty[String, String])
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unsupportedWritesWithMissingCoordinators("test")
      }
      checkError(e, "DELTA_UNSUPPORTED_WRITES_WITHOUT_COORDINATOR", "0AKDC",
        Map("coordinatorName" -> "test"))
    }
    {
      val exceptionWithContext =
        DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
      assert(exceptionWithContext.getMessage.contains("https") === true)

      val newSession = spark.newSession()
      setCustomContext(newSession, null)
      val exceptionWithoutContext =
        DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(newSession)
      assert(exceptionWithoutContext.getMessage.contains("https") === false)
    }
  }

  private def setCustomContext(session: SparkSession, context: SparkContext): Unit = {
    val scField = session.getClass.getDeclaredField("sparkContext")
    scField.setAccessible(true)
    try {
      scField.set(session, context)
    } finally {
      scField.setAccessible(false)
    }
  }
}

class DeltaErrorsSuite
  extends DeltaErrorsSuiteBase
