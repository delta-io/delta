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
import org.apache.spark.sql.delta.hooks.AutoCompactType
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, InvariantViolationException, SchemaMergingUtils, SchemaUtils, UnsupportedDataTypeInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path
import org.json4s.JString
import org.scalatest.GivenWhenThen

import org.apache.spark.SparkThrowable
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
    "blockStreamingReadsWithIncompatibleColumnMappingSchemaChanges" ->
      DeltaErrors.blockStreamingReadsWithIncompatibleColumnMappingSchemaChanges(
        spark,
        StructType.fromDDL("id int"),
        StructType.fromDDL("id2 int"),
        detectedDuringStreaming = true),
    "concurrentAppendException" ->
      DeltaErrors.concurrentAppendException(None, "p1"),
    "concurrentDeleteDeleteException" ->
      DeltaErrors.concurrentDeleteDeleteException(None, "p1"),
    "concurrentDeleteReadException" ->
      DeltaErrors.concurrentDeleteReadException(None, "p1"),
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

  def testUrls(): Unit = {
    errorMessagesToTest.foreach { case (errName, message) =>
      getUrlsFromMessage(message).foreach { url =>
        Given(s"*** Checking response for url: $url")
        var response = ""
        (1 to MAX_URL_ACCESS_RETRIES).foreach { attempt =>
          if (attempt > 1) Thread.sleep(1000)
          response = try {
            Process("curl -I " + url).!!
          } catch {
            case e: RuntimeException =>
              val sw = new StringWriter
              e.printStackTrace(new PrintWriter(sw))
              sw.toString
          }
          if (!checkIfValidResponse(url, response)) {
            fail(
              s"""
                 |A link to the URL: '$url' is broken in the error: $errName, accessing this URL
                 |does not result in a valid response, received the following response: $response
         """.stripMargin)
          }
        }
      }
    }
  }

  /**
   * New testcases should always use this method.
   */
  def checkErrorMessage(
      e: Exception with DeltaThrowable,
      errClassOpt: Option[String] = None,
      sqlStateOpt: Option[String] = None,
      errMsgOpt: Option[String] = None,
      startWith: Boolean = false): Unit = {
    val prefix = errClassOpt match {
      case Some(exist) =>
        assert(e.getErrorClass == exist)
        exist
      case _ => e.getErrorClass
    }
    sqlStateOpt match {
      case Some(sqlState) => assert(e.getSqlState == sqlState)
      case _ =>
    }
    (errMsgOpt, startWith) match {
      case (Some(errMsg), true) =>
        assert(e.getMessage.startsWith(s"[${prefix}] ${errMsg}"))
      case (Some(errMsg), false) =>
        assert(e.getMessage == s"[${prefix}] ${errMsg}")
      case _ =>
    }
  }

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
      checkErrorMessage(e, Some("DELTA_TABLE_ALREADY_CONTAINS_CDC_COLUMNS"), Some("42711"),
        Some(s"""Unable to enable Change Data Capture on the table. The table already contains
           |reserved columns [col1,col2] that will
           |be used internally as metadata for the table's Change Data Feed. To enable
           |Change Data Feed on the table rename/drop these columns.
           |""".stripMargin))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cdcColumnsInData(Seq("col1", "col2"))
      }
      checkErrorMessage(e, Some("RESERVED_CDC_COLUMNS_ON_WRITE"), Some("42939"),
        Some(s"""
           |The write contains reserved columns [col1,col2] that are used
           |internally as metadata for Change Data Feed. To write to the table either rename/drop
           |these columns or disable Change Data Feed on the table by setting
           |delta.enableChangeDataFeed to false.""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.multipleCDCBoundaryException("sample")
      }
      checkErrorMessage(e, Some("DELTA_MULTIPLE_CDC_BOUNDARY"), Some("42614"),
        Some("Multiple sample arguments provided for CDC read. Please provide " +
        "one of either sampleTimestamp or sampleVersion."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnCheckpointRename(new Path("path-1"), new Path("path-2"))
      }
      checkErrorMessage(e, None, None,
        Some("Cannot rename path-1 to path-2"))
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaErrors.notNullColumnMissingException(NotNull(Seq("c0", "c1")))
      }
      checkErrorMessage(e, Some("DELTA_MISSING_NOT_NULL_COLUMN_VALUE"), Some("23502"),
        Some("Column c0.c1, which has a NOT NULL constraint, is missing " +
        "from the data being written into the table."))
    }
    {
      val parent = "parent"
      val nested = IntegerType
      val nestType = "nestType"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedNotNullConstraint(parent, nested, nestType)
      }
      checkErrorMessage(e, Some("DELTA_NESTED_NOT_NULL_CONSTRAINT"), Some("0AKDC"),
        Some(s"The $nestType type of the field $parent contains a NOT NULL " +
        s"constraint. Delta does not support NOT NULL constraints nested within arrays or maps. " +
        s"To suppress this error and silently ignore the specified constraints, set " +
        s"${DeltaSQLConf.ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS.key} = true.\n" +
        s"Parsed $nestType type:\n${nested.prettyJson}"))
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(Constraints.NotNull(Seq("col1")))
      }
      checkErrorMessage(e, Some("DELTA_NOT_NULL_CONSTRAINT_VIOLATED"), Some("23502"),
        Some("NOT NULL constraint violated for column: col1.\n"))
    }
    {
      val expr = UnresolvedAttribute("col")
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(
          Constraints.Check(CharVarcharConstraint.INVARIANT_NAME,
            LessThanOrEqual(Length(expr), Literal(5))),
          Map("col" -> "Hello World"))
      }
      checkErrorMessage(e, Some("DELTA_EXCEED_CHAR_VARCHAR_LIMIT"), Some("22001"),
        Some("Value \"Hello World\" exceeds char/varchar type length limitation. " +
          "Failed check: (length(col) <= 5)."))
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(
          Constraints.Check("__dummy__",
            CatalystSqlParser.parseExpression("id < 0")),
          Map("a" -> "b"))
      }
      checkErrorMessage(e, Some("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES"), Some("23001"),
        Some("CHECK constraint __dummy__ (id < 0) violated " +
        "by row with values:\n - a : b"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(Some("path")))
      }
      checkErrorMessage(e, None, None,
        Some("`path` is not a Delta table."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException(
          operation = "delete",
          DeltaTableIdentifier(Some("path")))
      }
      checkErrorMessage(e, None, None,
        Some("`path` is not a Delta table. delete is only supported for Delta tables."))
    }
    {
      val table = TableIdentifier("table")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotWriteIntoView(table)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_WRITE_INTO_VIEW"), Some("0A000"),
        Some(s"$table is a view. Writes to a view are not supported."))
    }
    {
      val sourceType = IntegerType
      val targetType = DateType
      val columnName = "column_name"
      val e = intercept[DeltaArithmeticException] {
        throw DeltaErrors.castingCauseOverflowErrorInTableWrite(sourceType, targetType, columnName)
      }
      checkErrorMessage(e, Some("DELTA_CAST_OVERFLOW_IN_TABLE_WRITE"), Some("22003"), None)
      assert(e.getMessageParameters.get("sourceType") == toSQLType(sourceType))
      assert(e.getMessageParameters.get("targetType") == toSQLType(targetType))
      assert(e.getMessageParameters.get("columnName") == toSQLId(columnName))
      assert(e.getMessageParameters.get("storeAssignmentPolicyFlag")
        == SQLConf.STORE_ASSIGNMENT_POLICY.key)
      assert(e.getMessageParameters.get("updateAndMergeCastingFollowsAnsiEnabledFlag")
        ==  DeltaSQLConf.UPDATE_AND_MERGE_CASTING_FOLLOWS_ANSI_ENABLED_FLAG.key)
      assert(e.getMessageParameters.get("ansiEnabledFlag") == SQLConf.ANSI_ENABLED.key)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidColumnName(name = "col-1")
      }
      checkErrorMessage(e, None, None,
        Some("Attribute name \"col-1\" contains invalid character(s) " +
        "among \" ,;{}()\\\\n\\\\t=\". Please use alias to rename it."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSetColumnNotFoundException(col = "c0", colList = Seq("c1", "c2"))
      }
      checkErrorMessage(e, None, None,
        Some("SET column `c0` not found given columns: [`c1`, `c2`]."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSetConflictException(cols = Seq("c1", "c2"))
      }
      checkErrorMessage(e, None, None,
        Some("There is a conflict from these SET columns: [`c1`, `c2`]."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnNestedColumnNotSupportedException("c0")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_NESTED_COLUMN_IN_BLOOM_FILTER"), Some("0AKDC"),
        Some("Creating a bloom filer index on a nested " +
        "column is currently unsupported: c0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnPartitionColumnNotSupportedException("c0")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_PARTITION_COLUMN_IN_BLOOM_FILTER"),
        Some("0AKDC"),
        Some("Creating a bloom filter index on a partitioning column " +
        "is unsupported: c0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterDropOnNonIndexedColumnException("c0")
      }
      checkErrorMessage(e, None, None,
        Some("Cannot drop bloom filter index on a non indexed column: c0"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotRenamePath("a", "b")
      }
      checkErrorMessage(e, None, None,
        Some("Cannot rename a to b"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cannotSpecifyBothFileListAndPatternString()
      }
      checkErrorMessage(e, None, None,
        Some("Cannot specify both file list and pattern string."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateArrayField("t", "f")
      }
      checkErrorMessage(e, None, None,
        Some("Cannot update t field f type: update the element by updating f.element"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateMapField("t", "f")
      }
      checkErrorMessage(e, None, None,
        Some("Cannot update t field f type: update a map by updating f.key or f.value"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateStructField("t", "f")
      }
      checkErrorMessage(e, None, None,
        Some("Cannot update t field f type: update struct by adding, deleting, " +
        "or updating its fields"))
    }
    {
      val tableName = "table"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateOtherField(tableName, IntegerType)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_UPDATE_OTHER_FIELD"), Some("429BQ"),
        Some(s"Cannot update $tableName field of type ${IntegerType}"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.duplicateColumnsOnUpdateTable(originalException = new Exception("123"))
      }
      checkErrorMessage(e, None, None,
        Some("123\nPlease remove duplicate columns before you update your table."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.maxCommitRetriesExceededException(0, 1, 2, 3, 4)
      }
      checkErrorMessage(e, None, None,
        Some(s"""This commit has failed as it has been tried 0 times but did not succeed.
           |This can be caused by the Delta table being committed continuously by many concurrent
           |commits.
           |
           |Commit started at version: 2
           |Commit failed at version: 1
           |Number of actions attempted to commit: 3
           |Total time spent attempting this commit: 4 ms""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingColumnsInInsertInto("c")
      }
      checkErrorMessage(e, None, None,
        Some("Column c is not specified in INSERT"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidAutoCompactType("invalid")
      }
      val allowed = AutoCompactType.ALLOWED_VALUES.mkString("(", ",", ")")
      checkErrorMessage(e, None, None,
        Some(s"Invalid auto-compact type: invalid. Allowed values are: $allowed."))
    }
    {
      val table = DeltaTableIdentifier(Some("path"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentDeltaTable(table)
      }
      checkErrorMessage(e, None, None,
        Some(s"Delta table $table doesn't exist."))
    }
    checkError(
      intercept[DeltaIllegalStateException] {
        throw DeltaErrors.differentDeltaTableReadByStreamingSource(
          newTableId = "027fb01c-94aa-4cab-87cb-5aab6aec6d17",
          oldTableId = "2edf2c02-bb63-44e9-a84c-517fad0db296")
      },
      "DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE",
      parameters = Map(
        "oldTableId" -> "2edf2c02-bb63-44e9-a84c-517fad0db296",
        "newTableId" -> "027fb01c-94aa-4cab-87cb-5aab6aec6d17")
    )

    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentColumnInSchema("c", "s")
      }
      checkErrorMessage(e, None, None,
        Some("Couldn't find column c in:\ns"))
    }
    {
      val ident = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaNoSuchTableException] {
        throw DeltaErrors.noRelationTable(ident)
      }
      checkErrorMessage(e, Some("DELTA_NO_RELATION_TABLE"), Some("42P01"),
        Some(s"Table ${ident.quoted} not found"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTable("t")
      }
      checkErrorMessage(e, None, None,
        Some("t is not a Delta table. Please drop this table first if you would " +
        "like to recreate it with Delta Lake."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.notFoundFileToBeRewritten("f", Seq("a", "b"))
      }
      checkErrorMessage(e, None, None,
        Some("File (f) to be rewritten not found among candidate files:\na\nb"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsetNonExistentProperty("k", "t")
      }
      checkErrorMessage(e, None, None,
        Some("Attempted to unset non-existent property 'k' in table t"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsReferToWrongColumns(
          new AnalysisException(
            errorClass = "INTERNAL_ERROR",
            messageParameters = Map("message" -> "internal test error msg"))
        )
      }
      checkErrorMessage(
        e,
        Some("DELTA_INVALID_GENERATED_COLUMN_REFERENCES"),
        Some("42621"),
        Some("A generated column cannot use a non-existent column or " +
        "another generated column"))
    }
    {
      val current = StructField("c0", IntegerType)
      val update = StructField("c0", StringType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUpdateColumnType(current, update)
      }
      checkErrorMessage(e, Some("DELTA_GENERATED_COLUMN_UPDATE_TYPE_MISMATCH"), Some("42K09"),
        Some(
        s"Column ${current.name} is a generated column or a column used by a generated column. " +
        s"The data type is ${current.dataType.sql} and cannot be converted to data type " +
        s"${update.dataType.sql}"))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.changeColumnMappingModeNotSupported(oldMode = "old", newMode = "new")
      }
      checkErrorMessage(e, None, None,
        Some("Changing column mapping mode from 'old' to 'new' is not supported."))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.generateManifestWithColumnMappingNotSupported
      }
      checkErrorMessage(e, None, None,
        Some("Manifest generation is not supported for tables that leverage " +
        "column mapping, as external readers cannot read these Delta tables. See Delta " +
        "documentation for more details."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertToDeltaNoPartitionFound("testTable")
      }
      checkErrorMessage(e, Some("DELTA_CONVERSION_NO_PARTITION_FOUND"), Some("42KD6"),
        Some("Found no partition information in the catalog for table testTable." +
        " Have you run \"MSCK REPAIR TABLE\" on your table to discover partitions?"))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.convertToDeltaWithColumnMappingNotSupported(IdMapping)
      }
      checkErrorMessage(e, None, None,
        Some("The configuration " +
        "'spark.databricks.delta.properties.defaults.columnMapping.mode' cannot be set to `id` " +
        "when using CONVERT TO DELTA."))
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.schemaChangeDuringMappingModeChangeNotSupported(
          StructType(Seq(StructField("c0", IntegerType))),
          StructType(Seq(StructField("c1", IntegerType))))
      }
      checkErrorMessage(e, None, None,
        Some("""
           |Schema change is detected:
           |
           |old schema:
           |root
           | |-- c0: integer (nullable = true)
           |
           |
           |new schema:
           |root
           | |-- c1: integer (nullable = true)
           |
           |
           |Schema changes are not allowed during the change of column mapping mode.
           |
           |""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notEnoughColumnsInInsert(
          "table", 1, 2, Some("nestedField"))
      }
      checkErrorMessage(e, None, None,
        Some("Cannot write to 'table', not enough nested fields in nestedField; " +
        s"target table has 2 column(s) but the inserted data has " +
        s"1 column(s)"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotInsertIntoColumn(
          "tableName", "source", "target", "targetType")
      }
      checkErrorMessage(e, None, None,
        Some("Struct column source cannot be inserted into a " +
        "targetType field target in tableName."))
    }
    {
      val colName = "col1"
      val schema = Seq(UnresolvedAttribute("col2"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.partitionColumnNotFoundException(colName, schema)
      }
      checkErrorMessage(e, Some("DELTA_PARTITION_COLUMN_NOT_FOUND"), Some("42703"),
        Some(s"Partition column ${DeltaErrors.formatColumn(colName)} not found in schema " +
        s"[${schema.map(_.name).mkString(", ")}]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.partitionPathParseException("fragment")
      }
      checkErrorMessage(e, None, None,
        Some("A partition path fragment should be the form like " +
        "`part1=foo/part2=bar`. The partition path: fragment"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereMismatchException("replaceWhere",
          new InvariantViolationException("Invariant violated."))
      }
      checkErrorMessage(e, Some("DELTA_REPLACE_WHERE_MISMATCH"), Some("44000"),
        Some("""Written data does not conform to partial table overwrite condition or constraint 'replaceWhere'.
        |Invariant violated.""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereMismatchException("replaceWhere", "badPartitions")
      }
      checkErrorMessage(e, Some("DELTA_REPLACE_WHERE_MISMATCH"), Some("44000"),
        Some("""Written data does not conform to partial table overwrite condition or constraint 'replaceWhere'.
        |Invalid data would be written to partitions badPartitions.""".stripMargin))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.actionNotFoundException("action", 0)
      }
      val msg = s"""The action of your Delta table could not be recovered while Reconstructing
        |version: 0. Did you manually delete files in the _delta_log directory?""".stripMargin
      checkErrorMessage(e, None, None,
        Some(msg))
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
        assert(e.getErrorClass == "DELTA_SCHEMA_CHANGED")
        assert(e.getSqlState == "KD007")
        // Use '#' as stripMargin interpolator to get around formatSchema having '|' in it
        var msg =
          s"""Detected schema change:
             #streaming source schema: ${DeltaErrors.formatSchema(oldSchema)}
             #
             #data file schema: ${DeltaErrors.formatSchema(newSchema)}
             #
             #Please try restarting the query. If this issue repeats across query restarts without
             #making progress, you have made an incompatible schema change and need to start your
             #query from scratch using a new checkpoint directory.
             #""".stripMargin('#')
        // [StreamingRetryableException] is a SparkThrowable
        // but uses DeltaThrowableHelper to format its message.
        // It does not contain a parameter map, so we cannot use [checkError]
        // It is not a DeltaThrowable so we cannot use [checkErrorMessage]
        // Directly compare the error message here.
        assert(e.getMessage == s"[DELTA_SCHEMA_CHANGED] ${msg}")

        // Check the error message with version information
        e = intercept[Exception with SparkThrowable] {
          throw DeltaErrors.schemaChangedException(oldSchema, newSchema, retryable, Some(10), false)
        }
        assert(expectedClass.isAssignableFrom(e.getClass))
        assert(e.getErrorClass == "DELTA_SCHEMA_CHANGED_WITH_VERSION")
        assert(e.getSqlState == "KD007")
        // Use '#' as stripMargin interpolator to get around formatSchema having '|' in it
        msg =
          s"""Detected schema change in version 10:
             #streaming source schema: ${DeltaErrors.formatSchema(oldSchema)}
             #
             #data file schema: ${DeltaErrors.formatSchema(newSchema)}
             #
             #Please try restarting the query. If this issue repeats across query restarts without
             #making progress, you have made an incompatible schema change and need to start your
             #query from scratch using a new checkpoint directory.
             #""".stripMargin('#')
        assert(e.getMessage == s"[DELTA_SCHEMA_CHANGED_WITH_VERSION] $msg")

        // Check the error message with startingVersion/Timestamp error message
        e = intercept[Exception with SparkThrowable] {
          throw DeltaErrors.schemaChangedException(oldSchema, newSchema, retryable, Some(10), true)
        }
        assert(expectedClass.isAssignableFrom(e.getClass))
        assert(e.getErrorClass == "DELTA_SCHEMA_CHANGED_WITH_STARTING_OPTIONS")
        assert(e.getSqlState == "KD007")
        // Use '#' as stripMargin interpolator to get around formatSchema having '|' in it
        msg =
          s"""Detected schema change in version 10:
             #streaming source schema: ${DeltaErrors.formatSchema(oldSchema)}
             #
             #data file schema: ${DeltaErrors.formatSchema(newSchema)}
             #
             #Please try restarting the query. If this issue repeats across query restarts without
             #making progress, you have made an incompatible schema change and need to start your
             #query from scratch using a new checkpoint directory. If the issue persists after
             #changing to a new checkpoint directory, you may need to change the existing
             #'startingVersion' or 'startingTimestamp' option to start from a version newer than
             #10 with a new checkpoint directory.
             #""".stripMargin('#')
        assert(e.getMessage == s"[DELTA_SCHEMA_CHANGED_WITH_STARTING_OPTIONS] $msg")
      }
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreVersionNotExistException(0, 0, 0)
      }
      checkErrorMessage(e, None, None,
        Some("Cannot restore table to version 0. " +
        "Available versions: [0, 0]."))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedGenerateModeException("modeName")
      }
      import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
      val supportedModes = DeltaGenerateCommand.modeNameToGenerationFunc.keys.toSeq.mkString(", ")
      checkErrorMessage(e, None, None,
        Some(s"Specified mode 'modeName' is not supported. " +
        s"Supported modes are: $supportedModes"))
    }
    {
      import org.apache.spark.sql.delta.DeltaOptions.EXCLUDE_REGEX_OPTION
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.excludeRegexOptionException(EXCLUDE_REGEX_OPTION)
      }
      checkErrorMessage(e, None, None,
        Some(s"Please recheck your syntax for '$EXCLUDE_REGEX_OPTION'"))
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileNotFoundException("path")
      }
      checkErrorMessage(e, None, None,
        Some(s"File path path"))
    }
    {
      val ex = new FileNotFoundException("reason")
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(ex)
      }
      checkErrorMessage(e, Some("DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE"), Some("42K03"), None)
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidIsolationLevelException("level")
      }
      checkErrorMessage(e, None, None,
        Some("invalid isolation level 'level'"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnNameNotFoundException("a", "b")
      }
      checkErrorMessage(e, None, None,
        Some("Unable to find the column `a` given [b]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnAtIndexLessThanZeroException("1", "a")
      }
      checkErrorMessage(e, None, None,
        Some("Index 1 to add column a is lower than 0"))
    }
    {
      val pos = -1
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropColumnAtIndexLessThanZeroException(-1)
      }
      checkErrorMessage(e, Some("DELTA_DROP_COLUMN_AT_INDEX_LESS_THAN_ZERO"), Some("42KD8"),
        Some(s"Index $pos to drop column is lower than 0"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.incorrectArrayAccess()
      }
      checkErrorMessage(e, None, None,
        Some(s"""Incorrectly accessing an ArrayType. Use arrayname.element.elementname position to
            |add to an array.""".stripMargin))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.partitionColumnCastFailed("Value", "Type", "Name")
      }
      checkErrorMessage(e, None, None,
        Some("Failed to cast value `Value` to `Type` for partition column `Name`"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidTimestampFormat("ts", "format")
      }
      checkErrorMessage(e, None, None,
        Some("The provided timestamp ts does not match the expected syntax format."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotChangeDataType("example message")
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_CHANGE_DATA_TYPE"), Some("429BQ"),
        Some("Cannot change data type: example message"))
    }
    {
      val table = CatalogTable(TableIdentifier("my table"), null, null, null)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableAlreadyExists(table)
      }
      checkErrorMessage(e, Some("DELTA_TABLE_ALREADY_EXISTS"), Some("42P07"),
        Some("Table `my table` already exists."))
    }
    {
      val storage1 =
        CatalogStorageFormat(Option(new URI("loc1")), null, null, null, false, Map.empty)
      val storage2 =
        CatalogStorageFormat(Option(new URI("loc2")), null, null, null, false, Map.empty)
      val table = CatalogTable(TableIdentifier("table"), null, storage1, null)
      val existingTable = CatalogTable(TableIdentifier("existing table"), null, storage2, null)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableLocationMismatch(table, existingTable)
      }
      checkErrorMessage(e, Some("DELTA_TABLE_LOCATION_MISMATCH"), Some("42613"),
        Some(s"The location of the existing table ${table.identifier.quotedString} is " +
        s"`${existingTable.location}`. It doesn't match the specified location " +
        s"`${table.location}`."))
    }
    {
      val ident = "ident"
      val e = intercept[DeltaNoSuchTableException] {
        throw DeltaErrors.nonSinglePartNamespaceForCatalog(ident)
      }
      checkErrorMessage(e, Some("DELTA_NON_SINGLE_PART_NAMESPACE_FOR_CATALOG"), Some("42K05"),
        Some(s"Delta catalog requires a single-part namespace, but $ident is multi-part."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.targetTableFinalSchemaEmptyException()
      }
      checkErrorMessage(e, Some("DELTA_TARGET_TABLE_FINAL_SCHEMA_EMPTY"), Some("428GU"),
        Some("Target table final schema is empty."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonDeterministicNotSupportedException("op", Uuid())
      }
      checkErrorMessage(e, Some("DELTA_NON_DETERMINISTIC_FUNCTION_NOT_SUPPORTED"), Some("0AKDC"),
        Some("Non-deterministic functions " +
        "are not supported in the op (condition = uuid())."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableNotSupportedException("someOp")
      }
      checkErrorMessage(e, Some("DELTA_TABLE_NOT_SUPPORTED_IN_OP"), Some("42809"),
        Some("Table is not supported in someOp. Please use a path instead."))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.postCommitHookFailedException(new PostCommitHook() {
          override val name: String = "DummyPostCommitHook"
          override def run(
            spark: SparkSession, txn: OptimisticTransactionImpl, committedVersion: Long,
            postCommitSnapshot: Snapshot, committedActions: Seq[Action]): Unit = {}
        }, 0, "msg", null)
      }
      checkErrorMessage(e, Some("DELTA_POST_COMMIT_HOOK_FAILED"), Some("2DKD0"),
        Some("Committing to the Delta table version 0 " +
        "succeeded but error while executing post-commit hook DummyPostCommitHook: msg"))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.postCommitHookFailedException(new PostCommitHook() {
          override val name: String = "DummyPostCommitHook"
          override def run(
            spark: SparkSession, txn: OptimisticTransactionImpl, committedVersion: Long,
            postCommitSnapshot: Snapshot, committedActions: Seq[Action]): Unit = {}
        }, 0, null, null)
      }
      checkErrorMessage(e, Some("DELTA_POST_COMMIT_HOOK_FAILED"), Some("2DKD0"),
        Some("Committing to the Delta table version 0 " +
        "succeeded but error while executing post-commit hook DummyPostCommitHook"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerThanStruct(1, StructField("col1", IntegerType), 1)
      }
      checkErrorMessage(e, Some("DELTA_INDEX_LARGER_THAN_STRUCT"), Some("42KD8"),
        Some("Index 1 to add column StructField(col1,IntegerType,true) is larger " +
        "than struct length: 1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerOrEqualThanStruct(1, 1)
      }
      checkErrorMessage(e, Some("DELTA_INDEX_LARGER_OR_EQUAL_THAN_STRUCT"), Some("42KD8"),
        Some("Index 1 to drop column equals to or is larger " +
        "than struct length: 1"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidV1TableCall("v1Table", "DeltaTableV2")
      }
      checkErrorMessage(e, Some("DELTA_INVALID_V1_TABLE_CALL"), Some("XXKDS"),
        Some("v1Table call is not expected with path based DeltaTableV2"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotGenerateUpdateExpressions()
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_GENERATE_UPDATE_EXPRESSIONS"), Some("XXKDS"),
        Some("Calling without generated columns should always return a update " +
        "expression for each column"))
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
      checkErrorMessage(e, Some("DELTA_CANNOT_DESCRIBE_VIEW_HISTORY"), Some("42809"),
        Some("Cannot describe the history of a view."))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedInvariant()
      }
      checkErrorMessage(e, Some("DELTA_UNRECOGNIZED_INVARIANT"), Some("56038"),
        Some("Unrecognized invariant. Please upgrade your Spark version."))
    }
    {
      val baseSchema = StructType(Seq(StructField("c0", StringType)))
      val field = StructField("id", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotResolveColumn(field.name, baseSchema)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_RESOLVE_COLUMN"), Some("42703"),
        Some("""Can't resolve column id in root
         | |-- c0: string (nullable = true)
         |""".stripMargin
        ))
    }
    {
      checkError(
        intercept[DeltaAnalysisException] {
          throw DeltaErrors.alterTableChangeColumnException(
            fieldPath = "a.b.c",
            oldField = StructField("c", IntegerType),
            newField = StructField("c", LongType))
        },
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
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_ALTER_TABLE_REPLACE_COL_OP"), Some("0AKDC"),
        Some("""Unsupported ALTER TABLE REPLACE COLUMNS operation. Reason: incompatible
          |
          |Failed to change schema from:
          |root
          | |-- c0: integer (nullable = true)
          |
          |to:
          |root
          | |-- c0: string (nullable = true)
          |""".stripMargin
      ))
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
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_TYPE_CHANGE_IN_SCHEMA"), Some("0AKDC"),
        Some("Unable to operate on this table because an unsupported type change was applied. " +
          "Field s.a was changed from INT to STRING."
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val classConf = Seq(("classKey", "classVal"))
        val schemeConf = Seq(("schemeKey", "schemeVal"))
        throw DeltaErrors.logStoreConfConflicts(classConf, schemeConf)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_LOGSTORE_CONF"), Some("F0000"),
        Some("(`classKey`) and (`schemeKey`) cannot " +
        "be set at the same time. Please set only one group of them."))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        val schemeConf = Seq(("key", "val"))
        throw DeltaErrors.inconsistentLogStoreConfs(
          Seq(("delta.key", "value1"), ("spark.delta.key", "value2")))
      }
      checkErrorMessage(e, Some("DELTA_INCONSISTENT_LOGSTORE_CONFS"), Some("F0000"),
        Some("(delta.key = value1, spark.delta.key = value2) cannot be set to " +
        "different values. Please only set one of them, or set them to the same value."))
    }
    {
      val e = intercept[DeltaSparkException] {
        throw DeltaErrors.failedMergeSchemaFile("file", "schema", null)
      }
      checkErrorMessage(e, Some("DELTA_FAILED_MERGE_SCHEMA_FILE"), None,
        Some("Failed to merge schema of file file:\nschema"))
    }
    {
      val id = TableIdentifier("id")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("op", id)
      }
      checkErrorMessage(e, Some("DELTA_OPERATION_NOT_ALLOWED_DETAIL"), None,
        Some(s"Operation not allowed: `op` is not supported " +
        s"for Delta tables: $id"))
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileOrDirectoryNotFoundException("path")
      }
      checkErrorMessage(e, Some("DELTA_FILE_OR_DIR_NOT_FOUND"), None,
        Some("No such file or directory: path"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidPartitionColumn("col", "tbl")
      }
      checkErrorMessage(e, Some("DELTA_INVALID_PARTITION_COLUMN"), None,
        Some("col is not a valid partition column in table tbl."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotFindSourceVersionException("json")
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_FIND_VERSION"), None,
        Some("Cannot find 'sourceVersion' in json"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unknownConfigurationKeyException("confKey")
      }
      var msg = "Unknown configuration was specified: confKey\nTo disable this check, set " +
        "spark.databricks.delta.allowArbitraryProperties.enabled=true in the Spark session " +
        "configuration."
      checkErrorMessage(e, Some("DELTA_UNKNOWN_CONFIGURATION"), None,
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathNotExistsException("path")
      }
      checkErrorMessage(e, Some("DELTA_PATH_DOES_NOT_EXIST"), None,
        Some("path doesn't exist"), startWith = true)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failRelativizePath("path")
      }
      var msg =
        """Failed to relativize the path (path). This can happen when absolute paths make
          |it into the transaction log, which start with the scheme
          |s3://, wasbs:// or adls://.
          |
          |If this table is NOT USED IN PRODUCTION, you can set the SQL configuration
          |spark.databricks.delta.vacuum.relativize.ignoreError to true.
          |Using this SQL configuration could lead to accidental data loss, therefore we do
          |not recommend the use of this flag unless this is for testing purposes.""".stripMargin
      checkErrorMessage(e, Some("DELTA_FAIL_RELATIVIZE_PATH"), Some("XXKDS"),
        Some(msg))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.illegalFilesFound("file")
      }
      checkErrorMessage(e, Some("DELTA_ILLEGAL_FILE_FOUND"), None,
        Some("Illegal files found in a dataChange = false transaction. Files: file"))
    }
    {
      val name = "name"
      val input = "input"
      val explain = "explain"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.illegalDeltaOptionException(name, input, explain)
      }
      checkErrorMessage(e, Some("DELTA_ILLEGAL_OPTION"), Some("42616"),
        Some(s"Invalid value '$input' for option '$name', $explain"))
    }
    {
      val version = "version"
      val timestamp = "timestamp"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.startingVersionAndTimestampBothSetException(version, timestamp)
      }
      checkErrorMessage(e, Some("DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET"), Some("42613"),
        Some(s"Please either provide '$version' or '$timestamp'"))
    }
    {
      val path = new Path("parent", "child")
      val specifiedSchema = StructType(Seq(StructField("a", IntegerType)))
      val existingSchema = StructType(Seq(StructField("b", StringType)))
      val diffs = Seq("a", "b")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentSchemaException(
          path, specifiedSchema, existingSchema, diffs)
      }
      checkErrorMessage(e, Some("DELTA_CREATE_TABLE_SCHEME_MISMATCH"), None, None)
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noHistoryFound(path)
      }
      checkErrorMessage(e, Some("DELTA_NO_COMMITS_FOUND"), Some("KD006"),
        Some(s"No commits found at $path"))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noRecreatableHistoryFound(path)
      }
      checkErrorMessage(e, Some("DELTA_NO_RECREATABLE_HISTORY_FOUND"), Some("KD006"),
        Some(s"No recreatable commits found at $path"))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.castPartitionValueException("partitionValue", StringType)
      }
      checkErrorMessage(e, Some("DELTA_FAILED_CAST_PARTITION_VALUE"), None,
        Some(s"Failed to cast partition value `partitionValue` to $StringType"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkSessionNotSetException()
      }
      checkErrorMessage(e, Some("DELTA_SPARK_SESSION_NOT_SET"), None,
        Some("Active SparkSession not set."))
    }
    {
      val id = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotReplaceMissingTableException(id)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_REPLACE_MISSING_TABLE"), None,
        Some(s"Table $id cannot be replaced as it does not exist. " +
        s"Use CREATE OR REPLACE TABLE to create the table."))
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.cannotCreateLogPathException("logPath")
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_CREATE_LOG_PATH"), None,
        Some("Cannot create logPath"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.protocolPropNotIntException("key", "value")
      }
      checkErrorMessage(e, Some("DELTA_PROTOCOL_PROPERTY_NOT_INT"), None,
        Some("Protocol property key needs to be an integer. Found value"))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createExternalTableWithoutLogException(path, "tableName", spark)
      }
      val msg = s"""
        |You are trying to create an external table tableName
        |from `$path` using Delta, but there is no transaction log present at
        |`$path/_delta_log`. Check the upstream job to make sure that it is writing using
        |format("delta") and that the path is the root of the table.""".stripMargin
      checkErrorMessage(e, Some("DELTA_CREATE_EXTERNAL_TABLE_WITHOUT_TXN_LOG"), None, Some(msg), true)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.ambiguousPathsInCreateTableException("loc1", "loc2")
      }
      checkErrorMessage(e, Some("DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE"), Some("42613"),
        Some(s"""CREATE TABLE contains two different locations: loc1 and loc2.
        |You can remove the LOCATION clause from the CREATE TABLE statement, or set
        |${DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS.key} to true to skip this check.
        |""".stripMargin))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.illegalUsageException("overwriteSchema", "replacing")
      }
      checkErrorMessage(e, Some("DELTA_ILLEGAL_USAGE"), Some("42601"),
        Some("The usage of overwriteSchema is not allowed when replacing a Delta table."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.expressionsNotFoundInGeneratedColumn("col1")
      }
      checkErrorMessage(e, Some("DELTA_EXPRESSIONS_NOT_FOUND_IN_GENERATED_COLUMN"), Some("XXKDS"),
        Some("Cannot find the expressions in the generated column col1"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.activeSparkSessionNotFound()
      }
      checkErrorMessage(e, Some("DELTA_ACTIVE_SPARK_SESSION_NOT_FOUND"), Some("08003"),
        Some("Could not find active SparkSession"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationOnTempViewWithGenerateColsNotSupported("UPDATE")
      }
      checkErrorMessage(e, Some("DELTA_OPERATION_ON_TEMP_VIEW_WITH_GENERATED_COLS_NOT_SUPPORTED"), Some("0A000"),
        Some("UPDATE command on a temp view referring to a Delta table that " +
        "contains generated columns is not supported. Please run the UPDATE command on the Delta " +
        "table directly"))
    }
    {
      val property = "prop"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.cannotModifyTableProperty(property)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_MODIFY_TABLE_PROPERTY"), Some("42939"),
        Some(s"The Delta table configuration $property cannot be specified by the user"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingProviderForConvertException("parquet_path")
      }
      checkErrorMessage(e, Some("DELTA_MISSING_PROVIDER_FOR_CONVERT"), Some("0AKDC"),
        Some("CONVERT TO DELTA only supports parquet tables. Please rewrite your " +
        "target as parquet.`parquet_path` if it's a parquet directory."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.iteratorAlreadyClosed()
      }
      checkErrorMessage(e, Some("DELTA_ITERATOR_ALREADY_CLOSED"), Some("XXKDS"),
        Some("Iterator is closed"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.activeTransactionAlreadySet()
      }
      checkErrorMessage(e, Some("DELTA_ACTIVE_TRANSACTION_ALREADY_SET"), Some("0B000"),
        Some("Cannot set a new txn as active when one is already active"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterMultipleConfForSingleColumnException("col1")
      }
      checkErrorMessage(e, Some("DELTA_MULTIPLE_CONF_FOR_SINGLE_COLUMN_IN_BLOOM_FILTER"), Some("42614"),
        Some("Multiple bloom filter index configurations passed to " +
        "command for column: col1"))
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null)
      }
      val docsLink = DeltaErrors.generateDocsLink(
        sparkConf, "/delta-storage.html", skipValidation = true)
      checkErrorMessage(e, Some("DELTA_INCORRECT_LOG_STORE_IMPLEMENTATION"), Some("0AKDC"),
        Some(s"""The error typically occurs when the default LogStore implementation, that
           |is, HDFSLogStore, is used to write into a Delta table on a non-HDFS storage system.
           |In order to get the transactional ACID guarantees on table updates, you have to use the
           |correct implementation of LogStore that is appropriate for your storage system.
           |See $docsLink for details.
           |""".stripMargin))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidSourceVersion("xyz")
      }
      checkErrorMessage(e, Some("DELTA_INVALID_SOURCE_VERSION"), Some("XXKDS"),
        Some("sourceVersion(xyz) is invalid"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidSourceOffsetFormat()
      }
      checkErrorMessage(e, Some("DELTA_INVALID_SOURCE_OFFSET_FORMAT"), Some("XXKDS"),
        Some("The stored source offset format is invalid"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidCommittedVersion(1L, 2L)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_COMMITTED_VERSION"), Some("XXKDS"),
        Some("The committed version is 1 but the current version is 2."
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnReference("col1", Seq("col2", "col3"))
      }
      checkErrorMessage(e, Some("DELTA_NON_PARTITION_COLUMN_REFERENCE"), Some("42P10"),
        Some("Predicate references non-partition column 'col1'. Only the " +
        "partition columns may be referenced: [col2, col3]"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val attr = UnresolvedAttribute("col1")
        val attrs = Seq(UnresolvedAttribute("col2"), UnresolvedAttribute("col3"))
        throw DeltaErrors.missingColumn(attr, attrs)
      }
      checkErrorMessage(e, Some("DELTA_MISSING_COLUMN"), Some("42703"),
        Some("Cannot find col1 in table columns: col2, col3"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val schema = StructType(Seq(StructField("c0", IntegerType)))
        throw DeltaErrors.missingPartitionColumn("c1", schema.catalogString)
      }
      checkErrorMessage(e, Some("DELTA_MISSING_PARTITION_COLUMN"), Some("42KD6"),
        Some("Partition column `c1` not found in schema struct<c0:int>"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.aggsNotSupportedException("op", SparkVersion())
      }
      checkErrorMessage(e, Some("DELTA_AGGREGATION_NOT_SUPPORTED"), Some("42903"),
        Some("Aggregate functions are not supported in the op " +
        "(condition = version()).."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotChangeProvider()
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_CHANGE_PROVIDER"), Some("42939"),
        Some("'provider' is a reserved table property, and cannot be altered."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.noNewAttributeId(AttributeReference("attr1", IntegerType)())
      }
      checkErrorMessage(e, Some("DELTA_NO_NEW_ATTRIBUTE_ID"), Some("XXKDS"),
        Some("Could not find a new attribute ID for column attr1. This " +
        "should have been checked earlier."))
    }
    {
      val e = intercept[ProtocolDowngradeException] {
        val p1 = Protocol(1, 1)
        val p2 = Protocol(2, 2)
        throw new ProtocolDowngradeException(p1, p2)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_PROTOCOL_DOWNGRADE"), Some("KD004"),
        Some("Protocol version cannot be downgraded from (1,1) to (2,2)"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsExprTypeMismatch("col1", IntegerType, StringType)
      }
      checkErrorMessage(e, Some("DELTA_GENERATED_COLUMNS_EXPR_TYPE_MISMATCH"), Some("42K09"),
        Some("The expression type of the generated column col1 is STRING, " +
        "but the column type is INT"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.nonGeneratedColumnMissingUpdateExpression("attr1")
      }
      val msg = "attr1 is not a generated column but is missing its update expression"
      checkErrorMessage(e, Some("DELTA_NON_GENERATED_COLUMN_MISSING_UPDATE_EXPR"), Some("XXKDS"),
        Some(msg))
    }
    {
      checkError(
        intercept[DeltaAnalysisException] {
          throw DeltaErrors.constraintDataTypeMismatch(
            columnPath = Seq("a", "x"),
            columnType = ByteType,
            dataType = IntegerType,
            constraints = Map("ck1" -> "a > 0", "ck2" -> "hash(b) > 0"))
        },
        "DELTA_CONSTRAINT_DATA_TYPE_MISMATCH",
        parameters = Map(
          "columnName" -> "a.x",
          "columnType" -> "TINYINT",
          "dataType" -> "INT",
          "constraints" -> "ck1 -> a > 0\nck2 -> hash(b) > 0"
      ))
    }
    {
      checkError(
        intercept[DeltaAnalysisException] {
          throw DeltaErrors.generatedColumnsDataTypeMismatch(
            columnPath = Seq("a", "x"),
            columnType = ByteType,
            dataType = IntegerType,
            generatedColumns = Map(
              "gen1" -> "a . x + 1",
              "gen2" -> "3 + a . x"
            ))
        },
        "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH",
        parameters = Map(
          "columnName" -> "a.x",
          "columnType" -> "TINYINT",
          "dataType" -> "INT",
          "generatedColumns" -> "gen1 -> a . x + 1\ngen2 -> 3 + a . x"
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.useSetLocation()
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_CHANGE_LOCATION"), Some("42601"),
        Some("Cannot change the 'location' of the Delta table using SET " +
        "TBLPROPERTIES. Please use ALTER TABLE SET LOCATION instead."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnAbsentException(false)
      }
      checkErrorMessage(e, Some("DELTA_NON_PARTITION_COLUMN_ABSENT"), Some("KD005"),
        Some("Data written into Delta needs to contain at least " +
        "one non-partitioned column."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnAbsentException(true)
      }
      checkErrorMessage(e, Some("DELTA_NON_PARTITION_COLUMN_ABSENT"), Some("KD005"),
        Some("Data written into Delta needs to contain at least " +
        "one non-partitioned column. Columns which are of NullType have been dropped."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.constraintAlreadyExists("name", "oldExpr")
      }
      checkErrorMessage(e, Some("DELTA_CONSTRAINT_ALREADY_EXISTS"), Some("42710"),
        Some("Constraint 'name' already exists. Please " +
        "delete the old constraint first.\nOld constraint:\noldExpr"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.timeTravelNotSupportedException
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS"), Some("0AKDC"),
        Some("Cannot time travel views, subqueries, streams or change data feed queries."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.addFilePartitioningMismatchException(Seq("col3"), Seq("col2"))
      }
      checkErrorMessage(e, Some("DELTA_INVALID_PARTITIONING_SCHEMA"), Some("XXKDS"),
        Some("""
          |The AddFile contains partitioning schema different from the table's partitioning schema
          |expected: [`col2`]
          |actual: [`col3`]
          |To disable this check set """.stripMargin +
          "spark.databricks.delta.commitValidation.enabled to \"false\""))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.emptyCalendarInterval
      }
      checkErrorMessage(e, Some("DELTA_INVALID_CALENDAR_INTERVAL_EMPTY"), Some("2200P"),
        Some("Interval cannot be null or blank."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createManagedTableWithoutSchemaException("table-1", spark)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_MANAGED_TABLE_SYNTAX_NO_SCHEMA"), Some("42000"),
        Some(s"""
           |You are trying to create a managed table table-1
           |using Delta, but the schema is not specified.
           |
           |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
              "/index.html", skipValidation = true)}""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUnsupportedExpression("someExp".expr)
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_EXPRESSION_GENERATED_COLUMN"), Some("42621"),
        Some("'someExp' cannot be used in a generated column"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedExpression("Merge", DataTypes.DateType, Seq("Integer", "Long"))
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_EXPRESSION"), Some("0A000"),
        Some("Unsupported expression type(DateType) for Merge. " +
        "The supported types are [Integer,Long]."))
    }
    {
      val expr = "someExp"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUDF(expr.expr)
      }
      checkErrorMessage(e, Some("DELTA_UDF_IN_GENERATED_COLUMN"), Some("42621"),
        Some(s"Found ${expr.sql}. A generated column cannot use a user-defined function"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnColumnTypeNotSupportedException("col1", DateType)
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_COLUMN_TYPE_IN_BLOOM_FILTER"), Some("0AKDC"),
        Some("Creating a bloom filter index on a column with type date is " +
        "unsupported: col1"))
    }
    {
      val e = intercept[DeltaTableFeatureException] {
        throw DeltaErrors.tableFeatureDropHistoryTruncationNotAllowed()
      }
      checkErrorMessage(e, Some("DELTA_FEATURE_DROP_HISTORY_TRUNCATION_NOT_ALLOWED"),
        Some("0AKDE"), Some("The particular feature does not require history truncation."))
    }
    {
      val logRetention = DeltaConfigs.LOG_RETENTION
      val e = intercept[DeltaTableFeatureException] {
        throw DeltaErrors.dropTableFeatureWaitForRetentionPeriod(
          "test_feature",
          Metadata(configuration = Map(logRetention.key -> "30 days")))
      }

      val expectedMessage =
        """Dropping test_feature was partially successful.
          |
          |The feature is now no longer used in the current version of the table. However, the feature
          |is still present in historical versions of the table. The table feature cannot be dropped
          |from the table protocol until these historical versions have expired.
          |
          |To drop the table feature from the protocol, please wait for the historical versions to
          |expire, and then repeat this command. The retention period for historical versions is
          |currently configured as delta.logRetentionDuration=30 days.
          |
          |Alternatively, please wait for the TRUNCATE HISTORY retention period to expire (24 hours)
          |and then run:
          |    ALTER TABLE table_name DROP FEATURE feature_name TRUNCATE HISTORY""".stripMargin
      checkErrorMessage(e, Some("DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD"), Some("0AKDE"),
        Some(expectedMessage))
    }
  }

  test("test DeltaErrors methods -- part 2") {
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedDataTypes(
          UnsupportedDataTypeInfo("foo", CalendarIntervalType),
          UnsupportedDataTypeInfo("bar", TimestampNTZType))
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_DATA_TYPES"), Some("0AKDC"),
        Some("Found columns using unsupported data types: " +
        "[foo: CalendarIntervalType, bar: TimestampNTZType]. " +
        "You can set 'spark.databricks.delta.schema.typeCheck.enabled' to 'false' " +
        "to disable the type check. Disabling this type check may allow users to create " +
        "unsupported Delta tables and should only be used when trying to read/write legacy tables."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnDataLossException(12, 10)
      }
      checkErrorMessage(e, Some("DELTA_MISSING_FILES_UNEXPECTED_VERSION"), Some("XXKDS"),
        Some(s"""The stream from your Delta table was expecting process data from version 12,
         |but the earliest available version in the _delta_log directory is 10. The files
         |in the transaction log may have been deleted due to log cleanup. In order to avoid losing
         |data, we recommend that you restart your stream with a new checkpoint location and to
         |increase your delta.logRetentionDuration setting, if you have explicitly set it below 30
         |days.
         |If you would like to ignore the missed data and continue your stream from where it left
         |off, you can set the .option("failOnDataLoss", "false") as part
         |of your readStream statement.""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedFieldNotSupported("INSERT clause of MERGE operation", "col1")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_NESTED_FIELD_IN_OPERATION"), Some("0AKDC"),
        Some("Nested field is not supported in the INSERT clause of MERGE " +
        "operation (field = col1)."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.newCheckConstraintViolated(10, "table-1", "sample")
      }
      checkErrorMessage(e, Some("DELTA_NEW_CHECK_CONSTRAINT_VIOLATION"), Some("23512"),
        Some("10 rows in table-1 violate the new CHECK constraint (sample)"))
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.failedInferSchema
      }
      checkErrorMessage(e, Some("DELTA_FAILED_INFER_SCHEMA"), Some("42KD9"),
        Some("Failed to infer schema from the given list of files."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartialScan(new Path("path-1"))
      }
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_PARTIAL_SCAN"), Some("KD00A"),
        Some("Expect a full scan of Delta sources, but found a partial scan. " +
        "path:path-1"))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedLogFile(new Path("path-1"))
      }
      checkErrorMessage(e, Some("DELTA_UNRECOGNIZED_LOGFILE"), Some("KD00B"),
        Some("Unrecognized log file path-1"))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unsupportedAbsPathAddFile("path-1")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_ABS_PATH_ADD_FILE"), Some("0AKDC"),
        Some("path-1 does not support adding files with an absolute path"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.outputModeNotSupportedException("source1", "sample")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_OUTPUT_MODE"), Some("0AKDC"),
        Some("Data source source1 does not support sample output mode"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US)
        throw DeltaErrors.timestampGreaterThanLatestCommit(
          new Timestamp(sdf.parse("2022-02-28 10:30:00").getTime),
          new Timestamp(sdf.parse("2022-02-28 10:00:00").getTime), "2022-02-28 10:00:00")
      }
      checkErrorMessage(e, Some("DELTA_TIMESTAMP_GREATER_THAN_COMMIT"), Some("42816"),
        Some("""The provided timestamp (2022-02-28 10:30:00.0) is after the latest version available to this
          |table (2022-02-28 10:00:00.0). Please use a timestamp before or """.stripMargin +
          "at 2022-02-28 10:00:00."))
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.timestampInvalid(expr)
      }
      checkErrorMessage(e, Some("DELTA_TIMESTAMP_INVALID"), Some("42816"),
        Some(s"The provided timestamp (${expr.sql}) cannot be converted to a valid timestamp."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaSourceException("sample")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_SOURCE"), Some("0AKDD"),
        Some("sample destination only supports Delta sources.\n"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreTimestampGreaterThanLatestException("2022-02-02 12:12:12",
          "2022-02-02 12:12:10")
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_RESTORE_TIMESTAMP_GREATER"), Some("22003"),
        Some("Cannot restore table to timestamp (2022-02-02 12:12:12) as it is " +
        "after the latest version available. Please use a timestamp before (2022-02-02 12:12:10)"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnStructNotFoundException("pos1")
      }
      checkErrorMessage(e, Some("DELTA_ADD_COLUMN_STRUCT_NOT_FOUND"), Some("42KD3"),
        Some("Struct not found at position pos1"))
    }
    {
      val column = StructField("c0", IntegerType)
      val other = IntegerType
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnParentNotStructException(column, other)
      }
      checkErrorMessage(e, Some("DELTA_ADD_COLUMN_PARENT_NOT_STRUCT"), Some("42KD3"),
        Some(s"Cannot add ${column.name} because its parent is not a " +
        s"StructType. Found $other"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateNonStructTypeFieldNotSupportedException("col1", DataTypes.DateType)
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_FIELD_UPDATE_NON_STRUCT"), Some("0AKDC"),
        Some("Updating nested fields is only supported for StructType, but you " +
        "are trying to update a field of `col1`, which is of type: DateType."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.extractReferencesFieldNotFound("struct1",
          DeltaErrors.updateSchemaMismatchExpression(
            StructType(Seq(StructField("c0", IntegerType))),
            StructType(Seq(StructField("c1", IntegerType)))
          ))
      }
      checkErrorMessage(e, Some("DELTA_EXTRACT_REFERENCES_FIELD_NOT_FOUND"), Some("XXKDS"),
        Some("Field struct1 could not be found when extracting references."))
    }
    {
      val e = intercept[DeltaIndexOutOfBoundsException] {
        throw DeltaErrors.notNullColumnNotFoundInStruct("struct1")
      }
      checkErrorMessage(e, Some("DELTA_NOT_NULL_COLUMN_NOT_FOUND_IN_STRUCT"), Some("42K09"),
        Some("Not nullable column not found in struct: struct1"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidIdempotentWritesOptionsException("reason")
      }
      checkErrorMessage(e, Some("DELTA_INVALID_IDEMPOTENT_WRITES_OPTIONS"), Some("42616"),
        Some("Invalid options for idempotent Dataframe writes: reason"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("dummyOp")
      }
      checkErrorMessage(e, Some("DELTA_OPERATION_NOT_ALLOWED"), Some("0AKDC"),
        Some("Operation not allowed: `dummyOp` is not supported for Delta tables"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val s1 = StructType(Seq(StructField("c0", IntegerType)))
        val s2 = StructType(Seq(StructField("c0", StringType)))
        throw DeltaErrors.alterTableSetLocationSchemaMismatchException(s1, s2)
      }
      checkErrorMessage(e, Some("DELTA_SET_LOCATION_SCHEMA_MISMATCH"), Some("42KD7"),
        Some(s"""
           |The schema of the new Delta location is different than the current table schema.
           |original schema:
           |root
           | |-- c0: integer (nullable = true)
           |
           |destination schema:
           |root
           | |-- c0: string (nullable = true)
           |
           |
           |If this is an intended change, you may turn this check off by running:
           |%sql set ${DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK.key}""".stripMargin +
          " = true"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundDuplicateColumnsException("integer", "col1")
      }
      checkErrorMessage(e, Some("DELTA_DUPLICATE_COLUMNS_FOUND"), Some("42711"),
        Some("Found duplicate column(s) integer: col1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.subqueryNotSupportedException("dummyOp", "col1")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_SUBQUERY"), Some("0AKDC"),
        Some("Subqueries are not supported in the dummyOp (condition = 'col1')."))
    }
    {
      val schema = StructType(Array(StructField("foo", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundMapTypeColumnException("dummyKey", "dummyVal", schema)
      }
      checkErrorMessage(e, Some("DELTA_FOUND_MAP_TYPE_COLUMN"), Some("KD003"),
        Some(s"""A MapType was found. In order to access the key or value of a MapType, specify one
          |of:
          |dummyKey or
          |dummyVal
          |followed by the name of the column (only if that column is a struct type).
          |e.g. mymap.key.mykey
          |If the column is a basic type, mymap.key or mymap.value is sufficient.
          |Schema:\n""".stripMargin + schema.treeString))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnOfTargetTableNotFoundInMergeException("target", "dummyCol")
      }
      checkErrorMessage(e, Some("DELTA_COLUMN_NOT_FOUND_IN_MERGE"), Some("42703"),
        Some("Unable to find the column 'target' of the target table from " +
        "the INSERT columns: dummyCol. " +
        "INSERT clause must specify value for all the columns of the target table."
      ))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.multiColumnInPredicateNotSupportedException("dummyOp")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_MULTI_COL_IN_PREDICATE"), Some("0AKDC"),
        Some("Multi-column In predicates are not supported in the dummyOp condition."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.newNotNullViolated(10L, "table1", UnresolvedAttribute("col1"))
      }
      checkErrorMessage(e, Some("DELTA_NEW_NOT_NULL_VIOLATION"), Some("23512"),
        Some("10 rows in table1 violate the new NOT NULL constraint on col1"))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.modifyAppendOnlyTableException("dummyTable")
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_MODIFY_APPEND_ONLY"), Some("42809"),
        Some("This table is configured to only allow appends. If you would like to permit " +
          "updates or deletes, use 'ALTER TABLE dummyTable SET TBLPROPERTIES " +
          s"(${DeltaConfigs.IS_APPEND_ONLY.key}=false)'."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.schemaNotConsistentWithTarget("dummySchema", "targetAttr")
      }
      checkErrorMessage(e, Some("DELTA_SCHEMA_NOT_CONSISTENT_WITH_TARGET"), Some("XXKDS"),
        Some("The table schema dummySchema is not consistent with " +
        "the target attributes: targetAttr"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkTaskThreadNotFound
      }
      checkErrorMessage(e, Some("DELTA_SPARK_THREAD_NOT_FOUND"), Some("XXKDS"),
        Some("Not running on a Spark task thread"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.staticPartitionsNotSupportedException
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_STATIC_PARTITIONS"), Some("0AKDD"),
        Some("Specifying static partitions in the partition spec is" +
        " currently not supported during inserts"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedWriteStagedTable("table1")
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_WRITES_STAGED_TABLE"), Some("42807"),
        Some("Table implementation does not support writes: table1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.vacuumBasePathMissingException(new Path("path-1"))
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_VACUUM_SPECIFIC_PARTITION"), Some("0AKDC"),
        Some("Please provide the base path (path-1) when Vacuuming Delta tables. " +
        "Vacuuming specific partitions is currently not supported."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterCreateOnNonExistingColumnsException(Seq("col1", "col2"))
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_CREATE_BLOOM_FILTER_NON_EXISTING_COL"), Some("42703"),
        Some("Cannot create bloom filter indices for the following non-existent column(s): col1, col2"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.zOrderingColumnDoesNotExistException("colName")
      }
      checkErrorMessage(e, None, None,
        Some("Z-Ordering column colName does not exist in data schema."))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.zOrderingOnPartitionColumnException("column1")
      }
      checkErrorMessage(e, Some("DELTA_ZORDERING_ON_PARTITION_COLUMN"), Some("42P10"),
        Some("column1 is a partition column. Z-Ordering can only be performed on data columns"))
    }
    {
      val colNames = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.zOrderingOnColumnWithNoStatsException(colNames, spark)
      }
      checkErrorMessage(e, Some("DELTA_ZORDERING_ON_COLUMN_WITHOUT_STATS"), Some("KD00D"), None)
    }
    {
      checkError(
        intercept[DeltaIllegalStateException] {
          throw MaterializedRowId.missingMetadataException("table_name")
        },
        "DELTA_MATERIALIZED_ROW_TRACKING_COLUMN_NAME_MISSING",
        parameters = Map(
          "rowTrackingColumn" -> "Row ID",
          "tableName" -> "table_name"
        )
      )
    }
    {
      checkError(
        intercept[DeltaIllegalStateException] {
          throw MaterializedRowCommitVersion.missingMetadataException("table_name")
        },
        "DELTA_MATERIALIZED_ROW_TRACKING_COLUMN_NAME_MISSING",
        parameters = Map(
          "rowTrackingColumn" -> "Row Commit Version",
          "tableName" -> "table_name"
        )
      )
    }
  }

  // The compiler complains the lambda function is too large if we put all tests in one lambda.
  test("test DeltaErrors OSS methods more") {
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaNotSetException
      }
      checkErrorMessage(e, Some("DELTA_SCHEMA_NOT_SET"), None,
        Some("Table schema is not set.  Write data into it or use CREATE TABLE to set the schema."))
      checkErrorMessage(e, None, Some("KD008"), None)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaNotProvidedException
      }
      checkErrorMessage(e, Some("DELTA_SCHEMA_NOT_PROVIDED"), None,
        Some("Table schema is not provided. Please provide the schema (column definition) " +
          "of the table when using REPLACE table and an AS SELECT query is not provided."))
      checkErrorMessage(e, None, Some("42908"), None)
    }
    {
      val st1 = StructType(Seq(StructField("a0", IntegerType)))
      val st2 = StructType(Seq(StructField("b0", IntegerType)))
      val schemaDiff = SchemaUtils.reportDifferences(st1, st2)
        .map(_.replace("Specified", "Latest"))

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaChangedSinceAnalysis(st1, st2)
      }

      val msg =
        s"""The schema of your Delta table has changed in an incompatible way since your DataFrame
           |or DeltaTable object was created. Please redefine your DataFrame or DeltaTable object.
           |Changes:
           |${schemaDiff.mkString("\n")}""".stripMargin
      checkErrorMessage(e, Some("DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS"), Some("KD007"),
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsAggregateExpression("1".expr)
      }

      checkErrorMessage(e, Some("DELTA_AGGREGATE_IN_GENERATED_COLUMN"), Some("42621"),
        Some(s"Found ${"1".expr.sql}. " +
        "A generated column cannot use an aggregate expression"))
    }
    {
      val path = new Path("path")
      val specifiedColumns = Seq("col1", "col2")
      val existingColumns = Seq("col3", "col4")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentPartitioningException(
          path, specifiedColumns, existingColumns)
      }

      val msg =
        s"""The specified partitioning does not match the existing partitioning at $path.
           |
           |== Specified ==
           |${specifiedColumns.mkString(", ")}
           |
           |== Existing ==
           |${existingColumns.mkString(", ")}
           |""".stripMargin
      checkErrorMessage(e, Some("DELTA_CREATE_TABLE_WITH_DIFFERENT_PARTITIONING"), Some("42KD7"),
        Some(msg))
    }
    {
      val path = new Path("a/b")
      val smaps = Map("abc" -> "xyz")
      val emaps = Map("def" -> "hjk")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentPropertiesException(path, smaps, emaps)
      }

      val msg =
        s"""The specified properties do not match the existing properties at $path.
           |
           |== Specified ==
           |${smaps.map { case (k, v) => s"$k=$v" }.mkString("\n")}
           |
           |== Existing ==
           |${emaps.map { case (k, v) => s"$k=$v" }.mkString("\n")}
           |""".stripMargin
      checkErrorMessage(e, Some("DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY"), Some("42KD7"),
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportSubqueryInPartitionPredicates()
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_SUBQUERY_IN_PARTITION_PREDICATES"),
        Some("0AKDC"), Some("Subquery is not supported in partition predicates."))
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.emptyDirectoryException("dir")
      }
      checkErrorMessage(e, Some("DELTA_EMPTY_DIRECTORY"), None,
        Some("No file found in the directory: dir."))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.replaceWhereUsedWithDynamicPartitionOverwrite()
      }
      checkErrorMessage(e, Some("DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE"), None,
        Some("A 'replaceWhere' expression and 'partitionOverwriteMode'='dynamic' " +
        "cannot both be set in the DataFrameWriter options."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereUsedInOverwrite()
      }
      checkErrorMessage(e, Some("DELTA_REPLACE_WHERE_IN_OVERWRITE"), Some("42613"),
        Some("You can't use replaceWhere in conjunction with an overwrite by filter"))
    }
    {
      val schema = StructType(Array(StructField("foo", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.incorrectArrayAccessByName("rightName", "wrongName", schema)
      }
      val msg = "An ArrayType was found. In order to access elements of an ArrayType, specify\n" +
        s"rightName instead of wrongName.\nSchema:\n${schema.treeString}"
      checkErrorMessage(e, Some("DELTA_INCORRECT_ARRAY_ACCESS_BY_NAME"), Some("KD003"), Some(msg))
    }
    {
      val columnPath = "colPath"
      val other = IntegerType
      val column = Seq("col1", "col2")
      val schema = StructType(Array(StructField("foo", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnPathNotNested(columnPath, other, column, schema)
      }
      val msg = s"Expected $columnPath to be a nested data type, but found $other. Was looking " +
          s"for the\nindex of ${SchemaUtils.prettyFieldName(column)} in a nested field.\nSchema:\n" +
          s"${schema.treeString}"
      checkErrorMessage(e, Some("DELTA_COLUMN_PATH_NOT_NESTED"), Some("42704"),
        Some(msg))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
      }

      val docLink = generateDocsLink(
        spark.sparkContext.getConf,
        multipleSourceRowMatchingTargetRowInMergeUrl,
        skipValidation = true)
      val msg =
        s"""Cannot perform Merge as multiple source rows matched and attempted to modify the same
           |target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
           |when multiple source rows match on the same target row, the result may be ambiguous
           |as it is unclear which source row should be used to update or delete the matching
           |target row. You can preprocess the source table to eliminate the possibility of
           |multiple matches. Please refer to
           |${docLink}""".stripMargin
      checkErrorMessage(e, Some("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE"), Some("21506"),
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.showPartitionInNotPartitionedTable("table")
      }
      checkErrorMessage(e, Some("DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_TABLE"), Some("42809"),
        Some("SHOW PARTITIONS is not allowed on a table that is not partitioned: table"))
    }
    {
      val badColumns = Set("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.showPartitionInNotPartitionedColumn(badColumns)
      }
      checkErrorMessage(e, Some("DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_COLUMN"), Some("42P10"),
        Some(s"Non-partitioning column(s) ${badColumns.mkString("[", ", ", "]")}" +
          " are specified for SHOW PARTITIONS"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.duplicateColumnOnInsert()
      }
      checkErrorMessage(e, Some("DELTA_DUPLICATE_COLUMNS_ON_INSERT"), Some("42701"),
        Some("Duplicate column names in INSERT clause"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.timeTravelInvalidBeginValue("key", new Throwable)
      }
      checkErrorMessage(e, Some("DELTA_TIME_TRAVEL_INVALID_BEGIN_VALUE"), Some("42604"),
        Some("key needs to be a valid begin value."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.metadataAbsentException()
      }
      checkErrorMessage(e, Some("DELTA_METADATA_ABSENT"), Some("XXKDS"),
        Some("Couldn't find Metadata while committing the first version of the " +
        "Delta table."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(errorClass = "DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION",
          Array.empty)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION"), Some("428FT"),
        Some("Cannot use all columns for partition columns"))
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.failedReadFileFooter("test.txt", null)
      }
      checkErrorMessage(e, Some("DELTA_FAILED_READ_FILE_FOOTER"), Some("KD001"),
        Some("Could not read footer for file: test.txt"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedScanWithHistoricalVersion(123)
      }
      checkErrorMessage(e, Some("DELTA_FAILED_SCAN_WITH_HISTORICAL_VERSION"), Some("KD002"),
        Some("Expect a full scan of the latest version of the Delta source, " +
        "but found a historical scan of version 123"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedRecognizePredicate("select ALL", new Throwable())
      }
      checkErrorMessage(e, Some("DELTA_FAILED_RECOGNIZE_PREDICATE"), Some("42601"),
        Some("Cannot recognize the predicate 'select ALL'"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedFindAttributeInOutputColumns("col1",
          "col2,col3,col4")
      }

      val msg = "Could not find col1 among the existing target output col2,col3,col4"
      checkErrorMessage(e, Some("DELTA_FAILED_FIND_ATTRIBUTE_IN_OUTPUT_COLUMNS"), Some("42703"),
        Some(msg))
    }
    {
      val col = "col1"
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failedFindPartitionColumnInOutputPlan(col)
      }
      checkErrorMessage(e, Some("DELTA_FAILED_FIND_PARTITION_COLUMN_IN_OUTPUT_PLAN"), Some("XXKDS"),
        Some(s"Could not find $col in output plan."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.deltaTableFoundInExecutor()
      }
      checkErrorMessage(e, Some("DELTA_TABLE_FOUND_IN_EXECUTOR"), Some("XXKDS"),
        Some("DeltaTable cannot be used in executors"))
    }
    {
      val e = intercept[DeltaFileAlreadyExistsException] {
        throw DeltaErrors.fileAlreadyExists("file.txt")
      }
      checkErrorMessage(e, Some("DELTA_FILE_ALREADY_EXISTS"), Some("42K04"),
        Some("Existing file path file.txt"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.configureSparkSessionWithExtensionAndCatalog(Some(new Throwable()))
      }

      val catalogImplConfig = SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key
      val msg =
        s"""This Delta operation requires the SparkSession to be configured with the
           |DeltaSparkSessionExtension and the DeltaCatalog. Please set the necessary
           |configurations when creating the SparkSession as shown below.
           |
           |  SparkSession.builder()
           |    .config("spark.sql.extensions", "${classOf[DeltaSparkSessionExtension].getName}")
           |    .config("$catalogImplConfig", "${classOf[DeltaCatalog].getName}")
           |    ...
           |    .getOrCreate()
           |""".stripMargin +
          "\nIf you are using spark-shell/pyspark/spark-submit, you can add the required " +
           "configurations to the command as show below:\n"  +
          s"--conf spark.sql.extensions=${classOf[DeltaSparkSessionExtension].getName} " +
          s"--conf ${catalogImplConfig}=${classOf[DeltaCatalog].getName}\n"
      checkErrorMessage(e, Some("DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG"), Some("56038"),
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cdcNotAllowedInThisVersion()
      }
      checkErrorMessage(e, Some("DELTA_CDC_NOT_ALLOWED_IN_THIS_VERSION"), Some("0AKDC"),
        Some("Configuration delta.enableChangeDataFeed cannot be set." +
          " Change data feed from Delta is not yet available."))
    }
    {
      val ident = TableIdentifier("view1")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertNonParquetTablesException(ident, "source1")
      }
      checkErrorMessage(e, Some("DELTA_CONVERT_NON_PARQUET_TABLE"), Some("0AKDC"),
        Some("CONVERT TO DELTA only supports parquet tables, but you are trying to " +
          s"convert a source1 source: $ident"))
    }
    {
      val from = StructType(Seq(StructField("c0", IntegerType)))
      val to = StructType(Seq(StructField("c1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSchemaMismatchExpression(from, to)
      }
      checkErrorMessage(e, Some("DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION"), Some("42846"),
        Some(s"Cannot cast ${from.catalogString} to ${to.catalogString}. All nested " +
          "columns must match."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.removeFileCDCMissingExtendedMetadata("file")
      }
      checkErrorMessage(e, Some("DELTA_REMOVE_FILE_CDC_MISSING_EXTENDED_METADATA"), Some("XXKDS"),
        Some("""RemoveFile created without extended metadata is ineligible for CDC:
          |file""".stripMargin))
    }
    {
      val columnName = "c0"
      val colMatches = Seq(StructField("c0", IntegerType))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.ambiguousPartitionColumnException(columnName, colMatches)
      }

      val msg =
        s"Ambiguous partition column ${DeltaErrors.formatColumn(columnName)} can be" +
          s" ${DeltaErrors.formatColumnList(colMatches.map(_.name))}."
      checkErrorMessage(e, Some("DELTA_AMBIGUOUS_PARTITION_COLUMN"), Some("42702"),
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.truncateTablePartitionNotSupportedException
      }
      checkErrorMessage(e, Some("DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED"), Some("0AKDC"),
        Some("Operation not allowed: TRUNCATE TABLE on Delta tables does not support" +
          " partition predicates; use DELETE to delete specific partitions or rows."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidFormatFromSourceVersion(100, 10)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION"), Some("XXKDS"),
        Some("Unsupported format. Expected version should be smaller than or equal to 10 but was 100. " +
          "Please upgrade to newer version of Delta."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.emptyDataException
      }
      checkErrorMessage(e, Some("DELTA_EMPTY_DATA"), Some("428GU"),
        Some("Data used in creating the Delta table doesn't have any columns."))
    }
    {
      val path = "path"
      val parsedCol = "col1"
      val expectedCol = "col2"

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(path, parsedCol,
          expectedCol)
      }

      val msg =
        s"Expecting partition column ${DeltaErrors.formatColumn(expectedCol)}, but" +
          s" found partition column ${DeltaErrors.formatColumn(parsedCol)}" +
          s" from parsing the file name: $path"
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_PARTITION_COLUMN_FROM_FILE_NAME"), Some("KD009"),
        Some(msg))
    }
    {
      val path = "path"
      val parsedCols = Seq("col1", "col2")
      val expectedCols = Seq("col3", "col4")

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(path, parsedCols,
          expectedCols)
      }

      val msg =
        s"Expecting ${expectedCols.size} partition column(s): " +
          s"${DeltaErrors.formatColumnList(expectedCols)}," +
          s" but found ${parsedCols.size} partition column(s): " +
          s"${DeltaErrors.formatColumnList(parsedCols)} from parsing the file name: $path"
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_NUM_PARTITION_COLUMNS_FROM_FILE_NAME"), Some("KD009"),
        Some(msg))
    }
    {
      val version = 100L
      val removedFile = "file"
      val dataPath = "tablePath"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(version, removedFile, dataPath)
      }

      val msg =
        s"Detected deleted data (for example $removedFile) from streaming source at " +
          s"version $version. This is currently not supported. If you'd like to ignore deletes, " +
          "set the option 'ignoreDeletes' to 'true'. The source table can be found " +
          s"at path $dataPath."
      checkErrorMessage(e, Some("DELTA_SOURCE_IGNORE_DELETE"), Some("0A000"),
        Some(msg))
    }
    {
      val tableId = "tableId"
      val tableLocation = "path"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithNonEmptyLocation(tableId, tableLocation)
      }

      val msg =
        s"Cannot create table ('${tableId}')." +
          s" The associated location ('${tableLocation}') is not empty and " +
          "also not a Delta table."
      checkErrorMessage(e, Some("DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION"), Some("42601"),
        Some(msg))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.maxArraySizeExceeded()
      }
      checkErrorMessage(e, Some("DELTA_MAX_ARRAY_SIZE_EXCEEDED"), Some("42000"),
        Some("Please use a limit less than Int.MaxValue - 8."))
    }
    {
      val unknownColumns = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterDropOnNonExistingColumnsException(unknownColumns)
      }
      checkErrorMessage(e, Some("DELTA_BLOOM_FILTER_DROP_ON_NON_EXISTING_COLUMNS"), Some("42703"),
        Some("Cannot drop bloom filter indices for the following non-existent column(s): "
          + unknownColumns.mkString(", ")))
    }
    {
      val dataFilters = "filters"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters)
      }

      val msg =
        "'replaceWhere' cannot be used with data filters when " +
          s"'dataChange' is set to false. Filters: ${dataFilters}"
      checkErrorMessage(e, Some("DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET"), Some("42613"),
        Some(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingTableIdentifierException("read")
      }
      checkErrorMessage(e, Some("DELTA_OPERATION_MISSING_PATH"), Some("42601"),
        Some("Please provide the path or table identifier for read."))
    }
    {
      val column = StructField("c0", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUseDataTypeForPartitionColumnError(column)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_PARTITION_COLUMN_TYPE"), Some("42996"),
        Some("Using column c0 of type IntegerType as a partition column is not supported."))
    }
    {
      val catalogPartitionSchema = StructType(Seq(StructField("a", IntegerType)))
      val userPartitionSchema = StructType(Seq(StructField("b", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartitionSchemaFromUserException(catalogPartitionSchema,
          userPartitionSchema)
      }
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_PARTITION_SCHEMA_FROM_USER"), Some("KD009"),
        Some("CONVERT TO DELTA was called with a partition schema different from the partition " +
          "schema inferred from the catalog, please avoid providing the schema so that the " +
          "partition schema can be chosen from the catalog.\n" +
          s"\ncatalog partition schema:\n${catalogPartitionSchema.treeString}" +
          s"\nprovided partition schema:\n${userPartitionSchema.treeString}"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidInterval("interval1")
      }
      checkErrorMessage(e, Some("DELTA_INVALID_INTERVAL"), Some("22006"),
        Some("interval1 is not a valid INTERVAL."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cdcWriteNotAllowedInThisVersion
      }
      checkErrorMessage(e, Some("DELTA_CHANGE_TABLE_FEED_DISABLED"), Some("42807"),
        Some("Cannot write to table with delta.enableChangeDataFeed set. " +
        "Change data feed from Delta is not available."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.specifySchemaAtReadTimeException
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_SCHEMA_DURING_READ"), Some("0AKDC"),
        Some("Delta does not support specifying the schema at read time."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedDataChangeException("operation1")
      }
      checkErrorMessage(e, Some("DELTA_DATA_CHANGE_FALSE"), Some("0AKDE"),
        Some("Cannot change table metadata because the 'dataChange' option is " +
        "set to false. Attempted operation: 'operation1'."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noStartVersionForCDC
      }
      checkErrorMessage(e, Some("DELTA_NO_START_FOR_CDC_READ"), Some("42601"),
        Some("No startingVersion or startingTimestamp provided for CDC read."))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedColumnChange("change1")
      }
      checkErrorMessage(e, Some("DELTA_UNRECOGNIZED_COLUMN_CHANGE"), Some("42601"),
        Some("Unrecognized column change change1. You may be running an out-of-date Delta Lake version."))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.endBeforeStartVersionInCDC(2, 1)
      }
      checkErrorMessage(e, Some("DELTA_INVALID_CDC_RANGE"), Some("22003"),
        Some("CDC range from start 2 to end 1 was invalid. End cannot be before start."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unexpectedChangeFilesFound("a.parquet")
      }
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_CHANGE_FILES_FOUND"), Some("XXKDS"),
        Some("""Change files found in a dataChange = false transaction. Files:
          |a.parquet""".stripMargin))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.logFailedIntegrityCheck(2, "option1")
      }
      checkErrorMessage(e, Some("DELTA_TXN_LOG_FAILED_INTEGRITY"), Some("XXKDS"),
        Some("The transaction log has failed integrity checks. Failed " +
        "verification at version 2 of:\noption1"))
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.checkpointNonExistTable(path)
      }
      checkErrorMessage(e, Some("DELTA_CHECKPOINT_NON_EXIST_TABLE"), Some("42K03"),
        Some(s"Cannot checkpoint a non-existing table $path. " +
          "Did you manually delete files in the _delta_log directory?"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.viewInDescribeDetailException(TableIdentifier("customer"))
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_DESCRIBE_DETAIL_VIEW"), Some("42809"),
        Some("`customer` is a view. DESCRIBE DETAIL is only supported for tables."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathAlreadyExistsException(new Path(path))
      }
      checkErrorMessage(e, Some("DELTA_PATH_EXISTS"), Some("42K04"),
        Some("Cannot write to already existent path /sample/path without setting OVERWRITE = 'true'."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "DELTA_MERGE_MISSING_WHEN",
          messageParameters = Array.empty
        )
      }
      checkErrorMessage(e, Some("DELTA_MERGE_MISSING_WHEN"), Some("42601"),
        Some("There must be at least one WHEN clause in a MERGE statement."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unrecognizedFileAction("invalidAction", "invalidClass")
      }
      checkErrorMessage(e, Some("DELTA_UNRECOGNIZED_FILE_ACTION"), Some("XXKDS"),
        Some("Unrecognized file action invalidAction with type invalidClass."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.streamWriteNullTypeException
      }
      checkErrorMessage(e, Some("DELTA_NULL_SCHEMA_IN_STREAMING_WRITE"), Some("42P18"),
        Some("Delta doesn't accept NullTypes in the schema for streaming writes."))
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaIllegalArgumentException] {
        throw new DeltaIllegalArgumentException(
          errorClass = "DELTA_UNEXPECTED_ACTION_EXPRESSION",
          messageParameters = Array(s"$expr"))
      }
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_ACTION_EXPRESSION"), Some("42601"),
        Some(s"Unexpected action expression $expr."))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unexpectedAlias("alias1")
      }
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_ALIAS"), Some("XXKDS"),
        Some("Expected Alias but got alias1"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unexpectedProject("project1")
      }
      checkErrorMessage(e, Some("DELTA_UNEXPECTED_PROJECT"), Some("XXKDS"),
        Some("Expected Project but got project1"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nullableParentWithNotNullNestedField
      }
      checkErrorMessage(e, Some("DELTA_NOT_NULL_NESTED_FIELD"), Some("0A000"),
        Some("A non-nullable nested field can't be added to a nullable parent. " +
        "Please set the nullability of the parent column accordingly."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.useAddConstraints
      }
      checkErrorMessage(e, Some("DELTA_ADD_CONSTRAINTS"), Some("0A000"),
        Some("Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints."))
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.deltaSourceIgnoreChangesError(10, "removedFile", "tablePath")
      }
      checkErrorMessage(e, Some("DELTA_SOURCE_TABLE_IGNORE_CHANGES"), Some("0A000"),
        Some("Detected a data update (for example removedFile) in the source table at version " +
          "10. This is currently not supported. If this is going to happen regularly and you are" +
          " okay to skip changes, set the option 'skipChangeCommits' to 'true'. If you would like" +
          " the data update to be reflected, please restart this query with a fresh checkpoint" +
          " directory" +
          ". The source table can be found at path tablePath."))
    }
    {
      val limit = "limit"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unknownReadLimit(limit)
      }
      checkErrorMessage(e, Some("DELTA_UNKNOWN_READ_LIMIT"), Some("42601"),
        Some(s"Unknown ReadLimit: $limit"))
    }
    {
      val privilege = "unknown"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unknownPrivilege(privilege)
      }
      checkErrorMessage(e, Some("DELTA_UNKNOWN_PRIVILEGE"), Some("42601"),
        Some(s"Unknown privilege: $privilege"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.deltaLogAlreadyExistsException("path")
      }
      checkErrorMessage(e, Some("DELTA_LOG_ALREADY_EXISTS"), Some("42K04"),
        Some("A Delta log already exists at path"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingPartFilesException(10L, new FileNotFoundException("reason"))
      }
      checkErrorMessage(e, Some("DELTA_MISSING_PART_FILES"), Some("42KD6"),
        Some("Couldn't find all part files of the checkpoint version: 10"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.checkConstraintNotBoolean("name1", "expr1")
      }
      checkErrorMessage(e, Some("DELTA_NON_BOOLEAN_CHECK_CONSTRAINT"), Some("42621"),
        Some("CHECK constraint 'name1' (expr1) should be a boolean expression."))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.checkpointMismatchWithSnapshot
      }
      checkErrorMessage(e, Some("DELTA_CHECKPOINT_SNAPSHOT_MISMATCH"), Some("XXKDS"),
        Some("State of the checkpoint doesn't match that of the snapshot."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException("operation1")
      }
      checkErrorMessage(e, Some("DELTA_ONLY_OPERATION"), Some("0AKDD"),
        Some("operation1 is only supported for Delta tables."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropNestedColumnsFromNonStructTypeException(StringType)
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_DROP_NESTED_COLUMN_FROM_NON_STRUCT_TYPE"), Some("0AKDC"),
        Some(s"Can only drop nested columns from StructType. Found $StringType"))
    }
    {
      val locations = Seq("location1", "location2")
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cannotSetLocationMultipleTimes(locations)
      }
      checkErrorMessage(e, Some("DELTA_CANNOT_SET_LOCATION_MULTIPLE_TIMES"), Some("XXKDS"),
        Some(s"Can't set location multiple times. Found ${locations}"))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.metadataAbsentForExistingCatalogTable("tblName", "file://path/to/table")
      }
      checkErrorMessage(
        e,
        Some("DELTA_METADATA_ABSENT_EXISTING_CATALOG_TABLE"),
        Some("XXKDS"),
        Some(
          "The table tblName already exists in the catalog but no metadata could be found for the table at the path file://path/to/table.\n" +
            "Did you manually delete files from the _delta_log directory? If so, then you should be able to recreate it as follows. First, drop the table by running `DROP TABLE tblName`. Then, recreate it by running the current command again."
        )
      )
    }
    {
      val e = intercept[DeltaStreamingColumnMappingSchemaIncompatibleException] {
        throw DeltaErrors.blockStreamingReadsWithIncompatibleColumnMappingSchemaChanges(
          spark,
          StructType.fromDDL("id int"),
          StructType.fromDDL("id2 int"),
          detectedDuringStreaming = true
        )
      }
      checkErrorMessage(e, Some("DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG"), Some("42KD4"), None)
      assert(e.readSchema == StructType.fromDDL("id int"))
      assert(e.incompatibleSchema == StructType.fromDDL("id2 int"))
      assert(e.additionalProperties("detectedDuringStreaming").toBoolean)
    }
    {
      val e = intercept[DeltaStreamingColumnMappingSchemaIncompatibleException] {
        throw DeltaErrors.blockStreamingReadsWithIncompatibleColumnMappingSchemaChanges(
          spark,
          StructType.fromDDL("id int"),
          StructType.fromDDL("id2 int"),
          detectedDuringStreaming = false
        )
      }
      checkErrorMessage(e, Some("DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG"), Some("42KD4"), None)
      assert(e.readSchema == StructType.fromDDL("id int"))
      assert(e.incompatibleSchema == StructType.fromDDL("id2 int"))
      assert(!e.additionalProperties("detectedDuringStreaming").toBoolean)
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.blockColumnMappingAndCdcOperation(DeltaOperations.ManualUpdate)
      }
      checkErrorMessage(e, Some("DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION"), Some("42KD4"),
        Some("Operation \"Manual Update\" is not allowed when the table has " +
        "enabled change data feed (CDF) and has undergone schema changes using DROP COLUMN or " +
        "RENAME COLUMN."))
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
      checkErrorMessage(e, Some("DELTA_TABLE_FOR_PATH_UNSUPPORTED_HADOOP_CONF"), Some("0AKDC"),
        Some("Currently DeltaTable.forPath only supports hadoop configuration " +
        s"keys starting with $prefixStr but got ${options.mkString(",")}"))
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneOnRelativePath("path")
      }
      checkErrorMessage(e, None, None,
        Some("""The target location for CLONE needs to be an absolute path or table name. Use an
          |absolute path instead of path.""".stripMargin))
    }
    {
      val e = intercept[AnalysisException] {
        throw DeltaErrors.cloneFromUnsupportedSource( "table-0", "CSV")
      }
      assert(e.getErrorClass == "DELTA_CLONE_UNSUPPORTED_SOURCE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == s"[DELTA_CLONE_UNSUPPORTED_SOURCE] Unsupported clone " +
        s"source 'table-0', whose format is CSV.\n" +
        "The supported formats are 'delta', 'iceberg' and 'parquet'.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneReplaceUnsupported(TableIdentifier("customer"))
      }
      checkErrorMessage(e, Some("DELTA_UNSUPPORTED_CLONE_REPLACE_SAME_TABLE"), Some("0AKDC"),
        Some(s"""
           |You tried to REPLACE an existing table (`customer`) with CLONE. This operation is
           |unsupported. Try a different target for CLONE or delete the table at the current target.
           |""".stripMargin))

    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneAmbiguousTarget("external-location", TableIdentifier("table1"))
      }
      checkErrorMessage(e, Some("DELTA_CLONE_AMBIGUOUS_TARGET"), Some("42613"),
        Some(s"""
           |Two paths were provided as the CLONE target so it is ambiguous which to use. An external
           |location for CLONE was provided at external-location at the same time as the path
           |`table1`.""".stripMargin))
    }
    {
      DeltaTableValueFunctions.supportedFnNames.foreach { fnName =>
        {
          val e = intercept[AnalysisException] {
            sql(s"SELECT * FROM ${fnName}()").collect()
          }
          assert(e.getErrorClass == "INCORRECT_NUMBER_OF_ARGUMENTS")
          assert(e.getMessage.contains(
            s"not enough args, $fnName requires at least 2 arguments " +
              "and at most 3 arguments."))
        }
        {
          val e = intercept[AnalysisException] {
            sql(s"SELECT * FROM ${fnName}(1, 2, 3, 4, 5)").collect()
          }
          assert(e.getErrorClass == "INCORRECT_NUMBER_OF_ARGUMENTS")
          assert(e.getMessage.contains(
            s"too many args, $fnName requires at least 2 arguments " +
              "and at most 3 arguments."))
        }
      }
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidTableValueFunction("invalid1")
      }
      checkErrorMessage(e, Some("DELTA_INVALID_TABLE_VALUE_FUNCTION"), Some("22000"),
        Some("Function invalid1 is an unsupported table valued function for CDC reads."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED",
          messageParameters = Array("ALTER TABLE"))
      }
      checkErrorMessage(
        e, Some("WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED"),
        Some("0AKDE"),
        Some(
          s"""Failed to execute ALTER TABLE command because it assigned a column DEFAULT value,
             |but the corresponding table feature was not enabled. Please retry the command again
             |after executing ALTER TABLE tableName SET
             |TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported').""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED",
          messageParameters = Array.empty)
      }
      checkErrorMessage(
        e, Some("WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED"),
        Some("0AKDC"),
        Some(
          s"""Failed to execute the command because DEFAULT values are not supported when adding new
             |columns to previously existing Delta tables; please add the column without a default
             |value first, then run a second ALTER TABLE ALTER COLUMN SET DEFAULT command to apply
             |for future inserted rows instead.""".stripMargin))
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingCommitInfo("featureName", "1225")
      }
      checkErrorMessage(
        e,
        Some("DELTA_MISSING_COMMIT_INFO"),
        Some("KD004"),
        Some(
          "This table has the feature featureName enabled which requires the presence of the " +
           "CommitInfo action in every commit. However, the CommitInfo action is missing from commit " +
           "version 1225.")
      )
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingCommitTimestamp("1225")
      }
      checkErrorMessage(
        e,
        Some("DELTA_MISSING_COMMIT_TIMESTAMP"),
        Some("KD004"),
        Some(
          s"This table has the feature ${InCommitTimestampTableFeature.name} enabled which requires " +
            "the presence of commitTimestamp in the CommitInfo action. However, this field has not " +
            "been set in commit version 1225.")
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidConstraintName("foo")
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0001"),
        None,
        Some("Cannot use 'foo' as the name of a CHECK constraint."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterInvalidParameterValueException("foo")
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0002"),
        None,
        Some("Cannot create bloom filter index, invalid parameter value: 'foo'."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertMetastoreMetadataMismatchException(
          tableProperties = Map("delta.prop1" -> "foo"),
          deltaConfiguration = Map("delta.config1" -> "bar"))
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0003"),
        None,
        Some(
          s"""You are trying to convert a table which already has a delta log where the table properties in the catalog don't match the configuration in the delta log.
             |Table properties in catalog:
             |[delta.prop1=foo]
             |Delta configuration:
             |[delta.config1=bar]
             |If you would like to merge the configurations (update existing fields and insert new ones), set the SQL configuration `spark.databricks.delta.convert.metadataCheck.enabled` to false.""".stripMargin))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreTimestampBeforeEarliestException("2022-02-02 12:12:12",
          "2022-02-02 12:12:14")
      }
      checkErrorMessage(
        e,
        Some("DELTA_CANNOT_RESTORE_TIMESTAMP_EARLIER"),
        Some("22003"),
        Some("Cannot restore table to timestamp (2022-02-02 12:12:12) as it is before the " +
            "earliest version available. Please use a timestamp after (2022-02-02 12:12:14)."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.viewNotSupported("FOO_OP")
      }
      checkErrorMessage(
        e,
        Some("DELTA_OPERATION_ON_VIEW_NOT_ALLOWED"),
        Some("0AKDC"),
        Some("Operation not allowed: FOO_OP cannot be performed on a view."))
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsNonDeterministicExpression(expr)
      }
      checkErrorMessage(
        e,
        Some("DELTA_NON_DETERMINISTIC_EXPRESSION_IN_GENERATED_COLUMN"),
        Some("42621"),
        Some(s"Found ${expr.sql}. A generated column cannot use a non deterministic expression.")
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnBuilderMissingDataType("col1")
      }
      checkErrorMessage(
        e,
        Some("DELTA_COLUMN_MISSING_DATA_TYPE"),
        Some("42601"),
        Some("The data type of the column `col1` was not provided.")
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundViolatingConstraintsForColumnChange(
          "col1", Map("foo" -> "bar"))
      }
      checkErrorMessage(
        e,
        Some("DELTA_CONSTRAINT_DEPENDENT_COLUMN_CHANGE"),
        None,
        Some(
          s"""Cannot alter column col1 because this column is referenced by the following check constraint(s):
             |foo -> bar""".stripMargin)
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableMissingTableNameOrLocation()
      }
      checkErrorMessage(
        e,
        Some("DELTA_CREATE_TABLE_MISSING_TABLE_NAME_OR_LOCATION"),
        Some("42601"),
        Some("Table name or location has to be specified.")
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableIdentifierLocationMismatch("delta.`somePath1`", "somePath2")
      }
      checkErrorMessage(
        e,
        Some("DELTA_CREATE_TABLE_IDENTIFIER_LOCATION_MISMATCH"),
        Some("0AKDC"),
        Some("Creating path-based Delta table with a different location isn't supported. Identifier: delta.`somePath1`, Location: somePath2")
      )
    }
    {
      val schema = StructType(Seq(StructField("col1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropColumnOnSingleFieldSchema(schema)
      }
      checkErrorMessage(
        e,
        Some("DELTA_DROP_COLUMN_ON_SINGLE_FIELD_SCHEMA"),
        Some("0AKDC"),
        Some(s"Cannot drop column from a schema with a single column. Schema:\n${schema.treeString}")
      )
    }
    {
      val schema = StructType(Seq(StructField("col1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.errorFindingColumnPosition(Seq("col2"), schema, "foo")
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0008"),
        None,
        Some(s"Error while searching for position of column col2.\nSchema:" +
          s"\n${schema.treeString}\nError:\nfoo")
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundViolatingGeneratedColumnsForColumnChange(
          columnName = "col1",
          generatedColumns = Map("col2" -> "col1 + 1", "col3" -> "col1 + 2"))
      }
      checkErrorMessage(
        e,
        Some("DELTA_GENERATED_COLUMNS_DEPENDENT_COLUMN_CHANGE"),
        None,
        Some(
          s"""Cannot alter column col1 because this column is referenced by the following generated column(s):
             |col2 -> col1 + 1
             |col3 -> col1 + 2""".stripMargin)
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.identityColumnInconsistentMetadata("col1", true, true, true)
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0006"),
        None,
        Some(s"Inconsistent IDENTITY metadata for column col1 detected: true, true, true")
      )
    }
    {
      val errorBuilder = new MetadataMismatchErrorBuilder()
      val schema1 = StructType(Seq(StructField("c0", IntegerType)))
      val schema2 = StructType(Seq(StructField("c0", StringType)))
      errorBuilder.addSchemaMismatch(schema1, schema2, "id")
      val e = intercept[DeltaAnalysisException] {
        errorBuilder.finalizeAndThrow(spark.sessionState.conf)
      }
      assert(e.getErrorClass == "_LEGACY_ERROR_TEMP_DELTA_0007")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.mergeAddVoidColumn("fooCol")
      }
      checkErrorMessage(
        e,
        Some("DELTA_MERGE_ADD_VOID_COLUMN"),
        Some("42K09"),
        Some(s"Cannot add column `fooCol` with type VOID. Please explicitly specify a non-void type.")
      )
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentAppendException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentAppendException(None, "p1")
      }
      checkErrorMessage(
        e,
        Some("DELTA_CONCURRENT_APPEND"),
        Some("2D521"),
        Some("ConcurrentAppendException: Files were added to p1 by a concurrent update. Please try the operation again."),
        startWith = true
      )
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentDeleteReadException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentDeleteReadException(None, "p1")
      }
      checkErrorMessage(
        e,
        Some("DELTA_CONCURRENT_DELETE_READ"),
        Some("2D521"),
        Some("ConcurrentDeleteReadException: This transaction attempted to read one or more files that were deleted (for example p1) by a concurrent update. "),
        startWith = true
      )
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentDeleteDeleteException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentDeleteDeleteException(None, "p1")
      }
      checkErrorMessage(
        e,
        Some("DELTA_CONCURRENT_DELETE_DELETE"),
        Some("2D521"),
        Some("ConcurrentDeleteDeleteException: This transaction attempted to delete one or more files that were deleted (for example p1) by a concurrent update. "),
        startWith = true
      )
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentTransactionException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentTransactionException(None)
      }
      checkErrorMessage(
        e,
        Some("DELTA_CONCURRENT_TRANSACTION"),
        Some("2D521"),
        Some("ConcurrentTransactionException: This error occurs when multiple streaming queries are using the same checkpoint to write into this table. Did you run multiple instances of the same streaming query at the same time?"),
        startWith = true
      )
    }
    {
      val e = intercept[io.delta.exceptions.ConcurrentWriteException] {
        throw org.apache.spark.sql.delta.DeltaErrors.concurrentWriteException(None)
      }
      checkErrorMessage(
        e,
        Some("DELTA_CONCURRENT_WRITE"),
        Some("2D521"),
        Some("ConcurrentWriteException: A concurrent transaction has written new data since the current transaction read the table."),
        startWith = true
      )
    }
    {
      val e = intercept[io.delta.exceptions.ProtocolChangedException] {
        throw org.apache.spark.sql.delta.DeltaErrors.protocolChangedException(None)
      }
      checkErrorMessage(
        e,
        Some("DELTA_PROTOCOL_CHANGED"),
        Some("2D521"),
        Some("ProtocolChangedException: The protocol version of the Delta table has been changed by a concurrent update."),
        startWith = true
      )
    }
    {
      val e = intercept[io.delta.exceptions.MetadataChangedException] {
        throw org.apache.spark.sql.delta.DeltaErrors.metadataChangedException(None)
      }
      checkErrorMessage(
        e,
        Some("DELTA_METADATA_CHANGED"),
        Some("2D521"),
        Some("MetadataChangedException: The metadata of the Delta table has been changed by a concurrent update."),
        startWith = true
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0009",
          messageParameters = Array("prefixMsg - "))
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0009"),
        None,
        Some("prefixMsg - Updating nested fields is only supported for StructType."))
    }
    {
      val expr = "someExp".expr
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0010",
          messageParameters = Array("prefixMsg - ", expr.sql))
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0010"),
        None,
        Some(s"prefixMsg - Found unsupported expression ${expr.sql} while parsing target column " +
            s"name parts.")
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0011",
          messageParameters = Array.empty)
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0011"),
        None,
        Some("Failed to resolve plan.")
      )
    }
    {
      val exprs = Seq("1".expr, "2".expr)
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "_LEGACY_ERROR_TEMP_DELTA_0012",
          messageParameters = Array(exprs.mkString(",")))
      }
      checkErrorMessage(
        e,
        Some("_LEGACY_ERROR_TEMP_DELTA_0012"),
        None,
        Some(s"Could not resolve expression: ${exprs.mkString(",")}")
      )
    }
    {
      val unsupportedDataType = IntegerType
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.identityColumnDataTypeNotSupported(unsupportedDataType)
      }
      checkErrorMessage(
        e,
        Some("DELTA_IDENTITY_COLUMNS_UNSUPPORTED_DATA_TYPE"),
        Some("428H2"),
        Some(s"DataType ${unsupportedDataType.typeName} is not supported for IDENTITY columns."),
        startWith = true
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.identityColumnIllegalStep()
      }
      checkErrorMessage(
        e,
        Some("DELTA_IDENTITY_COLUMNS_ILLEGAL_STEP"),
        Some("42611"),
        Some("IDENTITY column step cannot be 0."),
        startWith = true
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.identityColumnWithGenerationExpression()
      }
      checkErrorMessage(
        e,
        Some("DELTA_IDENTITY_COLUMNS_WITH_GENERATED_EXPRESSION"),
        Some("42613"),
        Some("IDENTITY column cannot be specified with a generated column expression."),
        startWith = true
      )
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unsupportedWritesWithMissingCoordinators("test")
      }
      checkErrorMessage(
        e,
        Some("DELTA_UNSUPPORTED_WRITES_WITHOUT_COORDINATOR"),
        Some("0AKDC"),
        Some("You are trying to perform writes on a table which has been registered with " +
          "the commit coordinator test"),
        startWith = true
      )
    }
  }
}

class DeltaErrorsSuite
  extends DeltaErrorsSuiteBase
