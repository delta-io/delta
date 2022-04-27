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

import java.io.{PrintWriter, StringWriter}

import scala.sys.process.Process

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaErrors.generateDocsLink
import org.apache.spark.sql.delta.actions.{Action, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.constraints.Invariants
import org.apache.spark.sql.delta.constraints.Invariants.PersistedRule
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.schema.{InvariantViolationException, SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{AnalysisException, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Uuid
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, MetadataBuilder, NullType, StringType, StructField, StructType}


trait DeltaErrorsSuiteBase
    extends QueryTest
    with SharedSparkSession    with GivenWhenThen
    with SQLTestUtils {

  val MAX_URL_ACCESS_RETRIES = 3
  val path = "/sample/path"

  // Map of error name to the actual error message it throws
  // When adding an error, add the name of the function throwing the error as the key and the value
  // as the error being thrown
  def errorsToTest: Map[String, Throwable] = Map(
    "useDeltaOnOtherFormatPathException" ->
      DeltaErrors.useDeltaOnOtherFormatPathException("operation", path, spark),
    "useOtherFormatOnDeltaPathException" ->
      DeltaErrors.useOtherFormatOnDeltaPathException("operation", path, path, "format", spark),
    "createExternalTableWithoutLogException" ->
      DeltaErrors.createExternalTableWithoutLogException(new Path(path), "tableName", spark),
    "createExternalTableWithoutSchemaException" ->
      DeltaErrors.createExternalTableWithoutSchemaException(new Path(path), "tableName", spark),
    "createManagedTableWithoutSchemaException" ->
      DeltaErrors.createManagedTableWithoutSchemaException("tableName", spark),
    "multipleSourceRowMatchingTargetRowInMergeException" ->
      DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark),
    "concurrentModificationException" -> new ConcurrentWriteException(None),
    "foundInvalidCharsInColumnNames" -> DeltaErrors.foundInvalidCharsInColumnNames(
      QueryCompilationErrors.columnNameContainsInvalidCharactersError("tableName")))

  def otherMessagesToTest: Map[String, String] = Map(
    "deltaFileNotFoundHint" ->
      DeltaErrors.deltaFileNotFoundHint(
        DeltaErrors.generateDocsLink(
          sparkConf,
          DeltaErrors.faqRelativePath,
          skipValidation = true), path))

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

  test("Validate that links to docs in DeltaErrors are correct") {
    testUrls()
  }


  test("test DeltaErrors OSS methods") {
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnCheckpoint(new Path("path-1"), new Path("path-2"))
      }
      assert(e.getMessage == "Cannot rename path-1 to path-2")
    }
    {
      val e = intercept[InvariantViolationException] {
        throw DeltaErrors.notNullColumnMissingException(NotNull(Seq("c0", "c1")))
      }
      assert(e.getMessage == "Column c0.c1, which has a NOT NULL constraint, is missing " +
        "from the data being written into the table.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException(DeltaTableIdentifier(Some("path")))
      }
      assert(e.getMessage == "`path` is not a Delta table.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException(
          operation = "delete",
          DeltaTableIdentifier(Some("path")))
      }
      assert(
        e.getMessage == "`path` is not a Delta table. delete is only supported for Delta tables.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidColumnName(name = "col-1")
      }
      assert(e.getMessage == "Attribute name \"col-1\" contains invalid character(s) " +
        "among \" ,;{}()\\\\n\\\\t=\". Please use alias to rename it.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSetColumnNotFoundException(col = "c0", colList = Seq("c1", "c2"))
      }
      assert(e.getMessage == "SET column `c0` not found given columns: [`c1`, `c2`].")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSetConflictException(cols = Seq("c1", "c2"))
      }
      assert(e.getMessage == "There is a conflict from these SET columns: [`c1`, `c2`].")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnNestedColumnNotSupportedException("c0")
      }
      assert(e.getMessage == "Creating a bloom filer index on a nested " +
        "column is currently unsupported: c0")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterDropOnNonIndexedColumnException("c0")
      }
      assert(e.getMessage == "Cannot drop bloom filter index on a non indexed column: c0")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotRenamePath("a", "b")
      }
      assert(e.getMessage == "Cannot rename a to b")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cannotSpecifyBothFileListAndPatternString()
      }
      assert(e.getMessage == "Cannot specify both file list and pattern string.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateArrayField("t", "f")
      }
      assert(
        e.getMessage == "Cannot update t field f type: update the element by updating f.element")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateMapField("t", "f")
      }
      assert(
        e.getMessage == "Cannot update t field f type: update a map by updating f.key or f.value")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateStructField("t", "f")
      }
      assert(e.getMessage == "Cannot update t field f type: update struct by adding, deleting, " +
        "or updating its fields")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.duplicateColumnsOnUpdateTable(originalException = new Exception("123"))
      }
      assert(e.getMessage == "123\nPlease remove duplicate columns before you update your table.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.maxCommitRetriesExceededException(0, 1, 2, 3, 4)
      }
      assert(e.getMessage ==
        s"""This commit has failed as it has been tried 0 times but did not succeed.
           |This can be caused by the Delta table being committed continuously by many concurrent
           |commits.
           |
           |Commit started at version: 2
           |Commit failed at version: 1
           |Number of actions attempted to commit: 3
           |Total time spent attempting this commit: 4 ms""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingColumnsInInsertInto("c")
      }
      assert(e.getMessage == "Column c is not specified in INSERT")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingColumnsInInsertInto("c")
      }
      assert(e.getMessage == "Column c is not specified in INSERT")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.nonExistentDeltaTable("t")
      }
      assert(e.getMessage == "Delta table t doesn't exist. Please delete your streaming query " +
        "checkpoint and restart.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentColumnInSchema("c", "s")
      }
      assert(e.getMessage == "Couldn't find column c in:\ns")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTable("t")
      }
      assert(e.getMessage == "t is not a Delta table. Please drop this table first if you would " +
        "like to recreate it with Delta Lake.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.notFoundFileToBeRewritten("f", Seq("a", "b"))
      }
      assert(e.getMessage == "File (f) to be rewritten not found among candidate files:\na\nb")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsetNonExistentProperty("k", "t")
      }
      assert(e.getMessage == "Attempted to unset non-existent property 'k' in table t")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsReferToWrongColumns(
          new AnalysisException("analysis exception"))
      }
      assert(e.getMessage == "A generated column cannot use a non-existent column or " +
        "another generated column")
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.changeColumnMappingModeNotSupported(oldMode = "old", newMode = "new")
      }
      assert(e.getMessage == "Changing column mapping mode from 'old' to 'new' is not supported.")
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.writesWithColumnMappingNotSupported
      }
      assert(e.getMessage == "Writing data with column mapping mode is not supported.")
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.generateManifestWithColumnMappingNotSupported
      }
      assert(e.getMessage == "Manifest generation is not supported for tables that leverage " +
        "column mapping, as external readers cannot read these Delta tables. See Delta " +
        "documentation for more details.")
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.convertToDeltaWithColumnMappingNotSupported(IdMapping)
      }
      assert(e.getMessage == "The configuration " +
        "'spark.databricks.delta.properties.defaults.columnMapping.mode' cannot be set to `id` " +
        "when using CONVERT TO DELTA.")
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.changeColumnMappingModeOnOldProtocol(Protocol())
      }
      val cmd = "ALTER TABLE SET TBLPROPERTIES"
      assert(e.getMessage ==
        s"""
           |Your current table protocol version does not support changing column mapping modes
           |using delta.columnMapping.mode.
           |
           |Required Delta protocol version for column mapping:
           |Protocol(2,5)
           |Your table's current Delta protocol version:
           |Protocol(2,6)
           |
           |Please upgrade your table's protocol version using $cmd and try again.
           |
           |""".stripMargin)
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.schemaChangeDuringMappingModeChangeNotSupported(
          StructType(Seq(StructField("c0", IntegerType))),
          StructType(Seq(StructField("c1", IntegerType))))
      }
      assert(e.getMessage ==
        """
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
           |""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notEnoughColumnsInInsert(
          "table", 1, 2, Some("nestedField"))
      }
      assert(e.getMessage == "Cannot write to 'table', not enough nested fields in nestedField; " +
        s"target table has 2 column(s) but the inserted data has " +
        s"1 column(s)")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotInsertIntoColumn(
          "tableName", "source", "target", "targetType")
      }
      assert(e.getMessage == "Struct column source cannot be inserted into a " +
        "targetType field target in tableName.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.partitionPathParseException("fragment")
      }
      assert(e.getMessage == "A partition path fragment should be the form like " +
        "`part1=foo/part2=bar`. The partition path: fragment")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereMismatchException("replaceWhere", "badPartitions")
      }
      assert(e.getMessage == """Data written out does not match replaceWhere 'replaceWhere'.
           |Invalid data would be written to partitions badPartitions.""".stripMargin)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.actionNotFoundException("action", 0)
      }
      val msg = s"""The action of your Delta table could not be recovered while Reconstructing
        |version: 0. Did you manually delete files in the _delta_log directory?
        |Set ${DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key} to "false"
        |to skip validation.""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreVersionNotExistException(0, 0, 0)
      }
      assert(e.getMessage == "Cannot restore table to version 0. " +
        "Available versions: [0, 0].")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedGenerateModeException("modeName")
      }
      import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
      val supportedModes = DeltaGenerateCommand.modeNameToGenerationFunc.keys.toSeq.mkString(", ")
      assert(e.getMessage == s"Specified mode 'modeName' is not supported. " +
        s"Supported modes are: $supportedModes")
    }
    {
      import org.apache.spark.sql.delta.DeltaOptions.EXCLUDE_REGEX_OPTION
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.excludeRegexOptionException(EXCLUDE_REGEX_OPTION)
      }
      assert(e.getMessage == s"Please recheck your syntax for '$EXCLUDE_REGEX_OPTION'")
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileNotFoundException("path")
      }
      assert(e.getMessage == s"File path path")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidIsolationLevelException("level")
      }
      assert(e.getMessage == "invalid isolation level 'level'")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnNameNotFoundException("a", "b")
      }
      assert(e.getMessage == "Unable to find the column `a` given [b]")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnAtIndexLessThanZeroException("1", "a")
      }
      assert(e.getMessage == "Index 1 to add column a is lower than 0")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.incorrectArrayAccess()
      }
      assert(e.getMessage ==
        s"""Incorrectly accessing an ArrayType. Use arrayname.element.elementname position to
            |add to an array.""".stripMargin)
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.partitionColumnCastFailed("Value", "Type", "Name")
      }
      assert(e.getMessage == "Failed to cast value `Value` to `Type` for partition column `Name`")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidTimestampFormat("ts", "format")
      }
      assert(e.getMessage == "The provided timestamp ts does not match the expected syntax format.")
    }
    {
      val e = intercept[AnalysisException] {
        throw DeltaErrors.cannotChangeDataType("example message")
      }
      assert(e.getErrorClass == "DELTA_CANNOT_CHANGE_DATA_TYPE")
      assert(e.getSqlState == "22000")
      assert(e.message == "Cannot change data type: example message")
    }
    {
      val table = CatalogTable(TableIdentifier("my table"), null, null, null)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableAlreadyExists(table)
      }
      assert(e.getErrorClass == "DELTA_TABLE_ALREADY_EXISTS")
      assert(e.getSqlState == "42000")
      assert(e.message == "Table `my table` already exists.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonDeterministicNotSupportedException("op", Uuid())
      }
      assert(e.getErrorClass == "DELTA_NON_DETERMINISTIC_FUNCTION_NOT_SUPPORTED")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "Non-deterministic functions " +
        "are not supported in the op (condition = uuid()).")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableNotSupportedException("someOp")
      }
      assert(e.getErrorClass == "DELTA_TABLE_NOT_SUPPORTED_IN_OP")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "Table is not supported in someOp. Please use a path instead.")
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.postCommitHookFailedException(new PostCommitHook() {
          override val name: String = "DummyPostCommitHook"
          override def run(spark: SparkSession, txn: OptimisticTransactionImpl,
            committedActions: Seq[Action]): Unit = {}
        }, 0, "msg", null)
      }
      assert(e.getErrorClass == "DELTA_POST_COMMIT_HOOK_FAILED")
      assert(e.getSqlState == "2D000")
      assert(e.getMessage == "Committing to the Delta table version 0 " +
        "succeeded but error while executing post-commit hook DummyPostCommitHook: msg")
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.postCommitHookFailedException(new PostCommitHook() {
          override val name: String = "DummyPostCommitHook"
          override def run(spark: SparkSession, txn: OptimisticTransactionImpl,
            committedActions: Seq[Action]): Unit = {}
        }, 0, null, null)
      }
      assert(e.getErrorClass == "DELTA_POST_COMMIT_HOOK_FAILED")
      assert(e.getSqlState == "2D000")
      assert(e.getMessage == "Committing to the Delta table version 0 " +
        "succeeded but error while executing post-commit hook DummyPostCommitHook")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerOrEqualThanStruct(1, 1)
      }
      assert(e.getErrorClass == "DELTA_INDEX_LARGER_OR_EQUAL_THAN_STRUCT")
      assert(e.getSqlState == "2F000")
      assert(e.getMessage == "Index 1 to drop column equals to or is larger " +
        "than struct length: 1")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidV1TableCall("v1Table", "DeltaTableV2")
      }
      assert(e.getErrorClass == "DELTA_INVALID_V1_TABLE_CALL")
      assert(e.getSqlState == "42000")
      assert(e.getMessage == "v1Table call is not expected with path based DeltaTableV2")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotGenerateUpdateExpressions()
      }
      assert(e.getErrorClass == "DELTA_CANNOT_GENERATE_UPDATE_EXPRESSIONS")
      assert(e.getSqlState == "42000")
      assert(e.getMessage == "Calling without generated columns should always return a update " +
        "expression for each column")
    }
    {
      val e = intercept[AnalysisException] {
        val s1 = StructType(Seq(StructField("c0", IntegerType)))
        val s2 = StructType(Seq(StructField("c0", StringType)))
        SchemaMergingUtils.mergeSchemas(s1, s2)
      }
      assert(e.getMessage == "Failed to merge fields 'c0' and 'c0'. Failed to merge " +
        "incompatible data types IntegerType and StringType")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.describeViewHistory
      }
      assert(e.getErrorClass == "DELTA_CANNOT_DESCRIBE_VIEW_HISTORY")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "Cannot describe the history of a view.")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedInvariant()
      }
      assert(e.getErrorClass == "DELTA_UNRECOGNIZED_INVARIANT")
      assert(e.getSqlState == "42000")
      assert(e.getMessage == "Unrecognized invariant. Please upgrade your Spark version.")
    }
    {
      val baseSchema = StructType(Seq(StructField("c0", StringType)))
      val field = StructField("id", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotResolveColumn(field, baseSchema)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_RESOLVE_COLUMN")
      assert(e.getSqlState == "42000")
      assert(e.getMessage ==
        """Can't resolve column id in root
         | |-- c0: string (nullable = true)
         |""".stripMargin
        )
    }
    {
      val s1 = StructType(Seq(StructField("c0", IntegerType)))
      val s2 = StructType(Seq(StructField("c0", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.alterTableReplaceColumnsException(s1, s2, "incompatible")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_ALTER_TABLE_REPLACE_COL_OP")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage ==
        """Unsupported ALTER TABLE REPLACE COLUMNS operation. Reason: incompatible
          |
          |Failed to change schema from:
          |root
          | |-- c0: integer (nullable = true)
          |
          |to:
          |root
          | |-- c0: string (nullable = true)
          |""".stripMargin
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val schemeConf = Seq(("key", "val"))
        throw DeltaErrors.logStoreConfConflicts(schemeConf)
      }
      assert(e.getErrorClass == "DELTA_INVALID_LOGSTORE_CONF")
      assert(e.getSqlState == "42000")
      assert(e.getMessage == "(`spark.delta.logStore.class`) and " +
        "(`spark.delta.logStore.key`) cannot " +
        "be set at the same time. Please set only one group of them.")
    }
    {
      val e = intercept[DeltaSparkException] {
        throw DeltaErrors.failedMergeSchemaFile("file", "schema", null)
      }
      assert(e.getErrorClass == "FAILED_MERGE_SCHEMA_FILE")
      assert(e.getMessage == "Failed to merge schema of file file:\nschema")
    }
    {
      val id = TableIdentifier("id")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("op", id)
      }
      assert(e.getErrorClass == "OPERATION_NOT_ALLOWED")
      assert(e.getMessage == s"Operation not allowed: `op` is not supported " +
        s"for Delta tables: $id")
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileOrDirectoryNotFoundException("path")
      }
      assert(e.getErrorClass == "FILE_OR_DIR_NOT_FOUND")
      assert(e.getMessage == "No such file or directory: path")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidPartitionColumn("col", "tbl")
      }
      assert(e.getErrorClass == "INVALID_PARTITION_COLUMN")
      assert(e.getMessage == "col is not a valid partition column in table tbl.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotFindSourceVersionException("json")
      }
      assert(e.getErrorClass == "CANNOT_FIND_VERSION")
      assert(e.getMessage == "Cannot find 'sourceVersion' in json")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unknownConfigurationKeyException("confKey")
      }
      assert(e.getErrorClass == "UNKNOWN_CONFIGURATION")
      assert(e.getMessage == "Unknown configuration was specified: confKey")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathNotExistsException("path")
      }
      assert(e.getErrorClass == "PATH_DOES_NOT_EXIST")
      assert(e.getMessage == "path doesn't exist")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.illegalFilesFound("file")
      }
      assert(e.getErrorClass == "ILLEGAL_FILE_FOUND")
      assert(e.getMessage == "Illegal files found in a dataChange = false transaction. Files: file")
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
      assert(e.getErrorClass == "CREATE_TABLE_SCHEME_MISMATCH")
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noHistoryFound(path)
      }
      assert(e.getErrorClass == "NO_COMMITS_FOUND")
      assert(e.getMessage == s"No commits found at $path")
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.castPartitionValueException("partitionValue", StringType)
      }
      assert(e.getErrorClass == "FAILED_CAST_PARTITION_VALUE")
      assert(e.getMessage == s"Failed to cast partition value `partitionValue` to $StringType")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkSessionNotSetException()
      }
      assert(e.getErrorClass == "SPARK_SESSION_NOT_SET")
      assert(e.getMessage == "Active SparkSession not set.")
    }
    {
      val id = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotReplaceMissingTableException(id)
      }
      assert(e.getErrorClass == "CANNOT_REPLACE_MISSING_TABLE")
      assert(e.getMessage == s"Table $id cannot be replaced as it does not exist. " +
        s"Use CREATE OR REPLACE TABLE to create the table.")
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.cannotCreateLogPathException("logPath")
      }
      assert(e.getErrorClass == "CANNOT_CREATE_LOG_PATH")
      assert(e.getMessage == "Cannot create logPath")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.protocolPropNotIntException("key", "value")
      }
      assert(e.getErrorClass == "PROTOCOL_PROPERTY_NOT_INT")
      assert(e.getMessage == "Protocol property key needs to be an integer. Found value")
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
      assert(e.getErrorClass == "CREATE_EXTERNAL_TABLE_WITHOUT_TXN_LOG")
      assert(e.getMessage.startsWith(msg))
    }
  }
}

class DeltaErrorsSuite
  extends DeltaErrorsSuiteBase
