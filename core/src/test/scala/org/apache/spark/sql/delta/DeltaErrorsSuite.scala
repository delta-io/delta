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
import org.apache.spark.sql.delta.DeltaErrors.generateDocsLink
import org.apache.spark.sql.delta.actions.{Action, Protocol}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.constraints.CharVarcharConstraint
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, InvariantViolationException, SchemaMergingUtils, SchemaUtils, UnsupportedDataTypeInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{CalendarIntervalType, DataTypes, DateType, IntegerType, StringType, StructField, StructType, TimestampNTZType}

trait DeltaErrorsSuiteBase
    extends QueryTest
    with SharedSparkSession    with GivenWhenThen
    with SQLTestUtils {

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
        detectedDuringStreaming = true)
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

  test("Validate that links to docs in DeltaErrors are correct") {
    // verify DeltaErrors.errorsWithDocsLinks is consistent with DeltaErrorsSuite
    assert(errorsToTest.keySet ++ otherMessagesToTest.keySet ==
      DeltaErrors.errorsWithDocsLinks.toSet
    )
    testUrls()
  }

  test("test DeltaErrors methods -- part 1") {
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.tableAlreadyContainsCDCColumns(Seq("col1", "col2"))
      }
      assert(e.getErrorClass == "DELTA_TABLE_ALREADY_CONTAINS_CDC_COLUMNS")
      assert(e.getSqlState == "42711")
      assert(e.getMessage ==
        s"""Unable to enable Change Data Capture on the table. The table already contains
           |reserved columns [col1,col2] that will
           |be used internally as metadata for the table's Change Data Feed. To enable
           |Change Data Feed on the table rename/drop these columns.
           |""".stripMargin)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cdcColumnsInData(Seq("col1", "col2"))
      }
      assert(e.getErrorClass == "RESERVED_CDC_COLUMNS_ON_WRITE")
      assert(e.getSqlState == "42939")
      assert(e.getMessage ==
        s"""
           |The write contains reserved columns [col1,col2] that are used
           |internally as metadata for Change Data Feed. To write to the table either rename/drop
           |these columns or disable Change Data Feed on the table by setting
           |delta.enableChangeDataFeed to false.""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.multipleCDCBoundaryException("sample")
      }
      assert(e.getErrorClass == "DELTA_MULTIPLE_CDC_BOUNDARY")
      assert(e.getSqlState == "42614")
      assert(e.getMessage == "Multiple sample arguments provided for CDC read. Please provide " +
        "one of either sampleTimestamp or sampleVersion.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnCheckpointRename(new Path("path-1"), new Path("path-2"))
      }
      assert(e.getMessage == "Cannot rename path-1 to path-2")
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaErrors.notNullColumnMissingException(NotNull(Seq("c0", "c1")))
      }
      assert(e.getErrorClass == "DELTA_MISSING_NOT_NULL_COLUMN_VALUE")
      assert(e.getSqlState == "23502")
      assert(e.getMessage == "Column c0.c1, which has a NOT NULL constraint, is missing " +
        "from the data being written into the table.")
    }
    {
      val parent = "parent"
      val nested = IntegerType
      val nestType = "nestType"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedNotNullConstraint(parent, nested, nestType)
      }
      assert(e.getErrorClass == "DELTA_NESTED_NOT_NULL_CONSTRAINT")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        s"The $nestType type of the field $parent contains a NOT NULL " +
        s"constraint. Delta does not support NOT NULL constraints nested within arrays or maps. " +
        s"To suppress this error and silently ignore the specified constraints, set " +
        s"${DeltaSQLConf.ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS.key} = true.\n" +
        s"Parsed $nestType type:\n${nested.prettyJson}")
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(Constraints.NotNull(Seq("col1")))
      }
      assert(e.getErrorClass == "DELTA_NOT_NULL_CONSTRAINT_VIOLATED")
      assert(e.getSqlState == "23502")
      assert(e.getMessage == "NOT NULL constraint violated for column: col1.\n")
    }
    {
      val expr = CatalystSqlParser.parseExpression("concat(\"hello \", \"world\")")
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(
          Constraints.Check(CharVarcharConstraint.INVARIANT_NAME,
            LessThanOrEqual(Length(expr), Literal(5))),
          Map.empty[String, Any])
      }
      assert(e.getErrorClass == "DELTA_EXCEED_CHAR_VARCHAR_LIMIT")
      assert(e.getSqlState == "22001")
      assert(e.getMessage == "Exceeds char/varchar type length limitation. " +
        "Failed check: (length('concat(hello , world)) <= 5).")
    }
    {
      val e = intercept[DeltaInvariantViolationException] {
        throw DeltaInvariantViolationException(
          Constraints.Check("__dummy__",
            CatalystSqlParser.parseExpression("id < 0")),
          Map("a" -> "b"))
      }
      assert(e.getErrorClass == "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES")
      assert(e.getSqlState == "23001")
      assert(e.getMessage == "CHECK constraint __dummy__ (id < 0) violated " +
        "by row with values:\n - a : b")
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
      val table = TableIdentifier("table")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotWriteIntoView(table)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_WRITE_INTO_VIEW")
      assert(e.getSqlState == "0A000")
      assert(
        e.getMessage == s"$table is a view. Writes to a view are not supported.")
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
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_NESTED_COLUMN_IN_BLOOM_FILTER")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Creating a bloom filer index on a nested " +
        "column is currently unsupported: c0")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnPartitionColumnNotSupportedException("c0")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_PARTITION_COLUMN_IN_BLOOM_FILTER")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Creating a bloom filter index on a partitioning column " +
        "is unsupported: c0")
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
      val tableName = "table"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUpdateOtherField(tableName, IntegerType)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_UPDATE_OTHER_FIELD")
      assert(e.getSqlState == "429BQ")
      assert(e.getMessage == s"Cannot update $tableName field of type ${IntegerType}")
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
      val table = DeltaTableIdentifier(Some("path"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentDeltaTable(table)
      }
      assert(e.getMessage == s"Delta table $table doesn't exist.")
    }
    {
      val table = "t"
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.nonExistentDeltaTableStreaming(table)
      }
      assert(e.getMessage ==
        s"Delta table $table doesn't exist. Please delete your streaming query " +
        "checkpoint and restart.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonExistentColumnInSchema("c", "s")
      }
      assert(e.getMessage == "Couldn't find column c in:\ns")
    }
    {
      val ident = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaNoSuchTableException] {
        throw DeltaErrors.noRelationTable(ident)
      }
      assert(e.getErrorClass == "DELTA_NO_RELATION_TABLE")
      assert(e.getSqlState == "42P01")
      assert(e.getMessage == s"Table ${ident.quoted} not found")
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
      val current = StructField("c0", IntegerType)
      val update = StructField("c0", StringType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUpdateColumnType(current, update)
      }
      assert(e.getErrorClass == "DELTA_GENERATED_COLUMN_UPDATE_TYPE_MISMATCH")
      assert(e.getSqlState == "42K09")
      assert(e.getMessage ==
        s"Column ${current.name} is a generated column or a column used by a generated column. " +
        s"The data type is ${current.dataType.sql} and cannot be converted to data type " +
        s"${update.dataType.sql}")
    }
    {
      val e = intercept[DeltaColumnMappingUnsupportedException] {
        throw DeltaErrors.changeColumnMappingModeNotSupported(oldMode = "old", newMode = "new")
      }
      assert(e.getMessage == "Changing column mapping mode from 'old' to 'new' is not supported.")
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
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertToDeltaNoPartitionFound("testTable")
      }
      assert(e.getErrorClass == "DELTA_CONVERSION_NO_PARTITION_FOUND")
      assert(e.getSqlState == "42KD6")
      assert(e.getMessage == "Found no partition information in the catalog for table testTable." +
        " Have you run \"MSCK REPAIR TABLE\" on your table to discover partitions?")
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
      val oldAndNew = Seq(
        (Protocol(2, 4), ColumnMappingTableFeature.minProtocolVersion),
        (
          Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION),
          Protocol(TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION)
            .withFeature(ColumnMappingTableFeature)))
      for ((oldProtocol, newProtocol) <- oldAndNew) {
        val e = intercept[DeltaColumnMappingUnsupportedException] {
          throw DeltaErrors.changeColumnMappingModeOnOldProtocol(oldProtocol)
        }
        // scalastyle:off line.size.limit
        assert(e.getMessage ==
          s"""
             |Your current table protocol version does not support changing column mapping modes
             |using delta.columnMapping.mode.
             |
             |Required Delta protocol version for column mapping:
             |${newProtocol.toString}
             |Your table's current Delta protocol version:
             |${oldProtocol.toString}
             |
             |Please enable Column Mapping on your Delta table with mapping mode 'name'.
             |You can use one of the following commands.
             |
             |If your table is already on the required protocol version:
             |ALTER TABLE table_name SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
             |
             |If your table is not on the required protocol version and requires a protocol upgrade:
             |ALTER TABLE table_name SET TBLPROPERTIES (
             |   'delta.columnMapping.mode' = 'name',
             |   'delta.minReaderVersion' = '${newProtocol.minReaderVersion}',
             |   'delta.minWriterVersion' = '${newProtocol.minWriterVersion}')
             |""".stripMargin)
          // scalastyle:off line.size.limit
      }
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
      val colName = "col1"
      val schema = Seq(UnresolvedAttribute("col2"))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.partitionColumnNotFoundException(colName, schema)
      }
      assert(e.getErrorClass == "DELTA_PARTITION_COLUMN_NOT_FOUND")
      assert(e.getSqlState == "42703")
      assert(e.getMessage ==
        s"Partition column ${DeltaErrors.formatColumn(colName)} not found in schema " +
        s"[${schema.map(_.name).mkString(", ")}]")
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
        throw DeltaErrors.replaceWhereMismatchException("replaceWhere",
          new InvariantViolationException("Invariant violated."))
      }
      assert(e.getErrorClass == "DELTA_REPLACE_WHERE_MISMATCH")
      assert(e.getSqlState == "44000")
      assert(e.getMessage == """Data written out does not match replaceWhere 'replaceWhere'.
        |Invariant violated.""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereMismatchException("replaceWhere", "badPartitions")
      }
      assert(e.getErrorClass == "DELTA_REPLACE_WHERE_MISMATCH")
      assert(e.getSqlState == "44000")
      assert(e.getMessage == """Data written out does not match replaceWhere 'replaceWhere'.
        |Invalid data would be written to partitions badPartitions.""".stripMargin)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.actionNotFoundException("action", 0)
      }
      val msg = s"""The action of your Delta table could not be recovered while Reconstructing
        |version: 0. Did you manually delete files in the _delta_log directory?""".stripMargin
      assert(e.getMessage == msg)
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
        assert(e.getMessage == msg)

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
        assert(e.getMessage == msg)

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
        assert(e.getMessage == msg)
      }
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
      val ex = new FileNotFoundException("reason")
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.logFileNotFoundExceptionForStreamingSource(ex)
      }
      assert(e.getErrorClass == "DELTA_LOG_FILE_NOT_FOUND_FOR_STREAMING_SOURCE")
      assert(e.getSqlState == "42K03")
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
      val pos = -1
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropColumnAtIndexLessThanZeroException(-1)
      }
      assert(e.getErrorClass == "DELTA_DROP_COLUMN_AT_INDEX_LESS_THAN_ZERO")
      assert(e.getSqlState == "42KD8")
      assert(e.getMessage == s"Index $pos to drop column is lower than 0")
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
      assert(e.getSqlState == "429BQ")
      assert(e.message == "Cannot change data type: example message")
    }
    {
      val table = CatalogTable(TableIdentifier("my table"), null, null, null)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableAlreadyExists(table)
      }
      assert(e.getErrorClass == "DELTA_TABLE_ALREADY_EXISTS")
      assert(e.getSqlState == "42P07")
      assert(e.message == "Table `my table` already exists.")
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
      assert(e.getErrorClass == "DELTA_TABLE_LOCATION_MISMATCH")
      assert(e.getSqlState == "42613")
      assert(e.message ==
        s"The location of the existing table ${table.identifier.quotedString} is " +
        s"`${existingTable.location}`. It doesn't match the specified location " +
        s"`${table.location}`.")
    }
    {
      val ident = "ident"
      val e = intercept[DeltaNoSuchTableException] {
        throw DeltaErrors.nonSinglePartNamespaceForCatalog(ident)
      }
      assert(e.getErrorClass == "DELTA_NON_SINGLE_PART_NAMESPACE_FOR_CATALOG")
      assert(e.getSqlState == "42K05")
      assert(e.message ==
        s"Delta catalog requires a single-part namespace, but $ident is multi-part.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.targetTableFinalSchemaEmptyException()
      }
      assert(e.getErrorClass == "DELTA_TARGET_TABLE_FINAL_SCHEMA_EMPTY")
      assert(e.getSqlState == "428GU")
      assert(e.getMessage == "Target table final schema is empty.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonDeterministicNotSupportedException("op", Uuid())
      }
      assert(e.getErrorClass == "DELTA_NON_DETERMINISTIC_FUNCTION_NOT_SUPPORTED")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Non-deterministic functions " +
        "are not supported in the op (condition = uuid()).")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.tableNotSupportedException("someOp")
      }
      assert(e.getErrorClass == "DELTA_TABLE_NOT_SUPPORTED_IN_OP")
      assert(e.getSqlState == "42809")
      assert(e.getMessage == "Table is not supported in someOp. Please use a path instead.")
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
      assert(e.getErrorClass == "DELTA_POST_COMMIT_HOOK_FAILED")
      assert(e.getSqlState == "2DKD0")
      assert(e.getMessage == "Committing to the Delta table version 0 " +
        "succeeded but error while executing post-commit hook DummyPostCommitHook: msg")
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
      assert(e.getErrorClass == "DELTA_POST_COMMIT_HOOK_FAILED")
      assert(e.getSqlState == "2DKD0")
      assert(e.getMessage == "Committing to the Delta table version 0 " +
        "succeeded but error while executing post-commit hook DummyPostCommitHook")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerThanStruct(1, StructField("col1", IntegerType), 1)
      }
      assert(e.getErrorClass == "DELTA_INDEX_LARGER_THAN_STRUCT")
      assert(e.getSqlState == "42KD8")
      assert(e.getMessage == "Index 1 to add column StructField(col1,IntegerType,true) is larger " +
        "than struct length: 1")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.indexLargerOrEqualThanStruct(1, 1)
      }
      assert(e.getErrorClass == "DELTA_INDEX_LARGER_OR_EQUAL_THAN_STRUCT")
      assert(e.getSqlState == "42KD8")
      assert(e.getMessage == "Index 1 to drop column equals to or is larger " +
        "than struct length: 1")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidV1TableCall("v1Table", "DeltaTableV2")
      }
      assert(e.getErrorClass == "DELTA_INVALID_V1_TABLE_CALL")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "v1Table call is not expected with path based DeltaTableV2")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotGenerateUpdateExpressions()
      }
      assert(e.getErrorClass == "DELTA_CANNOT_GENERATE_UPDATE_EXPRESSIONS")
      assert(e.getSqlState == "XXKDS")
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
      assert(e.getSqlState == "42809")
      assert(e.getMessage == "Cannot describe the history of a view.")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedInvariant()
      }
      assert(e.getErrorClass == "DELTA_UNRECOGNIZED_INVARIANT")
      assert(e.getSqlState == "56038")
      assert(e.getMessage == "Unrecognized invariant. Please upgrade your Spark version.")
    }
    {
      val baseSchema = StructType(Seq(StructField("c0", StringType)))
      val field = StructField("id", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotResolveColumn(field.name, baseSchema)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_RESOLVE_COLUMN")
      assert(e.getSqlState == "42703")
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
      assert(e.getSqlState == "0AKDC")
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
        val classConf = Seq(("classKey", "classVal"))
        val schemeConf = Seq(("schemeKey", "schemeVal"))
        throw DeltaErrors.logStoreConfConflicts(classConf, schemeConf)
      }
      assert(e.getErrorClass == "DELTA_INVALID_LOGSTORE_CONF")
      assert(e.getSqlState == "F0000")
      assert(e.getMessage == "(`classKey`) and (`schemeKey`) cannot " +
        "be set at the same time. Please set only one group of them.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        val schemeConf = Seq(("key", "val"))
        throw DeltaErrors.inconsistentLogStoreConfs(
          Seq(("delta.key", "value1"), ("spark.delta.key", "value2")))
      }
      assert(e.getErrorClass == "DELTA_INCONSISTENT_LOGSTORE_CONFS")
      assert(e.getSqlState == "F0000")
      assert(e.getMessage == "(delta.key = value1, spark.delta.key = value2) cannot be set to " +
        "different values. Please only set one of them, or set them to the same value.")
    }
    {
      val e = intercept[DeltaSparkException] {
        throw DeltaErrors.failedMergeSchemaFile("file", "schema", null)
      }
      assert(e.getErrorClass == "DELTA_FAILED_MERGE_SCHEMA_FILE")
      assert(e.getMessage == "Failed to merge schema of file file:\nschema")
    }
    {
      val id = TableIdentifier("id")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("op", id)
      }
      assert(e.getErrorClass == "DELTA_OPERATION_NOT_ALLOWED_DETAIL")
      assert(e.getMessage == s"Operation not allowed: `op` is not supported " +
        s"for Delta tables: $id")
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.fileOrDirectoryNotFoundException("path")
      }
      assert(e.getErrorClass == "DELTA_FILE_OR_DIR_NOT_FOUND")
      assert(e.getMessage == "No such file or directory: path")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidPartitionColumn("col", "tbl")
      }
      assert(e.getErrorClass == "DELTA_INVALID_PARTITION_COLUMN")
      assert(e.getMessage == "col is not a valid partition column in table tbl.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.cannotFindSourceVersionException("json")
      }
      assert(e.getErrorClass == "DELTA_CANNOT_FIND_VERSION")
      assert(e.getMessage == "Cannot find 'sourceVersion' in json")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unknownConfigurationKeyException("confKey")
      }
      var msg = "Unknown configuration was specified: confKey\nTo disable this check, set " +
        "spark.databricks.delta.allowArbitraryProperties.enabled=true in the Spark session " +
        "configuration."
      assert(e.getErrorClass == "DELTA_UNKNOWN_CONFIGURATION")
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathNotExistsException("path")
      }
      assert(e.getErrorClass == "DELTA_PATH_DOES_NOT_EXIST")
      assert(e.getMessage == "path doesn't exist")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failRelativizePath("path")
      }
      assert(e.getErrorClass == "DELTA_FAIL_RELATIVIZE_PATH")
      assert(e.getSqlState == "XXKDS")
      var msg =
        """Failed to relativize the path (path). This can happen when absolute paths make
          |it into the transaction log, which start with the scheme
          |s3://, wasbs:// or adls://.
          |
          |If this table is NOT USED IN PRODUCTION, you can set the SQL configuration
          |spark.databricks.delta.vacuum.relativize.ignoreError to true.
          |Using this SQL configuration could lead to accidental data loss, therefore we do
          |not recommend the use of this flag unless this is for testing purposes.""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.illegalFilesFound("file")
      }
      assert(e.getErrorClass == "DELTA_ILLEGAL_FILE_FOUND")
      assert(e.getMessage == "Illegal files found in a dataChange = false transaction. Files: file")
    }
    {
      val name = "name"
      val input = "input"
      val explain = "explain"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.illegalDeltaOptionException(name, input, explain)
      }
      assert(e.getErrorClass == "DELTA_ILLEGAL_OPTION")
      assert(e.getSqlState == "42616")
      assert(e.getMessage == s"Invalid value '$input' for option '$name', $explain")
    }
    {
      val version = "version"
      val timestamp = "timestamp"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.startingVersionAndTimestampBothSetException(version, timestamp)
      }
      assert(e.getErrorClass == "DELTA_STARTING_VERSION_AND_TIMESTAMP_BOTH_SET")
      assert(e.getSqlState == "42613")
      assert(e.getMessage == s"Please either provide '$version' or '$timestamp'")
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
      assert(e.getErrorClass == "DELTA_CREATE_TABLE_SCHEME_MISMATCH")
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noHistoryFound(path)
      }
      assert(e.getErrorClass == "DELTA_NO_COMMITS_FOUND")
      assert(e.getSqlState == "KD006")
      assert(e.getMessage == s"No commits found at $path")
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noRecreatableHistoryFound(path)
      }
      assert(e.getErrorClass == "DELTA_NO_RECREATABLE_HISTORY_FOUND")
      assert(e.getSqlState == "KD006")
      assert(e.getMessage == s"No recreatable commits found at $path")
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.castPartitionValueException("partitionValue", StringType)
      }
      assert(e.getErrorClass == "DELTA_FAILED_CAST_PARTITION_VALUE")
      assert(e.getMessage == s"Failed to cast partition value `partitionValue` to $StringType")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkSessionNotSetException()
      }
      assert(e.getErrorClass == "DELTA_SPARK_SESSION_NOT_SET")
      assert(e.getMessage == "Active SparkSession not set.")
    }
    {
      val id = Identifier.of(Array("namespace"), "name")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotReplaceMissingTableException(id)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_REPLACE_MISSING_TABLE")
      assert(e.getMessage == s"Table $id cannot be replaced as it does not exist. " +
        s"Use CREATE OR REPLACE TABLE to create the table.")
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.cannotCreateLogPathException("logPath")
      }
      assert(e.getErrorClass == "DELTA_CANNOT_CREATE_LOG_PATH")
      assert(e.getMessage == "Cannot create logPath")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.protocolPropNotIntException("key", "value")
      }
      assert(e.getErrorClass == "DELTA_PROTOCOL_PROPERTY_NOT_INT")
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
      assert(e.getErrorClass == "DELTA_CREATE_EXTERNAL_TABLE_WITHOUT_TXN_LOG")
      assert(e.getMessage.startsWith(msg))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.ambiguousPathsInCreateTableException("loc1", "loc2")
      }
      assert(e.getErrorClass == "DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE")
      assert(e.getSqlState == "42613")
      assert(e.getMessage == s"""CREATE TABLE contains two different locations: loc1 and loc2.
        |You can remove the LOCATION clause from the CREATE TABLE statement, or set
        |${DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS.key} to true to skip this check.
        |""".stripMargin)
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.illegalUsageException("overwriteSchema", "replacing")
      }
      assert(e.getErrorClass == "DELTA_ILLEGAL_USAGE")
      assert(e.getSqlState == "42601")
      assert(e.getMessage ==
        "The usage of overwriteSchema is not allowed when replacing a Delta table.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.expressionsNotFoundInGeneratedColumn("col1")
      }
      assert(e.getErrorClass == "DELTA_EXPRESSIONS_NOT_FOUND_IN_GENERATED_COLUMN")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Cannot find the expressions in the generated column col1")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.activeSparkSessionNotFound()
      }
      assert(e.getErrorClass == "DELTA_ACTIVE_SPARK_SESSION_NOT_FOUND")
      assert(e.getSqlState == "08003")
      assert(e.getMessage == "Could not find active SparkSession")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationOnTempViewWithGenerateColsNotSupported("UPDATE")
      }
      assert(e.getErrorClass == "DELTA_OPERATION_ON_TEMP_VIEW_WITH_GENERATED_COLS_NOT_SUPPORTED")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "UPDATE command on a temp view referring to a Delta table that " +
        "contains generated columns is not supported. Please run the UPDATE command on the Delta " +
        "table directly")
    }
    {
      val property = "prop"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.cannotModifyTableProperty(property)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_MODIFY_TABLE_PROPERTY")
      assert(e.getSqlState == "42939")
      assert(e.getMessage ==
        s"The Delta table configuration $property cannot be specified by the user")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingProviderForConvertException("parquet_path")
      }
      assert(e.getErrorClass == "DELTA_MISSING_PROVIDER_FOR_CONVERT")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "CONVERT TO DELTA only supports parquet tables. Please rewrite your " +
        "target as parquet.`parquet_path` if it's a parquet directory.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.iteratorAlreadyClosed()
      }
      assert(e.getErrorClass == "DELTA_ITERATOR_ALREADY_CLOSED")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Iterator is closed")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.activeTransactionAlreadySet()
      }
      assert(e.getErrorClass == "DELTA_ACTIVE_TRANSACTION_ALREADY_SET")
      assert(e.getSqlState == "0B000")
      assert(e.getMessage == "Cannot set a new txn as active when one is already active")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterMultipleConfForSingleColumnException("col1")
      }
      assert(e.getErrorClass == "DELTA_MULTIPLE_CONF_FOR_SINGLE_COLUMN_IN_BLOOM_FILTER")
      assert(e.getSqlState == "42614")
      assert(e.getMessage == "Multiple bloom filter index configurations passed to " +
        "command for column: col1")
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null)
      }
      val docsLink = DeltaErrors.generateDocsLink(
        sparkConf, "/delta-storage.html", skipValidation = true)
      assert(e.getErrorClass == "DELTA_INCORRECT_LOG_STORE_IMPLEMENTATION")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        s"""The error typically occurs when the default LogStore implementation, that
           |is, HDFSLogStore, is used to write into a Delta table on a non-HDFS storage system.
           |In order to get the transactional ACID guarantees on table updates, you have to use the
           |correct implementation of LogStore that is appropriate for your storage system.
           |See $docsLink for details.
           |""".stripMargin)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidSourceVersion(JString("xyz"))
      }
      assert(e.getErrorClass == "DELTA_INVALID_SOURCE_VERSION")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "sourceVersion(JString(xyz)) is invalid")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidCommittedVersion(1L, 2L)
      }
      assert(e.getErrorClass == "DELTA_INVALID_COMMITTED_VERSION")
      assert(e.getSqlState == "XXKDS")
      assert(
        e.getMessage == "The committed version is 1 but the current version is 2."
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnReference("col1", Seq("col2", "col3"))
      }
      assert(e.getErrorClass == "DELTA_NON_PARTITION_COLUMN_REFERENCE")
      assert(e.getSqlState == "42P10")
      assert(e.getMessage == "Predicate references non-partition column 'col1'. Only the " +
        "partition columns may be referenced: [col2, col3]")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val attr = UnresolvedAttribute("col1")
        val attrs = Seq(UnresolvedAttribute("col2"), UnresolvedAttribute("col3"))
        throw DeltaErrors.missingColumn(attr, attrs)
      }
      assert(e.getErrorClass == "DELTA_MISSING_COLUMN")
      assert(e.getSqlState == "42703")
      assert(e.getMessage == "Cannot find col1 in table columns: col2, col3")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val schema = StructType(Seq(StructField("c0", IntegerType)))
        throw DeltaErrors.missingPartitionColumn("c1", schema.catalogString)
      }
      assert(e.getErrorClass == "DELTA_MISSING_PARTITION_COLUMN")
      assert(e.getSqlState == "42KD6")
      assert(e.getMessage == "Partition column `c1` not found in schema struct<c0:int>"
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.aggsNotSupportedException("op", SparkVersion())
      }
      assert(e.getErrorClass == "DELTA_AGGREGATION_NOT_SUPPORTED")
      assert(e.getSqlState == "42903")
      assert(e.getMessage == "Aggregate functions are not supported in the op " +
        "(condition = version())..")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotChangeProvider()
      }
      assert(e.getErrorClass == "DELTA_CANNOT_CHANGE_PROVIDER")
      assert(e.getSqlState == "42939")
      assert(e.getMessage == "'provider' is a reserved table property, and cannot be altered.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.noNewAttributeId(AttributeReference("attr1", IntegerType)())
      }
      assert(e.getErrorClass == "DELTA_NO_NEW_ATTRIBUTE_ID")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Could not find a new attribute ID for column attr1. This " +
        "should have been checked earlier.")
    }
    {
      val e = intercept[ProtocolDowngradeException] {
        val p1 = Protocol(1, 1)
        val p2 = Protocol(2, 2)
        throw new ProtocolDowngradeException(p1, p2)
      }
      assert(e.getErrorClass == "DELTA_INVALID_PROTOCOL_DOWNGRADE")
      assert(e.getSqlState == "KD004")
      assert(e.getMessage == "Protocol version cannot be downgraded from (1,1) to (2,2)")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsTypeMismatch("col1", IntegerType, StringType)
      }
      assert(e.getErrorClass == "DELTA_GENERATED_COLUMNS_EXPR_TYPE_MISMATCH")
      assert(e.getSqlState == "42K09")
      assert(e.getMessage == "The expression type of the generated column col1 is STRING, " +
        "but the column type is INT")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.nonGeneratedColumnMissingUpdateExpression(
          AttributeReference("attr1", IntegerType)(ExprId(1234567L)))
      }
      assert(e.getErrorClass == "DELTA_NON_GENERATED_COLUMN_MISSING_UPDATE_EXPR")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage.matches("attr1#1234567 is not a generated column but is missing " +
        "its update expression"))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val s1 = StructType(Seq(StructField("c0", IntegerType, true)))
        val s2 = StructType(Seq(StructField("c0", StringType, false)))
        SchemaMergingUtils.mergeSchemas(s1, s2, false, false, Set("c0"))
      }
      assert(e.getErrorClass == "DELTA_GENERATED_COLUMNS_DATA_TYPE_MISMATCH")
      assert(e.getSqlState == "42K09")
      assert(e.getMessage == "Column c0 is a generated column or a column used by a generated " +
        "column. The data type is INT. It doesn't accept data type STRING")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.useSetLocation()
      }
      assert(e.getErrorClass == "DELTA_CANNOT_CHANGE_LOCATION")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == "Cannot change the 'location' of the Delta table using SET " +
        "TBLPROPERTIES. Please use ALTER TABLE SET LOCATION instead.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnAbsentException(false)
      }
      assert(e.getErrorClass == "DELTA_NON_PARTITION_COLUMN_ABSENT")
      assert(e.getSqlState == "KD005")
      assert(e.getMessage == "Data written into Delta needs to contain at least " +
        "one non-partitioned column.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nonPartitionColumnAbsentException(true)
      }
      assert(e.getErrorClass == "DELTA_NON_PARTITION_COLUMN_ABSENT")
      assert(e.getSqlState == "KD005")
      assert(e.getMessage == "Data written into Delta needs to contain at least " +
        "one non-partitioned column. Columns which are of NullType have been dropped.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.constraintAlreadyExists("name", "oldExpr")
      }
      assert(e.getErrorClass == "DELTA_CONSTRAINT_ALREADY_EXISTS")
      assert(e.getSqlState == "42710")
      assert(e.getMessage == "Constraint 'name' already exists. Please " +
        "delete the old constraint first.\nOld constraint:\noldExpr")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.timeTravelNotSupportedException
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        "Cannot time travel views, subqueries, streams or change data feed queries.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.addFilePartitioningMismatchException(Seq("col3"), Seq("col2"))
      }
      assert(e.getErrorClass == "DELTA_INVALID_PARTITIONING_SCHEMA")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage ==
        """
          |The AddFile contains partitioning schema different from the table's partitioning schema
          |expected: [`col2`]
          |actual: [`col3`]
          |To disable this check set """.stripMargin +
          "spark.databricks.delta.commitValidation.enabled to \"false\"")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.emptyCalendarInterval
      }
      assert(e.getErrorClass == "DELTA_INVALID_CALENDAR_INTERVAL_EMPTY")
      assert(e.getSqlState == "2200P")
      assert(e.getMessage == "Interval cannot be null or blank.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createManagedTableWithoutSchemaException("table-1", spark)
      }
      assert(e.getErrorClass == "DELTA_INVALID_MANAGED_TABLE_SYNTAX_NO_SCHEMA")
      assert(e.getSqlState == "42000")
      assert(e.getMessage ==
        s"""
           |You are trying to create a managed table table-1
           |using Delta, but the schema is not specified.
           |
           |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
              "/index.html", skipValidation = true)}""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUnsupportedExpression("someExp".expr)
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_EXPRESSION_GENERATED_COLUMN")
      assert(e.getSqlState == "42621")
      assert(e.getMessage == "'someExp' cannot be used in a generated column")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedExpression("Merge", DataTypes.DateType, Seq("Integer", "Long"))
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_EXPRESSION")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "Unsupported expression type(DateType) for Merge. " +
        "The supported types are [Integer,Long].")
    }
    {
      val expr = "someExp"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsUDF(expr.expr)
      }
      assert(e.getErrorClass == "DELTA_UDF_IN_GENERATED_COLUMN")
      assert(e.getSqlState == "42621")
      assert(e.getMessage ==
        s"Found ${expr.sql}. A generated column cannot use a user-defined function")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterOnColumnTypeNotSupportedException("col1", DateType)
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_COLUMN_TYPE_IN_BLOOM_FILTER")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Creating a bloom filter index on a column with type date is " +
        "unsupported: col1")
    }
  }

  test("test DeltaErrors methods -- part 2") {
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedDataTypes(
          UnsupportedDataTypeInfo("foo", CalendarIntervalType),
          UnsupportedDataTypeInfo("bar", TimestampNTZType))
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_DATA_TYPES")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Found columns using unsupported data types: " +
        "[foo: CalendarIntervalType, bar: TimestampNTZType]. " +
        "You can set 'spark.databricks.delta.schema.typeCheck.enabled' to 'false' " +
        "to disable the type check. Disabling this type check may allow users to create " +
        "unsupported Delta tables and should only be used when trying to read/write legacy tables.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failOnDataLossException(12, 10)
      }
      assert(e.getErrorClass == "DELTA_MISSING_FILES_UNEXPECTED_VERSION")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage ==
        s"""The stream from your Delta table was expecting process data from version 12,
         |but the earliest available version in the _delta_log directory is 10. The files
         |in the transaction log may have been deleted due to log cleanup. In order to avoid losing
         |data, we recommend that you restart your stream with a new checkpoint location and to
         |increase your delta.logRetentionDuration setting, if you have explicitly set it below 30
         |days.
         |If you would like to ignore the missed data and continue your stream from where it left
         |off, you can set the .option("failOnDataLoss", "false") as part
         |of your readStream statement.""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedFieldNotSupported("INSERT clause of MERGE operation", "col1")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_NESTED_FIELD_IN_OPERATION")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Nested field is not supported in the INSERT clause of MERGE " +
        "operation (field = col1).")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.newCheckConstraintViolated(10, "table-1", "sample")
      }
      assert(e.getErrorClass == "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION")
      assert(e.getSqlState == "23512")
      assert(e.getMessage == "10 rows in table-1 violate the new CHECK constraint (sample)")
    }
    {
      val e = intercept[DeltaRuntimeException] {
        throw DeltaErrors.failedInferSchema
      }
      assert(e.getErrorClass == "DELTA_FAILED_INFER_SCHEMA")
      assert(e.getSqlState == "42KD9")
      assert(e.getMessage == "Failed to infer schema from the given list of files.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartialScan(new Path("path-1"))
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_PARTIAL_SCAN")
      assert(e.getSqlState == "KD00A")
      assert(e.getMessage == "Expect a full scan of Delta sources, but found a partial scan. " +
        "path:path-1")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedLogFile(new Path("path-1"))
      }
      assert(e.getErrorClass == "DELTA_UNRECOGNIZED_LOGFILE")
      assert(e.getSqlState == "KD00B")
      assert(e.getMessage == "Unrecognized log file path-1")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unsupportedAbsPathAddFile("path-1")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_ABS_PATH_ADD_FILE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "path-1 does not support adding files with an absolute path")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.outputModeNotSupportedException("source1", "sample")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_OUTPUT_MODE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Data source source1 does not support sample output mode")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm", Locale.US)
        throw DeltaErrors.timestampGreaterThanLatestCommit(
          new Timestamp(sdf.parse("2022-02-28 10:30:00").getTime),
          new Timestamp(sdf.parse("2022-02-28 10:00:00").getTime), "2022-02-28 10:00:00")
      }
      assert(e.getErrorClass == "DELTA_TIMESTAMP_GREATER_THAN_COMMIT")
      assert(e.getSqlState == "42816")
      assert(e.getMessage ==
    """The provided timestamp (2022-02-28 10:30:00.0) is after the latest version available to this
          |table (2022-02-28 10:00:00.0). Please use a timestamp before or """.stripMargin +
          "at 2022-02-28 10:00:00.")
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.timestampInvalid(expr)
      }
      assert(e.getErrorClass == "DELTA_TIMESTAMP_INVALID")
      assert(e.getSqlState == "42816")
      assert(e.getMessage ==
        s"The provided timestamp (${expr.sql}) cannot be converted to a valid timestamp.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaSourceException("sample")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_SOURCE")
      assert(e.getSqlState == "0AKDD")
      assert(e.getMessage == "sample destination only supports Delta sources.\n")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.restoreTimestampGreaterThanLatestException("2022-02-02 12:12:12",
          "2022-02-02 12:12:10")
      }
      assert(e.getErrorClass == "DELTA_CANNOT_RESTORE_TIMESTAMP_GREATER")
      assert(e.getSqlState == "22003")
      assert(e.getMessage == "Cannot restore table to timestamp (2022-02-02 12:12:12) as it is " +
        "after the latest version available. Please use a timestamp before (2022-02-02 12:12:10)")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnStructNotFoundException("pos1")
      }
      assert(e.getErrorClass == "DELTA_ADD_COLUMN_STRUCT_NOT_FOUND")
      assert(e.getSqlState == "42KD3")
      assert(e.getMessage == "Struct not found at position pos1")
    }
    {
      val column = StructField("c0", IntegerType)
      val other = IntegerType
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.addColumnParentNotStructException(column, other)
      }
      assert(e.getErrorClass == "DELTA_ADD_COLUMN_PARENT_NOT_STRUCT")
      assert(e.getSqlState == "42KD3")
      assert(e.getMessage ==
        s"Cannot add ${column.name} because its parent is not a " +
        s"StructType. Found $other")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateNonStructTypeFieldNotSupportedException("col1", DataTypes.DateType)
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_FIELD_UPDATE_NON_STRUCT")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Updating nested fields is only supported for StructType, but you " +
        "are trying to update a field of `col1`, which is of type: DateType.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.extractReferencesFieldNotFound("struct1",
          DeltaErrors.updateSchemaMismatchExpression(
            StructType(Seq(StructField("c0", IntegerType))),
            StructType(Seq(StructField("c1", IntegerType)))
          ))
      }
      assert(e.getErrorClass == "DELTA_EXTRACT_REFERENCES_FIELD_NOT_FOUND")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Field struct1 could not be found when extracting references.")
    }
    {
      val e = intercept[DeltaIndexOutOfBoundsException] {
        throw DeltaErrors.notNullColumnNotFoundInStruct("struct1")
      }
      assert(e.getErrorClass == "DELTA_NOT_NULL_COLUMN_NOT_FOUND_IN_STRUCT")
      assert(e.getSqlState == "42K09")
      assert(e.getMessage == "Not nullable column not found in struct: struct1")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidIdempotentWritesOptionsException("reason")
      }
      assert(e.getErrorClass == "DELTA_INVALID_IDEMPOTENT_WRITES_OPTIONS")
      assert(e.getSqlState == "42616")
      assert(e.getMessage == "Invalid options for idempotent Dataframe writes: reason")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.operationNotSupportedException("dummyOp")
      }
      assert(e.getErrorClass == "DELTA_OPERATION_NOT_ALLOWED")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Operation not allowed: `dummyOp` is not supported for Delta tables")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        val s1 = StructType(Seq(StructField("c0", IntegerType)))
        val s2 = StructType(Seq(StructField("c0", StringType)))
        throw DeltaErrors.alterTableSetLocationSchemaMismatchException(s1, s2)
      }
      assert(e.getErrorClass == "DELTA_SET_LOCATION_SCHEMA_MISMATCH")
      assert(e.getSqlState == "42KD7")
      assert(e.getMessage ==
        s"""
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
          " = true")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundDuplicateColumnsException("integer", "col1")
      }
      assert(e.getErrorClass == "DELTA_DUPLICATE_COLUMNS_FOUND")
      assert(e.getSqlState == "42711")
      assert(e.getMessage == "Found duplicate column(s) integer: col1")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.subqueryNotSupportedException("dummyOp", "col1")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_SUBQUERY")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Subqueries are not supported in the dummyOp (condition = 'col1').")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.foundMapTypeColumnException("dummyKey", "dummyVal")
      }
      assert(e.getErrorClass == "DELTA_FOUND_MAP_TYPE_COLUMN")
      assert(e.getSqlState == "KD003")
      assert(e.getMessage ==
        """A MapType was found. In order to access the key or value of a MapType, specify one
          |of:
          |dummyKey or
          |dummyVal
          |followed by the name of the column (only if that column is a struct type).
          |e.g. mymap.key.mykey
          |If the column is a basic type, mymap.key or mymap.value is sufficient.""".stripMargin)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnOfTargetTableNotFoundInMergeException("target", "dummyCol")
      }
      assert(e.getErrorClass == "DELTA_COLUMN_NOT_FOUND_IN_MERGE")
      assert(e.getSqlState == "42703")
      assert(e.getMessage == "Unable to find the column 'target' of the target table from " +
        "the INSERT columns: dummyCol. " +
        "INSERT clause must specify value for all the columns of the target table."
      )
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.multiColumnInPredicateNotSupportedException("dummyOp")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_MULTI_COL_IN_PREDICATE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        "Multi-column In predicates are not supported in the dummyOp condition.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.newNotNullViolated(10L, "table1", UnresolvedAttribute("col1"))
      }
      assert(e.getErrorClass == "DELTA_NEW_NOT_NULL_VIOLATION")
      assert(e.getSqlState == "23512")
      assert(e.getMessage == "10 rows in table1 violate the new NOT NULL constraint on col1")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.modifyAppendOnlyTableException("dummyTable")
      }
      assert(e.getErrorClass == "DELTA_CANNOT_MODIFY_APPEND_ONLY")
      assert(e.getSqlState == "42809")
      assert(e.getMessage ==
        "This table is configured to only allow appends. If you would like to permit " +
          "updates or deletes, use 'ALTER TABLE dummyTable SET TBLPROPERTIES " +
          s"(${DeltaConfigs.IS_APPEND_ONLY.key}=false)'.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.schemaNotConsistentWithTarget("dummySchema", "targetAttr")
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_NOT_CONSISTENT_WITH_TARGET")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "The table schema dummySchema is not consistent with " +
        "the target attributes: targetAttr")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.sparkTaskThreadNotFound
      }
      assert(e.getErrorClass == "DELTA_SPARK_THREAD_NOT_FOUND")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Not running on a Spark task thread")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.staticPartitionsNotSupportedException
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_STATIC_PARTITIONS")
      assert(e.getSqlState == "0AKDD")
      assert(e.getMessage == "Specifying static partitions in the partition spec is" +
        " currently not supported during inserts")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportedWriteStagedTable("table1")
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_WRITES_STAGED_TABLE")
      assert(e.getSqlState == "42807")
      assert(e.getMessage == "Table implementation does not support writes: table1")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.vacuumBasePathMissingException(new Path("path-1"))
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_VACUUM_SPECIFIC_PARTITION")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Please provide the base path (path-1) when Vacuuming Delta tables. " +
        "Vacuuming specific partitions is currently not supported.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterCreateOnNonExistingColumnsException(Seq("col1", "col2"))
      }
      assert(e.getErrorClass == "DELTA_CANNOT_CREATE_BLOOM_FILTER_NON_EXISTING_COL")
      assert(e.getSqlState == "42703")
      assert(e.getMessage ==
        "Cannot create bloom filter indices for the following non-existent column(s): col1, col2")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.zOrderingColumnDoesNotExistException("colName")
      }
      assert(e.getMessage == "Z-Ordering column colName does not exist in data schema.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.zOrderingOnPartitionColumnException("column1")
      }
      assert(e.getErrorClass == "DELTA_ZORDERING_ON_PARTITION_COLUMN")
      assert(e.getSqlState == "42P10")
      assert(e.getMessage ==
                 "column1 is a partition column. Z-Ordering can only be performed on data columns")
    }
    {
      val colNames = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.zOrderingOnColumnWithNoStatsException(colNames, spark)
      }
      assert(e.getErrorClass == "DELTA_ZORDERING_ON_COLUMN_WITHOUT_STATS")
      assert(e.getSqlState == "KD00D")
    }
  }

  // Complier complains the lambda function is too large if we put all tests in one lambda
  test("test DeltaErrors OSS methods more") {
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaNotSetException
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_NOT_SET")
      assert(e.getMessage ==
        "Table schema is not set.  Write data into it or use CREATE TABLE to set the schema.")
      assert(e.getSqlState == "KD008")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaNotProvidedException
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_NOT_PROVIDED")
      assert(e.getMessage ==
        "Table schema is not provided. Please provide the schema (column definition) " +
          "of the table when using REPLACE table and an AS SELECT query is not provided.")
      assert(e.getSqlState == "42908")
    }
    {
      val st1 = StructType(Seq(StructField("a0", IntegerType)))
      val st2 = StructType(Seq(StructField("b0", IntegerType)))
      val schemaDiff = SchemaUtils.reportDifferences(st1, st2)
        .map(_.replace("Specified", "Latest"))

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.schemaChangedSinceAnalysis(st1, st2)
      }
      assert(e.getErrorClass == "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS")

      val msg =
        s"""The schema of your Delta table has changed in an incompatible way since your DataFrame
           |or DeltaTable object was created. Please redefine your DataFrame or DeltaTable object.
           |Changes:
           |${schemaDiff.mkString("\n")}""".stripMargin
      assert(e.getMessage == msg)
      assert(e.getSqlState == "KD007")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.generatedColumnsAggregateExpression("1".expr)
      }
      assert(e.getErrorClass == "DELTA_AGGREGATE_IN_GENERATED_COLUMN")
      assert(e.getSqlState == "42621")

      assert(e.getMessage == s"Found ${"1".expr.sql}. " +
        "A generated column cannot use an aggregate expression")
    }
    {
      val path = new Path("path")
      val specifiedColumns = Seq("col1", "col2")
      val existingColumns = Seq("col3", "col4")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentPartitioningException(
          path, specifiedColumns, existingColumns)
      }
      assert(e.getErrorClass == "DELTA_CREATE_TABLE_WITH_DIFFERENT_PARTITIONING")
      assert(e.getSqlState == "42KD7")

      val msg =
        s"""The specified partitioning does not match the existing partitioning at $path.
           |
           |== Specified ==
           |${specifiedColumns.mkString(", ")}
           |
           |== Existing ==
           |${existingColumns.mkString(", ")}
           |""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val path = new Path("a/b")
      val smaps = Map("abc" -> "xyz")
      val emaps = Map("def" -> "hjk")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithDifferentPropertiesException(path, smaps, emaps)
      }
      assert(e.getErrorClass == "DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY")
      assert(e.getSqlState == "42KD7")

      val msg =
        s"""The specified properties do not match the existing properties at $path.
           |
           |== Specified ==
           |${smaps.map { case (k, v) => s"$k=$v" }.mkString("\n")}
           |
           |== Existing ==
           |${emaps.map { case (k, v) => s"$k=$v" }.mkString("\n")}
           |""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unsupportSubqueryInPartitionPredicates()
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_SUBQUERY_IN_PARTITION_PREDICATES")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Subquery is not supported in partition predicates.")
    }
    {
      val e = intercept[DeltaFileNotFoundException] {
        throw DeltaErrors.emptyDirectoryException("dir")
      }
      assert(e.getErrorClass == "DELTA_EMPTY_DIRECTORY")
      assert(e.getMessage == "No file found in the directory: dir.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.replaceWhereUsedWithDynamicPartitionOverwrite()
      }
      assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE")
      assert(e.getMessage == "A 'replaceWhere' expression and 'partitionOverwriteMode'='dynamic' " +
        "cannot both be set in the DataFrameWriter options.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereUsedInOverwrite()
      }
      assert(e.getErrorClass == "DELTA_REPLACE_WHERE_IN_OVERWRITE")
      assert(e.getSqlState == "42613")
      assert(e.getMessage ==
        "You can't use replaceWhere in conjunction with an overwrite by filter")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.incorrectArrayAccessByName("rightName", "wrongName")
      }
      assert(e.getErrorClass == "DELTA_INCORRECT_ARRAY_ACCESS_BY_NAME")
      assert(e.getSqlState == "KD003")

      val msg =
        s"""An ArrayType was found. In order to access elements of an ArrayType, specify
           |rightName
           |Instead of wrongName
           |""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val columnPath = "colPath"
      val other = IntegerType
      val column = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.columnPathNotNested(columnPath, other, column)
      }
      assert(e.getErrorClass == "DELTA_COLUMN_PATH_NOT_NESTED")
      assert(e.getSqlState == "42704")
      val msg =
        s"""Expected $columnPath to be a nested data type, but found $other. Was looking for the
           |index of ${SchemaUtils.prettyFieldName(column)} in a nested field
           |""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.multipleSourceRowMatchingTargetRowInMergeException(spark)
      }
      assert(e.getErrorClass == "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE")
      assert(e.getSqlState == "21506")

      val docLink = generateDocsLink(spark.sparkContext.getConf,
        "/delta-update.html#upsert-into-a-table-using-merge", skipValidation = true)
      val msg =
        s"""Cannot perform Merge as multiple source rows matched and attempted to modify the same
           |target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
           |when multiple source rows match on the same target row, the result may be ambiguous
           |as it is unclear which source row should be used to update or delete the matching
           |target row. You can preprocess the source table to eliminate the possibility of
           |multiple matches. Please refer to
           |${docLink}""".stripMargin
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.showPartitionInNotPartitionedTable("table")
      }
      assert(e.getErrorClass == "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_TABLE")
      assert(e.getSqlState == "42809")
      assert(e.getMessage ==
        "SHOW PARTITIONS is not allowed on a table that is not partitioned: table")
    }
    {
      val badColumns = Set("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.showPartitionInNotPartitionedColumn(badColumns)
      }
      assert(e.getErrorClass == "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_COLUMN")
      assert(e.getSqlState == "42P10")
      assert(e.getMessage ==
        s"Non-partitioning column(s) ${badColumns.mkString("[", ", ", "]")}" +
          " are specified for SHOW PARTITIONS")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.duplicateColumnOnInsert()
      }
      assert(e.getErrorClass == "DELTA_DUPLICATE_COLUMNS_ON_INSERT")
      assert(e.getSqlState == "42701")
      assert(e.getMessage == "Duplicate column names in INSERT clause")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.timeTravelInvalidBeginValue("key", new Throwable)
      }
      assert(e.getErrorClass == "DELTA_TIME_TRAVEL_INVALID_BEGIN_VALUE")
      assert(e.getSqlState == "42604")
      assert(e.getMessage == "key needs to be a valid begin value.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.metadataAbsentException()
      }
      assert(e.getErrorClass == "DELTA_METADATA_ABSENT")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Couldn't find Metadata while committing the first version of the " +
        "Delta table.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(errorClass = "DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION",
          Array.empty)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_USE_ALL_COLUMNS_FOR_PARTITION")
      assert(e.getSqlState == "428FT")
      assert(e.getMessage == "Cannot use all columns for partition columns")
    }
    {
      val e = intercept[DeltaIOException] {
        throw DeltaErrors.failedReadFileFooter("test.txt", null)
      }
      assert(e.getErrorClass == "DELTA_FAILED_READ_FILE_FOOTER")
      assert(e.getSqlState == "KD001")
      assert(e.getMessage == "Could not read footer for file: test.txt")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedScanWithHistoricalVersion(123)
      }
      assert(e.getErrorClass == "DELTA_FAILED_SCAN_WITH_HISTORICAL_VERSION")
      assert(e.getSqlState == "KD002")
      assert(e.getMessage == "Expect a full scan of the latest version of the Delta source, " +
        "but found a historical scan of version 123")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedRecognizePredicate("select ALL", new Throwable())
      }
      assert(e.getErrorClass == "DELTA_FAILED_RECOGNIZE_PREDICATE")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == "Cannot recognize the predicate 'select ALL'")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.failedFindAttributeInOutputColumns("col1",
          "col2,col3,col4")
      }
      assert(e.getErrorClass == "DELTA_FAILED_FIND_ATTRIBUTE_IN_OUTPUT_COLUMNS")
      assert(e.getSqlState == "42703")

      val msg = "Could not find col1 among the existing target output col2,col3,col4"
      assert(e.getMessage == msg)
    }
    {
      val col = "col1"
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.failedFindPartitionColumnInOutputPlan(col)
      }
      assert(e.getErrorClass == "DELTA_FAILED_FIND_PARTITION_COLUMN_IN_OUTPUT_PLAN")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == s"Could not find $col in output plan.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.deltaTableFoundInExecutor()
      }
      assert(e.getErrorClass == "DELTA_TABLE_FOUND_IN_EXECUTOR")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "DeltaTable cannot be used in executors")
    }
    {
      val e = intercept[DeltaFileAlreadyExistsException] {
        throw DeltaErrors.fileAlreadyExists("file.txt")
      }
      assert(e.getErrorClass == "DELTA_FILE_ALREADY_EXISTS")
      assert(e.getSqlState == "42K04")
      assert(e.getMessage == "Existing file path file.txt")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.configureSparkSessionWithExtensionAndCatalog(Some(new Throwable()))
      }
      assert(e.getErrorClass == "DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG")
      assert(e.getSqlState == "56038")

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
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cdcNotAllowedInThisVersion()
      }
      assert(e.getErrorClass == "DELTA_CDC_NOT_ALLOWED_IN_THIS_VERSION")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        "Configuration delta.enableChangeDataFeed cannot be set." +
          " Change data feed from Delta is not yet available.")
    }
    {
      val ident = TableIdentifier("view1")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.convertNonParquetTablesException(ident, "source1")
      }
      assert(e.getErrorClass == "DELTA_CONVERT_NON_PARQUET_TABLE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        "CONVERT TO DELTA only supports parquet tables, but you are trying to " +
          s"convert a source1 source: $ident")
    }
    {
      val from = StructType(Seq(StructField("c0", IntegerType)))
      val to = StructType(Seq(StructField("c1", IntegerType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.updateSchemaMismatchExpression(from, to)
      }
      assert(e.getErrorClass == "DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION")
      assert(e.getSqlState == "42846")
      assert(e.getMessage ==
        s"Cannot cast ${from.catalogString} to ${to.catalogString}. All nested " +
          "columns must match.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.removeFileCDCMissingExtendedMetadata("file")
      }
      assert(e.getErrorClass == "DELTA_REMOVE_FILE_CDC_MISSING_EXTENDED_METADATA")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage ==
        """RemoveFile created without extended metadata is ineligible for CDC:
          |file""".stripMargin)
    }
    {
      val columnName = "c0"
      val colMatches = Seq(StructField("c0", IntegerType))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.ambiguousPartitionColumnException(columnName, colMatches)
      }
      assert(e.getErrorClass == "DELTA_AMBIGUOUS_PARTITION_COLUMN")
      assert(e.getSqlState == "42702")

      val msg =
        s"Ambiguous partition column ${DeltaErrors.formatColumn(columnName)} can be" +
          s" ${DeltaErrors.formatColumnList(colMatches.map(_.name))}."
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.truncateTablePartitionNotSupportedException
      }
      assert(e.getErrorClass == "DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        "Operation not allowed: TRUNCATE TABLE on Delta tables does not support" +
          " partition predicates; use DELETE to delete specific partitions or rows.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.invalidFormatFromSourceVersion(100, 10)
      }
      assert(e.getErrorClass == "DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage ==
        "Unsupported format. Expected version should be smaller than or equal to 10 but was 100. " +
          "Please upgrade to newer version of Delta.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.emptyDataException
      }
      assert(e.getErrorClass == "DELTA_EMPTY_DATA")
      assert(e.getSqlState == "428GU")
      assert(e.getMessage == "Data used in creating the Delta table doesn't have any columns.")
    }
    {
      val path = "path"
      val parsedCol = "col1"
      val expectedCol = "col2"

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartitionColumnFromFileNameException(path, parsedCol,
          expectedCol)
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_PARTITION_COLUMN_FROM_FILE_NAME")
      assert(e.getSqlState == "KD009")

      val msg =
        s"Expecting partition column ${DeltaErrors.formatColumn(expectedCol)}, but" +
          s" found partition column ${DeltaErrors.formatColumn(parsedCol)}" +
          s" from parsing the file name: $path"
      assert(e.getMessage == msg)
    }
    {
      val path = "path"
      val parsedCols = Seq("col1", "col2")
      val expectedCols = Seq("col3", "col4")

      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedNumPartitionColumnsFromFileNameException(path, parsedCols,
          expectedCols)
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_NUM_PARTITION_COLUMNS_FROM_FILE_NAME")
      assert(e.getSqlState == "KD009")

      val msg =
        s"Expecting ${expectedCols.size} partition column(s): " +
          s"${DeltaErrors.formatColumnList(expectedCols)}," +
          s" but found ${parsedCols.size} partition column(s): " +
          s"${DeltaErrors.formatColumnList(parsedCols)} from parsing the file name: $path"
      assert(e.getMessage == msg)
    }
    {
      val version = 100L
      val removedFile = "file"
      val dataPath = "tablePath"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.deltaSourceIgnoreDeleteError(version, removedFile, dataPath)
      }
      assert(e.getErrorClass == "DELTA_SOURCE_IGNORE_DELETE")
      assert(e.getSqlState == "0A000")

      val msg =
        s"Detected deleted data (for example $removedFile) from streaming source at " +
          s"version $version. This is currently not supported. If you'd like to ignore deletes, " +
          "set the option 'ignoreDeletes' to 'true'. The source table can be found " +
          s"at path $dataPath."
      assert(e.getMessage == msg)
    }
    {
      val tableId = "tableId"
      val tableLocation = "path"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.createTableWithNonEmptyLocation(tableId, tableLocation)
      }
      assert(e.getErrorClass == "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION")
      assert(e.getSqlState == "42601")

      val msg =
        s"Cannot create table ('${tableId}')." +
          s" The associated location ('${tableLocation}') is not empty and " +
          "also not a Delta table."
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.maxArraySizeExceeded()
      }
      assert(e.getErrorClass == "DELTA_MAX_ARRAY_SIZE_EXCEEDED")
      assert(e.getSqlState == "42000")
      assert(e.getMessage == "Please use a limit less than Int.MaxValue - 8.")
    }
    {
      val unknownColumns = Seq("col1", "col2")
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.bloomFilterDropOnNonExistingColumnsException(unknownColumns)
      }
      assert(e.getErrorClass == "DELTA_BLOOM_FILTER_DROP_ON_NON_EXISTING_COLUMNS")
      assert(e.getSqlState == "42703")
      assert(e.getMessage ==
        "Cannot drop bloom filter indices for the following non-existent column(s): "
          + unknownColumns.mkString(", "))
    }
    {
      val dataFilters = "filters"
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters)
      }
      assert(e.getErrorClass == "DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET")
      assert(e.getSqlState == "42613")

      val msg =
        "'replaceWhere' cannot be used with data filters when " +
          s"'dataChange' is set to false. Filters: ${dataFilters}"
      assert(e.getMessage == msg)
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.missingTableIdentifierException("read")
      }
      assert(e.getErrorClass == "DELTA_OPERATION_MISSING_PATH")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == "Please provide the path or table identifier for read.")
    }
    {
      val column = StructField("c0", IntegerType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cannotUseDataTypeForPartitionColumnError(column)
      }
      assert(e.getErrorClass == "DELTA_INVALID_PARTITION_COLUMN_TYPE")
      assert(e.getSqlState == "42996")
      assert(e.getMessage ==
        "Using column c0 of type IntegerType as a partition column is not supported.")
    }
    {
      val catalogPartitionSchema = StructType(Seq(StructField("a", IntegerType)))
      val userPartitionSchema = StructType(Seq(StructField("b", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedPartitionSchemaFromUserException(catalogPartitionSchema,
          userPartitionSchema)
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_PARTITION_SCHEMA_FROM_USER")
      assert(e.getSqlState == "KD009")
      assert(e.getMessage ==
        "CONVERT TO DELTA was called with a partition schema different from the partition " +
          "schema inferred from the catalog, please avoid providing the schema so that the " +
          "partition schema can be chosen from the catalog.\n" +
          s"\ncatalog partition schema:\n${catalogPartitionSchema.treeString}" +
          s"\nprovided partition schema:\n${userPartitionSchema.treeString}")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.invalidInterval("interval1")
      }
      assert(e.getErrorClass == "DELTA_INVALID_INTERVAL")
      assert(e.getSqlState == "22006")
      assert(e.getMessage == "interval1 is not a valid INTERVAL.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.cdcWriteNotAllowedInThisVersion
      }
      assert(e.getErrorClass == "DELTA_CHANGE_TABLE_FEED_DISABLED")
      assert(e.getSqlState == "42807")
      assert(e.getMessage == "Cannot write to table with delta.enableChangeDataFeed set. " +
        "Change data feed from Delta is not available.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.specifySchemaAtReadTimeException
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_SCHEMA_DURING_READ")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Delta does not support specifying the schema at read time.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.unexpectedDataChangeException("operation1")
      }
      assert(e.getErrorClass == "DELTA_DATA_CHANGE_FALSE")
      assert(e.getSqlState == "0AKDE")
      assert(e.getMessage == "Cannot change table metadata because the 'dataChange' option is " +
        "set to false. Attempted operation: 'operation1'.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.noStartVersionForCDC
      }
      assert(e.getErrorClass == "DELTA_NO_START_FOR_CDC_READ")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == "No startingVersion or startingTimestamp provided for CDC read.")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unrecognizedColumnChange("change1")
      }
      assert(e.getErrorClass == "DELTA_UNRECOGNIZED_COLUMN_CHANGE")
      assert(e.getSqlState == "42601")
      assert(e.getMessage ==
        "Unrecognized column change change1. You may be running an out-of-date Delta Lake version.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.endBeforeStartVersionInCDC(2, 1)
      }
      assert(e.getErrorClass == "DELTA_INVALID_CDC_RANGE")
      assert(e.getSqlState == "22003")
      assert(e.getMessage ==
        "CDC range from start 2 to end 1 was invalid. End cannot be before start.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unexpectedChangeFilesFound("a.parquet")
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_CHANGE_FILES_FOUND")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage ==
        """Change files found in a dataChange = false transaction. Files:
          |a.parquet""".stripMargin)
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.logFailedIntegrityCheck(2, "option1")
      }
      assert(e.getErrorClass == "DELTA_TXN_LOG_FAILED_INTEGRITY")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "The transaction log has failed integrity checks. Failed " +
        "verification at version 2 of:\noption1")
    }
    {
      val path = new Path("parent", "child")
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.checkpointNonExistTable(path)
      }
      assert(e.getErrorClass == "DELTA_CHECKPOINT_NON_EXIST_TABLE")
      assert(e.getSqlState == "42K03")
      assert(e.getMessage ==
        s"Cannot checkpoint a non-existing table $path. " +
          "Did you manually delete files in the _delta_log directory?")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.viewInDescribeDetailException(TableIdentifier("customer"))
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_DESCRIBE_DETAIL_VIEW")
      assert(e.getSqlState == "42809")
      assert(e.getMessage == "`customer` is a view. DESCRIBE DETAIL is only supported for tables.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.pathAlreadyExistsException(new Path(path))
      }
      assert(e.getErrorClass == "DELTA_PATH_EXISTS")
      assert(e.getSqlState == "42K04")
      assert(e.getMessage ==
        "Cannot write to already existent path /sample/path without setting OVERWRITE = 'true'.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw new DeltaAnalysisException(
          errorClass = "DELTA_MERGE_MISSING_WHEN",
          messageParameters = Array.empty
        )
      }
      assert(e.getErrorClass == "DELTA_MERGE_MISSING_WHEN")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == "There must be at least one WHEN clause in a MERGE statement.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unrecognizedFileAction("invalidAction", "invalidClass")
      }
      assert(e.getErrorClass == "DELTA_UNRECOGNIZED_FILE_ACTION")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Unrecognized file action invalidAction with type invalidClass.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.streamWriteNullTypeException
      }
      assert(e.getErrorClass == "DELTA_NULL_SCHEMA_IN_STREAMING_WRITE")
      assert(e.getSqlState == "42P18")
      assert(e.getMessage == "Delta doesn't accept NullTypes in the schema for streaming writes.")
    }
    {
      val expr = "1".expr
      val e = intercept[DeltaIllegalArgumentException] {
        throw new DeltaIllegalArgumentException(
          errorClass = "DELTA_UNEXPECTED_ACTION_EXPRESSION",
          messageParameters = Array(s"$expr"))
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_ACTION_EXPRESSION")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == s"Unexpected action expression $expr.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unexpectedAlias("alias1")
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_ALIAS")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Expected Alias but got alias1")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.unexpectedProject("project1")
      }
      assert(e.getErrorClass == "DELTA_UNEXPECTED_PROJECT")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "Expected Project but got project1")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nullableParentWithNotNullNestedField
      }
      assert(e.getErrorClass == "DELTA_NOT_NULL_NESTED_FIELD")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "A non-nullable nested field can't be added to a nullable parent. " +
        "Please set the nullability of the parent column accordingly.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.useAddConstraints
      }
      assert(e.getErrorClass == "DELTA_ADD_CONSTRAINTS")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage == "Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints.")
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.deltaSourceIgnoreChangesError(10, "removedFile", "tablePath")
      }
      assert(e.getErrorClass == "DELTA_SOURCE_TABLE_IGNORE_CHANGES")
      assert(e.getSqlState == "0A000")
      assert(e.getMessage ==
        "Detected a data update (for example removedFile) in the source table at version " +
          "10. This is currently not supported. If you'd like to ignore updates, set the " +
          "option 'skipChangeCommits' to 'true'. If you would like the data update to be reflected, " +
          "please restart this query with a fresh checkpoint directory. The source table can be " +
          "found at path tablePath.")
    }
    {
      val limit = "limit"
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.unknownReadLimit(limit)
      }
      assert(e.getErrorClass == "DELTA_UNKNOWN_READ_LIMIT")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == s"Unknown ReadLimit: $limit")
    }
    {
      val privilege = "unknown"
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unknownPrivilege(privilege)
      }
      assert(e.getErrorClass == "DELTA_UNKNOWN_PRIVILEGE")
      assert(e.getSqlState == "42601")
      assert(e.getMessage == s"Unknown privilege: $privilege")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.deltaLogAlreadyExistsException("path")
      }
      assert(e.getErrorClass == "DELTA_LOG_ALREADY_EXISTS")
      assert(e.getSqlState == "42K04")
      assert(e.getMessage == "A Delta log already exists at path")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.missingPartFilesException(10L, new FileNotFoundException("reason"))
      }
      assert(e.getErrorClass == "DELTA_MISSING_PART_FILES")
      assert(e.getSqlState == "42KD6")
      assert(e.getMessage == "Couldn't find all part files of the checkpoint version: 10")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.checkConstraintNotBoolean("name1", "expr1")
      }
      assert(e.getErrorClass == "DELTA_NON_BOOLEAN_CHECK_CONSTRAINT")
      assert(e.getSqlState == "42621")
      assert(e.getMessage == "CHECK constraint 'name1' (expr1) should be a boolean expression.")
    }
    {
      val e = intercept[DeltaIllegalStateException] {
        throw DeltaErrors.checkpointMismatchWithSnapshot
      }
      assert(e.getErrorClass == "DELTA_CHECKPOINT_SNAPSHOT_MISMATCH")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == "State of the checkpoint doesn't match that of the snapshot.")
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.notADeltaTableException("operation1")
      }
      assert(e.getErrorClass == "DELTA_ONLY_OPERATION")
      assert(e.getSqlState == "0AKDD")
      assert(e.getMessage == "operation1 is only supported for Delta tables.")
    }
    {
      val invalidStruct = StructField("invalid1", StringType)
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.dropNestedColumnsFromNonStructTypeException(invalidStruct)
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_DROP_NESTED_COLUMN_FROM_NON_STRUCT_TYPE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        "Can only drop nested columns from StructType. Found StructField(invalid1,StringType,true)")
    }
    {
      val columnsThatNeedRename = Set("c0", "c1")
      val schema = StructType(Seq(StructField("schema1", StringType)))
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.nestedFieldsNeedRename(columnsThatNeedRename, schema)
      }
      assert(e.getErrorClass == "DELTA_NESTED_FIELDS_NEED_RENAME")
      assert(e.getSqlState == "42K05")
      assert(e.getMessage ==
        "Nested fields need renaming to avoid data loss. Fields:\n[c0, c1].\n" +
          s"Original schema:\n${schema.treeString}")
    }
    {
      val locations = Seq("location1", "location2")
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cannotSetLocationMultipleTimes(locations)
      }
      assert(e.getErrorClass == "DELTA_CANNOT_SET_LOCATION_MULTIPLE_TIMES")
      assert(e.getSqlState == "XXKDS")
      assert(e.getMessage == s"Can't set location multiple times. Found ${locations}")
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
      assert(e.getErrorClass == "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE")
      assert(e.getSqlState == "42KD4")
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
      assert(e.getErrorClass == "DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE")
      assert(e.getSqlState == "42KD4")
      assert(e.readSchema == StructType.fromDDL("id int"))
      assert(e.incompatibleSchema == StructType.fromDDL("id2 int"))
      assert(!e.additionalProperties("detectedDuringStreaming").toBoolean)
    }
    {
      val e = intercept[DeltaUnsupportedOperationException] {
        throw DeltaErrors.blockColumnMappingAndCdcOperation(DeltaOperations.ManualUpdate)
      }
      assert(e.getErrorClass == "DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION")
      assert(e.getSqlState == "42KD4")
      assert(e.getMessage == "Operation \"Manual Update\" is not allowed when the table has " +
        "enabled change data feed (CDF) and has undergone schema changes using DROP COLUMN or " +
        "RENAME COLUMN.")
    }
    {
      val options = Map(
        "foo" -> "1",
        "bar" -> "2"
      )
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.unsupportedDeltaTableForPathHadoopConf(options)
      }
      assert(e.getErrorClass == "DELTA_TABLE_FOR_PATH_UNSUPPORTED_HADOOP_CONF")
      assert(e.getSqlState == "0AKDC")
      val prefixStr = DeltaTableUtils.validDeltaTableHadoopPrefixes.mkString("[", ",", "]")
      assert(e.getMessage == "Currently DeltaTable.forPath only supports hadoop configuration " +
        s"keys starting with $prefixStr but got ${options.mkString(",")}")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneOnRelativePath("path")
      }
      assert(e.getMessage ==
        """The target location for CLONE needs to be an absolute path or table name. Use an
          |absolute path instead of path.""".stripMargin)
    }
    {
      val e = intercept[AnalysisException] {
        throw DeltaErrors.cloneFromUnsupportedSource( "table-0", "CSV")
      }
      assert(e.getErrorClass == "DELTA_CLONE_UNSUPPORTED_SOURCE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage == "Unsupported clone source 'table-0', whose format is CSV.\n" +
        "The supported formats are 'delta', 'iceberg' and 'parquet'.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneReplaceUnsupported(TableIdentifier("customer"))
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_CLONE_REPLACE_SAME_TABLE")
      assert(e.getSqlState == "0AKDC")
      assert(e.getMessage ==
        s"""
           |You tried to REPLACE an existing table (`customer`) with CLONE. This operation is
           |unsupported. Try a different target for CLONE or delete the table at the current target.
           |""".stripMargin)

    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.cloneAmbiguousTarget("external-location", TableIdentifier("table1"))
      }
      assert(e.getErrorClass == "DELTA_CLONE_AMBIGUOUS_TARGET")
      assert(e.getSqlState == "42613")
      assert(e.getMessage ==
        s"""
           |Two paths were provided as the CLONE target so it is ambiguous which to use. An external
           |location for CLONE was provided at external-location at the same time as the path
           |`table1`.""".stripMargin)
    }
    {
      val e = intercept[AnalysisException] {
        DeltaTableValueFunctions.resolveChangesTableValueFunctions(
          spark, fnName = "dummy", args = Seq.empty)
      }
      assert(e.getErrorClass == "INCORRECT_NUMBER_OF_ARGUMENTS")
      assert(e.getMessage.contains(
        "not enough args, dummy requires at least 2 arguments and at most 3 arguments."))
    }
    {
      val e = intercept[AnalysisException] {
        DeltaTableValueFunctions.resolveChangesTableValueFunctions(
          spark, fnName = "dummy", args = Seq("1".expr, "2".expr, "3".expr, "4".expr, "5".expr))
      }
      assert(e.getErrorClass == "INCORRECT_NUMBER_OF_ARGUMENTS")
      assert(e.getMessage.contains(
        "too many args, dummy requires at least 2 arguments and at most 3 arguments."))
    }
    {
      val e = intercept[DeltaAnalysisException] {
        throw DeltaErrors.invalidTableValueFunction("invalid1")
      }
      assert(e.getErrorClass == "DELTA_INVALID_TABLE_VALUE_FUNCTION")
      assert(e.getSqlState == "22000")
      assert(e.getMessage ==
        "Function invalid1 is an unsupported table valued function for CDC reads.")
    }
  }
}

class DeltaErrorsSuite
  extends DeltaErrorsSuiteBase
