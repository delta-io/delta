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

import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.constraints.Constraints.NotNull
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.hadoop.fs.Path
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

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
    "concurrentModificationException" -> new ConcurrentWriteException(None))

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
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.copyIntoEncryptionSseCRequired()
      }
      assert(e.getMessage == "Invalid encryption type. COPY INTO source encryption " +
        "must specify 'TYPE' = 'AWS_SSE_C'.")
    }
    {
      val e = intercept[DeltaIllegalArgumentException] {
        throw DeltaErrors.copyIntoEncryptionMasterKeyRequired()
      }
      assert(e.getMessage == "Invalid encryption arguments. COPY INTO source encryption " +
        "must specify a MASTER_KEY.")
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
        "column mapping, as external readers cannot read these Delta tables. See Databricks " +
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
  }
}

class DeltaErrorsSuite
  extends DeltaErrorsSuiteBase
