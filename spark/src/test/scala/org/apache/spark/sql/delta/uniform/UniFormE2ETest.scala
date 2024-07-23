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

package org.apache.spark.sql.delta.uniform

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkSessionSwitch
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{ColumnMappingTableFeature, DeltaLog, FeatureAutomaticallyEnabledByMetadata, IcebergCompatV1TableFeature, IcebergCompatV2TableFeature, UniversalFormat, WriterFeature}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.test.SharedSparkSession

import java.util.UUID

/**
 * Base classes for all UniForm end-to-end test cases. Provides support to
 * write data with one SparkSession and read data from another for verification.
 *
 * People who need to write a new test suite should extend this class and
 * implement their test cases with [[write]] and [[read]]/[[readAndVerify]], which execute
 * with the writer session and reader session respectively.
 *
 * Implementing classes need to correctly set up the reader and writer environments.
 * See [[UniFormE2EIcebergSuiteBase]] for existing examples.
 */
trait UniFormE2ETest
  extends QueryTest
  with SharedSparkSession
  with SparkSessionSwitch {

  private var _readerSparkSession: Option[SparkSession] = None

  val compatVersion: Int = -1

  val allCompatVersions: Seq[Int] = Seq(1, 2)

  val allCompatTFs: Seq[WriterFeature with FeatureAutomaticallyEnabledByMetadata] =
    Seq(IcebergCompatV1TableFeature, IcebergCompatV2TableFeature)

  /**
   * Executes `f` with params (tableId, tempPath).
   *
   * We want to use a temp directory in addition to a unique temp table so that when the async
   * iceberg conversion runs and completes, the parent folder is still removed.
   */
  protected def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { dir =>
      val tablePath = new Path(dir.toString, "table")

      withTable(tableId) {
        f(tableId, s"'$tablePath'")
      }
    }
  }

  /**
   * Assert the protocol and properties for the specific iceberg compatible version.
   *
   * @param tableId the table to be checked.
   * @param compatVersion the specific iceberg compatible version to check.
   */
  protected def assertIcebergCompatProtocolAndProperties(
       tableId: String,
       compatVersion: Int = compatVersion): Unit = {
    assert(allCompatVersions.contains(compatVersion))

    val snapshot = DeltaLog.forTable(spark, new TableIdentifier(tableId)).update()
    val protocol = snapshot.protocol
    val tblProperties = snapshot.getProperties
    val tableFeature = allCompatTFs(compatVersion - 1)

    val expectedMinReaderVersion = Math.max(
      ColumnMappingTableFeature.minReaderVersion,
      tableFeature.minReaderVersion
    )

    val expectedMinWriterVersion = Math.max(
      ColumnMappingTableFeature.minWriterVersion,
      tableFeature.minWriterVersion
    )

    assert(protocol.minReaderVersion >= expectedMinReaderVersion)
    assert(protocol.minWriterVersion >= expectedMinWriterVersion)
    assert(protocol.writerFeatures.get.contains(tableFeature.name))
    assert(tblProperties(s"delta.enableIcebergCompatV$compatVersion") === "true")
    assert(Seq("name", "id").contains(tblProperties("delta.columnMapping.mode")))
  }

  /**
   * Assert UniForm Iceberg related protocol and properties.
   *
   * @param tableId the table to be checked.
   * @param compatVersion the specific iceberg compatible version to check.
   */
  protected def assertUniFormIcebergProtocolAndProperties(
      tableId: String,
      compatVersion: Int = compatVersion): Unit = {
    assertIcebergCompatProtocolAndProperties(tableId, compatVersion)

    val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableId)).update()
    assert(UniversalFormat.icebergEnabled(snapshot.metadata))
  }

  /**
   * Execute write operations through the writer SparkSession
   *
   * @param sqlText write query to the UniForm table
   */
  protected def write(sqlText: String): Unit = spark.sql(sqlText)

  /**
   * Execute a sql with reader SparkSession and return the result.
   * NOTE.
   * 1. The caller should use the correct table name. See [[tableNameForRead]]
   * 2. We eagerly collect the results because we will switch back to the
   *    writer session after read.
   * @param sqlText the read query against the UniForm table
   * @return the read result
   */
  protected def read(sqlText: String): Array[Row] = {
    withSession(readerSparkSession) { session =>
      session.sql(sqlText).collect()
    }
  }

  /**
   * Verify the result by reading from the reader session and compare the result to the expected.
   *
   * @param table  write table name
   * @param fields fields to verify, separated by comma. E.g., "col1, col2"
   * @param orderBy fields to order the results, separated by comma.
   * @param expect expected result
   */
  protected def readAndVerify(
      table: String, fields: String, orderBy: String, expect: Seq[Row]): Unit = {
    val translated = tableNameForRead(table)
    withSession(readerSparkSession) { session =>
      checkAnswer(session.sql(s"SELECT $fields FROM $translated ORDER BY $orderBy"), expect)
    }
  }

  protected def readerSparkSession: SparkSession = {
    if (_readerSparkSession.isEmpty) {
      // call to newSession makes sure
      // [[SparkSession.getOrCreate]] gives a new session
      // and [[SparkContext.getOrCreate]] uses a new context
      _readerSparkSession = Some(newSession(createReaderSparkSession))
    }
    _readerSparkSession.get
  }

  /**
   * Child classes should extend this to create reader SparkSession.
   * @return sparkSession for reading data and verify result.
   */
  protected def createReaderSparkSession: SparkSession

  /**
   * Subclasses should override this method when the table name for reading
   * is different from the table name used for writing. For example, when we
   * write a table using the name `table1`, and then read it from another catalog
   * `catalog_read`, this method should return `catalog_read.default.table1`
   * for the input `table1`.
   *
   * @param tableName table name for writing (name only)
   * @return table name for reading, default is no translation
   */
  protected def tableNameForRead(tableName: String): String = tableName
}
