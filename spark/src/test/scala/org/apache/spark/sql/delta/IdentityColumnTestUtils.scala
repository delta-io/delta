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

import java.util.UUID

import org.apache.spark.sql.delta.GeneratedAsIdentityType.{GeneratedAlways, GeneratedAsIdentityType}
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait IdentityColumnTestUtils
  extends DDLTestUtils {

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(DeltaSQLConf.DELTA_IDENTITY_COLUMN_ENABLED.key, "true")
  }

  protected def getRandomTableName: String =
    s"identity_test_${UUID.randomUUID()}".replaceAll("-", "_")

  protected val unsupportedDataTypes: Seq[DataType] = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    DoubleType,
    DateType,
    TimestampType,
    StringType,
    BinaryType,
    DecimalType(precision = 5, scale = 2),
    YearMonthIntervalType(startField = 0, endField = 0) // Interval Year
  )

  def createTableWithIdColAndIntValueCol(
      tableName: String,
      generatedAsIdentityType: GeneratedAsIdentityType,
      startsWith: Option[Long],
      incrementBy: Option[Long],
      tblProperties: Map[String, String] = Map.empty): Unit = {
    createTable(
      tableName,
      Seq(
        IdentityColumnSpec(
          generatedAsIdentityType,
          startsWith,
          incrementBy
        ),
        TestColumnSpec(colName = "value", dataType = IntegerType)
      ),
      tblProperties = tblProperties
    )
  }

  /**
   * Creates and manages a simple identity column table with one other column "value" of type int
   */
  protected def withIdentityColumnTable(
     generatedAsIdentityType: GeneratedAsIdentityType,
     tableName: String)(f: => Unit): Unit = {
    withTable(tableName) {
      createTableWithIdColAndIntValueCol(tableName, generatedAsIdentityType, None, None)
      f
    }
  }

  protected def generateTableWithIdentityColumn(tableName: String, step: Long = 1): Unit = {
    createTableWithIdColAndIntValueCol(
      tableName,
      GeneratedAlways,
      startsWith = Some(0),
      incrementBy = Some(step)
    )

    // Insert numRows and make sure they assigned sequential IDs
    val numRows = 6
    for (i <- 0 until numRows) {
      sql(s"INSERT INTO $tableName (value) VALUES ($i)")
    }
    val expectedAnswer = for (i <- 0 until numRows) yield Row(i * step, i)
    checkAnswer(sql(s"SELECT * FROM $tableName ORDER BY value ASC"), expectedAnswer)
  }


  /**
   * Retrieves the high watermark information for the given `colName` in the metadata of
   * given `snapshot`, if it's present. Returns None if the high watermark has not been set yet.
   */
  protected def getHighWaterMark(snapshot: Snapshot, colName: String): Option[Long] = {
    val metadata = snapshot.schema(colName).metadata
    if (metadata.contains(DeltaSourceUtils.IDENTITY_INFO_HIGHWATERMARK)) {
      Some(metadata.getLong(DeltaSourceUtils.IDENTITY_INFO_HIGHWATERMARK))
    } else {
      None
    }
  }

  /**
   * Retrieves the high watermark information for the given `colName` in the metadata of
   * given `snapshot`
   */
  protected def highWaterMark(snapshot: Snapshot, colName: String): Long = {
    getHighWaterMark(snapshot, colName).get
  }

  /**
   * Helper function to validate values of IDENTITY column `id` in table `tableName`. Returns the
   * new high water mark. We use minValue and maxValue to filter column `value` to get the set of
   * values we are checking in this batch.
   */
  protected def validateIdentity(
      tableName: String,
      expectedRowCount: Long,
      start: Long,
      step: Long,
      minValue: Long,
      maxValue: Long,
      oldHighWaterMark: Long): Long = {
    // Check row count.
    checkAnswer(
      sql(s"SELECT COUNT(*) FROM $tableName"),
      Row(expectedRowCount)
    )
    // Check values are unique.
    checkAnswer(
      sql(s"SELECT COUNT(DISTINCT id) FROM $tableName"),
      Row(expectedRowCount)
    )
    // Check values follow start and step configuration.
    checkAnswer(
      sql(s"SELECT COUNT(*) FROM $tableName WHERE (id - $start) % $step != 0"),
      Row(0)
    )
    // Check values generated in this batch are after previous high water mark.
    checkAnswer(
      sql(
        s"""
           |SELECT COUNT(*) FROM $tableName
           |  WHERE (value BETWEEN $minValue and $maxValue)
           |    AND ((id - $oldHighWaterMark) / $step < 0)
           |""".stripMargin),
      Row(0)
    )
    // Update high water mark.
    val func = if (step > 0) "MAX" else "MIN"
    sql(s"SELECT $func(id) FROM $tableName").collect().head.getLong(0)
  }

  /**
   * Helper function to validate generated identity values in sortedRows.
   *
   * @param sortedRows rows of the table sorted by id
   * @param start start value of the identity column
   * @param step step value of the identity column
   * @param expectedLowerBound expected lower bound of the generated values
   * @param expectedUpperBound expected upper bound of the generated values
   * @param expectedDistinctCount expected distinct count of the generated values
   */
  protected def checkGeneratedIdentityValues(
      sortedRows: Seq[IdentityColumnTestTableRow],
      start: Long,
      step: Long,
      expectedLowerBound: Long,
      expectedUpperBound: Long,
      expectedDistinctCount: Long): Unit = {
    assert(sortedRows.head.id >= expectedLowerBound)
    for (row <- sortedRows) {
      assert((row.id - start) % step === 0)
    }
    assert(sortedRows.last.id <= expectedUpperBound)
    assert(sortedRows.map(_.id).distinct.size === expectedDistinctCount)
  }
}

