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
}

