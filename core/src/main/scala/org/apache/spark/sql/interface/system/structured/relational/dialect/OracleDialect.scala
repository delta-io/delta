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

package org.apache.spark.sql.interface.system.structured.relational.dialect

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp, Types}
import java.util.{Locale, TimeZone}

private case object OracleDialect extends JdbcDialect {
  private val BINARY_FLOAT = 100
  private val BINARY_DOUBLE = 101
  private val TIMESTAMPTZ = -101
  private val TIMESTAMPLTZ = -102
  private val INTERVAL_DAY_TO_SECOND = -104
  private val INTERVAL_YEAR_TO_MONTH = -103

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:oracle")

  private def supportTimeZoneTypes: Boolean = {
    val timeZone = DateTimeUtils.getTimeZone(SQLConf.get.sessionLocalTimeZone)
    timeZone == TimeZone.getDefault
  }

  override def getCatalystType(
                                sqlType: Int,
                                typeName: String,
                                size: Int,
                                md: MetadataBuilder):
  Option[DataType] = {
    sqlType match {
      case Types.NUMERIC => Some(StringType)
      case TIMESTAMPTZ if supportTimeZoneTypes
      => Some(TimestampType)
      case BINARY_FLOAT => Some(StringType)
      case BINARY_DOUBLE => Some(StringType)
      case INTERVAL_DAY_TO_SECOND => Some(StringType)
      case INTERVAL_YEAR_TO_MONTH => Some(StringType)
      case Types.ROWID => Some(StringType)
      case TIMESTAMPLTZ => Some(TimestampType)
      case _ => None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.BOOLEAN))
    case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
    case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
    case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
    case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
    case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
    case StringType => Some(JdbcType("LONGVARCHAR", java.sql.Types.LONGVARCHAR))
    case _ => None
  }

  override def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "{ts '" + timestampValue + "'}"
    case dateValue: Date => "{d '" + dateValue + "'}"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  override def getTruncateQuery(
                                 table: String,
                                 cascade: Option[Boolean] = isCascadingTruncateTable): String = {
    cascade match {
      case Some(true) => s"TRUNCATE TABLE $table CASCADE"
      case _ => s"TRUNCATE TABLE $table"
    }
  }
}
