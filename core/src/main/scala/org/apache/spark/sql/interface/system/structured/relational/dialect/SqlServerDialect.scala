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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util.Locale

private object SqlServerDialect extends JdbcDialect {

  private object SpecificTypes {
    val GEOMETRY = -157
    val GEOGRAPHY = -158
  }

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:sqlserver")

  override def getCatalystType(
                                sqlType: Int,
                                typeName: String,
                                size: Int,
                                md: MetadataBuilder):
  Option[DataType] = {
    if (typeName.contains("datetimeoffset")) {
      Option(StringType)
    } else {
      if (SQLConf.get.legacyMsSqlServerNumericMappingEnabled) {
        None
      } else {
        sqlType match {
          case java.sql.Types.SMALLINT => Some(ShortType)
          case java.sql.Types.REAL => Some(FloatType)
          case SpecificTypes.GEOMETRY | SpecificTypes.GEOGRAPHY => Some(BinaryType)
          case _ => None
        }
      }
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case StringType => Some(JdbcType("NVARCHAR(MAX)", java.sql.Types.NVARCHAR))
    case BooleanType => Some(JdbcType("BIT", java.sql.Types.BIT))
    case BinaryType => Some(JdbcType("VARBINARY(MAX)", java.sql.Types.VARBINARY))
    case ShortType if !SQLConf.get.legacyMsSqlServerNumericMappingEnabled =>
      Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case _ => None
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
