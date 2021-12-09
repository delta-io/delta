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

package org.apache.spark.sql.interface.system.semistructured.hbase.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.interface.system.structured.relational.dialect.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.interface.system.structured.relational.util.JdbcUtils.{createConnectionFactory, savePartition}
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.interface.system.structured.relational.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.types.StructType

import java.sql.Connection

/**
 * provide some methods which are different from JdbcUtils
 */
object HBaseUtils extends Logging {

  /**
   *  phoenix doesn't support Truncate, so use 'delete' instead
   */
  def deleteTable(conn: Connection, table: String, options: JDBCOptions): Unit = {
    val statement = conn.createStatement
    try {
      statement.setQueryTimeout(options.queryTimeout)
      statement.executeUpdate(s"DELETE FROM $table")
    } finally {
      statement.close()
    }
  }

  def saveTable(
                 df: DataFrame,
                 tableSchema: Option[StructType],
                 isCaseSensitive: Boolean,
                 options: JdbcOptionsInWrite): Unit = {
    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val getConnection = createConnectionFactory(options)
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel

    val insertStmt = getInsertStatement(table, rddSchema, tableSchema, isCaseSensitive, dialect)
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }
    repartitionedDF.rdd.foreachPartition { iterator =>
      savePartition(
        getConnection, table, iterator, rddSchema, insertStmt, batchSize, dialect, isolationLevel,
        options)
    }
  }

  /**
   *  phoenix use 'upsert', not 'insert'
   * @param table
   * @param rddSchema
   * @param tableSchema
   * @param isCaseSensitive
   * @param dialect
   * @return
   */
  def getInsertStatement(
                          table: String,
                          rddSchema: StructType,
                          tableSchema: Option[StructType],
                          isCaseSensitive: Boolean,
                          dialect: JdbcDialect): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    } else {
      val columnNameEquality = if (isCaseSensitive) {
        org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
      } else {
        org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      }
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName = tableColumnNames.find(f => columnNameEquality(f, col.name)).getOrElse {
          throw new AnalysisException(s"""Column "${col.name}" not found in schema $tableSchema""")
        }
        dialect.quoteIdentifier(normalizedName)
      }.mkString(",")
    }
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    s"UPSERT INTO $table ($columns) VALUES ($placeholders)"
  }


}
