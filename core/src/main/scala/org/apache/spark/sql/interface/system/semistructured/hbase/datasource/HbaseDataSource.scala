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

package org.apache.spark.sql.interface.system.semistructured.hbase.datasource

import org.apache.phoenix.query.QueryServices
import org.apache.spark.sql._
import org.apache.spark.sql.interface.system.semistructured.hbase.util.HBaseUtils._
import org.apache.spark.sql.interface.system.structured.relational.util.JdbcUtils
import org.apache.spark.sql.interface.system.structured.relational.util.JdbcUtils.createTable

import org.apache.spark.sql.interface.system.structured.relational.{JDBCOptions, JDBCRelation, JdbcOptionsInWrite}
import org.apache.spark.sql.interface.system.util.{MultiSourceException, PropertiesUtil}
import org.apache.spark.sql.sources._


/**
 * This datasource is used to handle hbase via phoenix,
 * which is a jdbc driver used to connect with hbase
 */
class HbaseDataSource
 extends CreatableRelationProvider
with RelationProvider with DataSourceRegister {

  override def shortName(): String = "hbase"

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val params = initParams(sqlContext, parameters)
    val jdbcOptions = new JDBCOptions(params)
    val resolver = sqlContext.conf.resolver
    val timeZoneId = sqlContext.conf.sessionLocalTimeZone
    val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
    val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {
    val params = initParams(sqlContext, parameters)
    val options = new JdbcOptionsInWrite(params)
    val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis

    val conn = JdbcUtils.createConnectionFactory(options)()
    try {
      val tableExists = JdbcUtils.tableExists(conn, options)
      if (tableExists) {
        mode match {
          case SaveMode.Overwrite =>
            deleteTable(conn, options.table, options)
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)
          case SaveMode.Append =>
            val tableSchema = JdbcUtils.getSchemaOption(conn, options)
            saveTable(df, tableSchema, isCaseSensitive, options)

          case SaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '${options.table}' already exists. " +
                s"SaveMode: ErrorIfExists.")

          case SaveMode.Ignore =>
        }
      } else {
        createTable(conn, options.table, df.schema, isCaseSensitive, options)
        saveTable(df, Some(df.schema), isCaseSensitive, options)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }

  def initParams(sqlContext: SQLContext,
                 parameters: Map[String, String]): Map[String, String] = {
    var params = Map[String, String]()
    val database = parameters.get("database").get
    val table = parameters.get("table").get

    var maxSize = PropertiesUtil.chooseProperties("hbase",
      database + ".phoenix.mutate.maxSize")
    if(maxSize == null) {
      // default value
      maxSize = "500000"
    }
    var immutableRows = PropertiesUtil.chooseProperties("hbase",
      database + ".phoenix.mutate.immutableRows")
    if(immutableRows == null) {
      // default value
      immutableRows = "500000"
    }

    params = Map[String, String](
      "url" -> PropertiesUtil.chooseProperties("hbase", database + ".url"),
      "driver" -> PropertiesUtil.chooseProperties("hbase", database + ".driver"),
      "dbtable" -> table,
      QueryServices.MAX_MUTATION_SIZE_ATTRIB -> maxSize,
      QueryServices.IMMUTABLE_ROWS_ATTRIB -> immutableRows
    )
    if( params.get("url") == Some(null) ||
      params.get("driver") == Some(null) ) {
      throw new MultiSourceException(
        s" the database configuration infomation may worng ! "
      )
    }

    params
  }

}
