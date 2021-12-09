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

package org.apache.spark.sql.interface.system.structured.relational.datasource

import org.apache.spark.sql.interface.system.structured.relational.util.JdbcUtils
import org.apache.spark.sql.interface.system.structured.relational.util.JdbcUtils.{createTable, saveTable, truncateTable}
import org.apache.spark.sql.interface.system.structured.relational.{JDBCOptions, JDBCRelation, JdbcOptionsInWrite}

import org.apache.spark.sql._
import org.apache.spark.sql.interface.system.util.{MultiSourceException, PropertiesUtil}
import org.apache.spark.sql.sources._

/**
 * this datasource is used to handle relational database. such as MySQL, Oracle, SQL Server
 */
class RelationDBDataSource
  extends RelationProvider
    with CreatableRelationProvider
    with DataSourceRegister {

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {
    try {
      val params = initParams(sqlContext, parameters)

      val options = new JdbcOptionsInWrite(params)
      val isCaseSensitive = sqlContext.conf.caseSensitiveAnalysis
      // create the connection
      val conn = JdbcUtils.createConnectionFactory(options)()
      try {
        val tableExists = JdbcUtils.tableExists(conn, options)
        if (tableExists) {
          mode match {
            case SaveMode.Overwrite =>
              // clear data firstly, then insert data
              truncateTable(conn, options)
              saveTable(df, Some(df.schema), isCaseSensitive, options)
            case SaveMode.Append =>
              // get existed table schema first, then insert data
              val tableSchema = JdbcUtils.getSchemaOption(conn, options)
              saveTable(df, tableSchema, isCaseSensitive, options)

            case SaveMode.ErrorIfExists =>
              throw new AnalysisException(
                s"table '${options.table}' alraedy exist. " +
                  s"SaveMode: ErrorIfExists.")

            case SaveMode.Ignore =>
          }
        } else { // if table not existed, then create the table first
          createTable(conn, options.table, df.schema, isCaseSensitive, options)
          saveTable(df, Some(df.schema), isCaseSensitive, options)
        }
      } finally {
        conn.close()
      }

      createRelation(sqlContext, parameters)
    } catch {
      case multiSourceException: MultiSourceException =>
        throw  multiSourceException
      case exception: Exception =>
        throw exception
    }
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    try {
      val params = initParams(sqlContext, parameters)

      val jdbcOptions = new JDBCOptions(params)
      val resolver = sqlContext.conf.resolver
      val timeZoneId = sqlContext.conf.sessionLocalTimeZone
      // get schema
      val schema = JDBCRelation.getSchema(resolver, jdbcOptions)
      val parts = JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
      JDBCRelation(schema, parts, jdbcOptions)(sqlContext.sparkSession)
    } catch {
      case multiSourceException: MultiSourceException =>
        throw  multiSourceException
      case exception: Exception =>
        throw exception
    }
  }

  override def shortName(): String = {
    "relationaldb"
  }

  /**
   * initialize the parameters (read information from configuration file)
   * @param sqlContext
   * @param parameters
   * @return
   */
  def initParams(sqlContext: SQLContext,
                 parameters: Map[String, String]): Map[String, String] = {
    var params = Map[String, String]()
    val database = parameters.get("database").get
    val table = parameters.get("table").get

    params = Map[String, String](
      "url" -> PropertiesUtil.chooseProperties("relationaldb", database + ".url"),
      "driver" -> PropertiesUtil.chooseProperties("relationaldb", database + ".driver"),
      "user" -> PropertiesUtil.chooseProperties("relationaldb", database + ".user"),
      "password" -> PropertiesUtil.chooseProperties("relationaldb", database + ".password"),
      "dbtable" -> table
    )
    if( params.get("url") == Some(null) ||
      params.get("driver") == Some(null) ||
      params.get("user") == Some(null) ||
      params.get("password") == Some(null) ) {
      throw new MultiSourceException(
        s" the database configuration infomation may worng ! "
      )
    }

    params
  }

}
