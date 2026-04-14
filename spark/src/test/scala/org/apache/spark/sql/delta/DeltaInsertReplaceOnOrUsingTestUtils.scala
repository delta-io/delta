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

import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingStats
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

trait DeltaInsertReplaceOnOrUsingTestUtils
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  protected def createTable(
     tableName: String,
     tableCols: Seq[String],
     partCols: Seq[String] = Seq.empty,
     clusterCols: Seq[String] = Seq.empty): Unit = {
    assert(partCols.isEmpty || clusterCols.isEmpty,
      "Cannot specify both partitioning and clustering for a table.")
    sql(
      s"""
         |CREATE TABLE $tableName ${tableCols.mkString("(", ",", ")")}
         |USING delta
         |${if (partCols.nonEmpty) s"PARTITIONED BY ${partCols.mkString("(", ",", ")")}" else ""}
         |${if (clusterCols.nonEmpty) s"CLUSTER BY ${clusterCols.mkString("(", ",", ")")}" else ""}
         |""".stripMargin)
  }

  protected def insertValues(tableName: String, rows: Seq[String]): Unit = {
    sql(
      s"""
         |INSERT INTO $tableName
         |VALUES ${rows.mkString(", ")}
         |""".stripMargin)
  }

  protected def executeInsertReplaceOn(
      tableName: String,
      matchingCond: String,
      sourceQuery: String,
      byName: Boolean = false): Unit = {
    val byNameClause = if (byName) "BY NAME" else ""
    sql(
      s"""
         |INSERT INTO $tableName $byNameClause
         |REPLACE ON $matchingCond
         |$sourceQuery
         |""".stripMargin)
  }

  protected def getLastCommitNumAddedAndRemovedBytes(deltaLog: DeltaLog): (Long, Long) = {
    val changes = deltaLog.getChanges(deltaLog.update().version).flatMap(_._2).toSeq
    val addedBytes = changes.collect { case a: AddFile => a.size }.sum
    val removedBytes = changes.collect { case r: RemoveFile => r.getFileSize }.sum

    (addedBytes, removedBytes)
  }

  object DataLayoutType extends Enumeration {
    type DataLayoutType = Value
    val PARTITIONED, CLUSTERED, NORMAL = Value
  }
}
