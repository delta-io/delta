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

package io.delta.tables.shared

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Shared base for the table refresh, version pinning, and schema change detection
 * test traits. This single source file is compiled into BOTH the classic `spark`
 * module and the `spark-connect/client` module (wired via
 * [[Test / unmanagedSourceDirectories]] in build.sbt). To type-check under both
 * classic Spark and Spark Connect, it self-types only to [[AnyFunSuite]] and uses
 * only the unified [[org.apache.spark.sql]] API. Everything that differs between the
 * two execution modes is declared here as an abstract hook and implemented by the
 * per-module base traits:
 *   - classic: [[org.apache.spark.sql.delta.DeltaTableRefreshTestBase]]
 *   - connect: [[io.delta.tables.DeltaTableRefreshConnectTestBase]]
 *
 * The shared category traits mixed on top of this base are:
 *   - [[DeltaTempViewRefreshTests]] (Section [1])
 *   - [[DeltaRepeatedAccessRefreshTests]] (Section [2])
 *   - [[DeltaJoinRefreshTests]] (Section [3])
 *   - [[DeltaCacheTableRefreshTests]] (Section [5])
 *
 * Section [4] (Dataset pinning vs reanalysis) is intentionally NOT shared because
 * classic and connect test fundamentally opposite behaviors there.
 */
trait DeltaTableRefreshSharedBase { self: AnyFunSuite =>

  /** True in the Connect implementation, false in classic. Branches expectations. */
  def isConnect: Boolean

  /**
   * Spark 4.2+ fixes AMBIGUOUS_COLUMN_OR_FIELD for self-joins without aliases.
   * Only read inside connect branches; classic leaves the default.
   */
  protected def ambiguousColumnFixed: Boolean = false

  /** The V2 enable mode (NONE, AUTO, STRICT). Overridden by classic subclasses. */
  protected def v2EnableMode: String = "NONE"

  /** The active session. Classic and connect return their own concrete subtype. */
  protected def spark: SparkSession

  // Verification helpers, satisfied by QueryTest (classic) / DeltaQueryTest (connect).
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit
  protected def withTable(tableNames: String*)(f: => Unit): Unit

  // Error assertions. Classic implements with Spark checkError(parameters, matchPVals)
  // and DeltaAnalysisException; connect implements with its 2 arg substring checkError.
  protected def assertSchemaChangeError(f: => Unit): Unit
  protected def assertAmbiguousColumnError(f: => Unit): Unit
  protected def assertArityMismatchError(f: => Unit): Unit

  // Table setup helpers, already present and identically named on both base traits.
  protected def createSimpleTable(tableName: String): Unit
  protected def createColumnMappingTable(tableName: String): Unit
  protected def insertInitialData(tableName: String): Unit
  protected def writerSql(sqlText: String): Unit

  // External modification abstraction. Classic writes commits directly via LogStore
  // using DeltaLog and catalyst Metadata; connect writes commit JSON to the filesystem.
  // [[withRefreshTable]] sets up an isolated table and yields the SQL table reference to
  // use ("t" classic, a delta path identifier in connect) so the body can run CREATE,
  // INSERT, SELECT, and the external write hooks can locate the table.
  protected def withRefreshTable(body: String => Unit): Unit
  protected def externalDataWrite(tableRef: String, rows: Seq[(Int, Int)]): Unit
  /** Writes 3 column data to a table that already has the 3 column schema (no metadata change). */
  protected def externalDataWriteWide(tableRef: String, rows: Seq[(Int, Int, Int)]): Unit
  protected def externalAddColumnAndWrite(tableRef: String, rows: Seq[(Int, Int, Int)]): Unit
  protected def externalDropColumn(tableRef: String, column: String): Unit
  protected def externalDropAndRecreate(tableRef: String, columnMapping: Boolean): Unit
  protected def externalReplaceColumn(
      tableRef: String, column: String, newType: Option[String]): Unit

  /**
   * Asserts the body fails the way classic STRICT mode does for external writes
   * (a version conflict surfaced as [[java.nio.file.FileAlreadyExistsException]]).
   * Only invoked from classic STRICT branches.
   */
  protected def assertExternalStrictConflict(f: => Unit): Unit
}
