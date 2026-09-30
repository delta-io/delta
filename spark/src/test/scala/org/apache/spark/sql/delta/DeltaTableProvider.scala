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

/**
 * Indirection over the table format used by a test suite so that a single suite body can run
 * against different table formats.
 *
 * A suite mixes this in (directly or transitively) and uses [[tableProvider]] / [[writeFormat]]
 * instead of hard-coded `"delta"` literals in `USING <provider>` clauses and
 * `df.write.format(...)` calls. A variant of the suite can override these to target another
 * format. Because the defaults here are `"delta"`, mixing this into an existing Delta suite is
 * behavior-preserving.
 */
trait DeltaTableProvider {
  /** Provider name for `USING <provider>` in CREATE TABLE statements. Defaults to Delta. */
  protected def tableProvider: String = "delta"

  /**
   * Format name for `df.write.format(...)` / `spark.read.format(...)`. Defaults to
   * [[tableProvider]].
   */
  protected def writeFormat: String = tableProvider

  /**
   * Table properties applied to every table created via [[createTableSQL]]. Empty by default; a
   * variant can override this to add format-specific properties.
   */
  protected def defaultTableProperties: Map[String, String] = Map.empty

  /**
   * Build a `CREATE TABLE` statement for the table format under test. Merges
   * [[defaultTableProperties]] with any per-call `props` (the latter win on key conflicts).
   *
   * @param partitionBy passed through verbatim when non-empty, so it must already include its
   *                    keyword, e.g. `"PARTITIONED BY (p)"`.
   * @param clusterBy   passed through verbatim when non-empty, so it must already include its
   *                    keyword, e.g. `"CLUSTER BY (c)"`.
   * @param location    a bare path (no keyword); it is wrapped as `LOCATION '<location>'`.
   */
  protected def createTableSQL(
      name: String,
      schema: String,
      partitionBy: String = "",
      clusterBy: String = "",
      location: String = "",
      props: Map[String, String] = Map.empty): String = {
    val allProps = defaultTableProperties ++ props
    val propStr =
      if (allProps.isEmpty) ""
      else s"TBLPROPERTIES (${allProps.map { case (k, v) => s"'$k'='$v'" }.mkString(",")})"
    val locStr = if (location.isEmpty) "" else s"LOCATION '$location'"
    s"CREATE TABLE $name ($schema) USING $tableProvider $partitionBy $clusterBy $locStr $propStr"
  }
}
