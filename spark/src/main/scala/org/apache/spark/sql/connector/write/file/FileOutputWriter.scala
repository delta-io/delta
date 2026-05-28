/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.connector.write.file

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.SparkPlan

/**
 * Driver-side companion to a [[FileWrite]]. Owns the lifecycle of one write: open a connector
 * transaction, drive `FileFormatWriter.write` against the input `SparkPlan`, and commit (or
 * abort on failure).
 *
 * The exec node passes its already-projected child `SparkPlan` (constraints applied, generated
 * columns injected, partition columns ordered) directly here - no DataFrame/LogicalRDD wrap.
 * That keeps the path that Photon's plan-replacement rule needs to match clean.
 */
trait FileOutputWriter {
  /**
   * Drive the write to completion. Returns metrics summarizing what was committed.
   *
   * @param query   The already-projected physical plan whose rows should be written.
   * @param mode    The semantic write mode (Append / Overwrite). Other modes are
   *                connector-specific.
   */
  def write(query: SparkPlan, mode: SaveMode): WriteResult
}
