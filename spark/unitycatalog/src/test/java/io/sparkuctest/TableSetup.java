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

package io.sparkuctest;

import org.apache.spark.sql.SparkSession;

/**
 * Defines how to create and populate a Delta table for streaming test verification.
 *
 * <p>Each implementation represents one row in the "Delta Table Combination for Streaming" test
 * matrix. The framework creates the table via {@code withNewTable()} using {@link #schema()}, then
 * calls {@link #setUp} to populate it. For incremental mode, {@link #addIncrementalData} is called
 * between streaming micro-batch checkpoints.
 */
public interface TableSetup {

  /** Human-readable name for test output (e.g., "SimpleCreateTable"). */
  String name();

  /** Table schema DDL (e.g., "id INT, value STRING"). */
  String schema();

  /**
   * Populate the table with initial data and/or perform mutations. The table already exists
   * (created by the framework via {@code withNewTable}).
   */
  void setUp(SparkSession spark, String tableName);

  /**
   * Add more data mid-stream for incremental mode. Called between {@code processAllAvailable()}
   * calls. Only invoked when {@link #incrementalRounds()} > 0.
   *
   * @param round 1-based round number
   */
  /** Optional partition columns (e.g., "part_col"). Return null for no partitioning. */
  default String partitionColumns() {
    return null;
  }

  /** Optional table properties (e.g., "'key'='value'"). Return null for defaults. */
  default String tableProperties() {
    return null;
  }

  default void addIncrementalData(SparkSession spark, String tableName, int round) {}

  /** Number of incremental data rounds. Return 0 for snapshot-only setups. */
  default int incrementalRounds() {
    return 0;
  }
}
