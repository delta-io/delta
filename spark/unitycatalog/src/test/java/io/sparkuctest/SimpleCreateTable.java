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
 * Minimal {@link TableSetup} for framework validation. Inserts three rows, then adds two more in
 * one incremental round.
 */
public class SimpleCreateTable implements TableSetup {

  @Override
  public String name() {
    return "SimpleCreateTable";
  }

  @Override
  public String schema() {
    return "id INT, value STRING";
  }

  @Override
  public void setUp(SparkSession spark, String tableName) {
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName));
  }

  @Override
  public void addIncrementalData(SparkSession spark, String tableName, int round) {
    spark.sql(String.format("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", tableName));
  }

  @Override
  public int incrementalRounds() {
    return 1;
  }
}
