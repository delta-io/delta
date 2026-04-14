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
 * Tests streaming from a partitioned table. Data spans multiple partitions to verify streaming
 * correctly reads across partition boundaries.
 */
public class CreateWithPartitions implements TableSetup {

  @Override
  public String name() {
    return "CreateWithPartitions";
  }

  @Override
  public String schema() {
    return "id INT, value STRING, part STRING";
  }

  @Override
  public String partitionColumns() {
    return "part";
  }

  @Override
  public void setUp(SparkSession spark, String tableName) {
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'x')", tableName));
  }

  @Override
  public void addIncrementalData(SparkSession spark, String tableName, int round) {
    spark.sql(String.format("INSERT INTO %s VALUES (4, 'd', 'y'), (5, 'e', 'z')", tableName));
  }

  @Override
  public int incrementalRounds() {
    return 1;
  }
}
