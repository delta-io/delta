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

package io.delta.flink.inttest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class IntTestBase {

  protected final URI catalogEndpoint;
  protected final String catalogToken;
  protected final SparkSession spark;

  public IntTestBase(SparkSession spark, URI catalogEndpoint, String catalogToken) {
    this.spark = spark;
    this.catalogEndpoint = catalogEndpoint;
    this.catalogToken = catalogToken;
  }

  protected void assertCount(long expected, Dataset<Row> dataset) {
    assertEquals(expected, dataset.collectAsList().get(0).getLong(0));
  }
}
