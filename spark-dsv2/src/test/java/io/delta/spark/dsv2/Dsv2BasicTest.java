/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.dsv2;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class Dsv2BasicTest {

  private final SparkSession spark =
      SparkSession.builder()
          .master("local[*]")
          .config(
              new SparkConf()
                  .set("spark.sql.catalog.dsv2", "io.delta.spark.dsv2.catalog.TestCatalog"))
          .getOrCreate();

  @Test
  public void loadTableTest() {
    Exception exception =
        assertThrows(
            Exception.class, () -> spark.sql("select * from dsv2.test_namespace.test_table"));

    assertTrue(exception instanceof UnsupportedOperationException);
    assertTrue(exception.getMessage().contains("loadTable method is not implemented"));
  }
}
