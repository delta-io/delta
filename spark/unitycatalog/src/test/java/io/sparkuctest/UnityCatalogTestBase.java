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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Preconditions;
import io.sparkuctest.extension.UnityCatalogExtension;
import io.sparkuctest.extension.UnityCatalogExtensionUtil;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;

public class UnityCatalogTestBase {

  @RegisterExtension
  public static UnityCatalogExtension UC_EXTENSION = UnityCatalogExtensionUtil.initialize();

  private static SparkSession spark;

  @BeforeAll
  public static void beforeAll() {
    SparkConf conf = new SparkConf()
        .setAppName("UnityCatalog Support Tests")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.shuffle.partitions", "5")
        // Delta Lake required configurations
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    // Configure with Unity Catalog
    UC_EXTENSION.catalogSparkConf().forEach(conf::set);

    // Build the spark session.
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public static void afterAll() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  protected String fullTableName(String tableName) {
    return String.format("`%s`.`%s`.`%s`",
        UC_EXTENSION.catalogName(), UC_EXTENSION.schemaName(), tableName);
  }

  protected String path(String basename) {
    Preconditions.checkNotNull(basename, "basename cannot be null");
    if (basename.startsWith("/")) {
      return String.format("%s%s", UC_EXTENSION.rootTestingDir(), basename);
    } else {
      return String.format("%s/%s", UC_EXTENSION.rootTestingDir(), basename);
    }
  }

  public static List<Row> sql(String statement, Object... args) {
    return spark.sql(String.format(statement, args)).collectAsList();
  }

  public static Object[] row(Object... args) {
    return args;
  }

  public static void assertEquals(String context, List<Object[]> expected, List<Row> actual) {
    assertThat(expected)
        .as("%s: number of results should match", context)
        .hasSameSizeAs(actual);

    for (int row = 0; row < expected.size(); row += 1) {
      Object[] expectedRow = expected.get(row);
      Row actualRow = actual.get(row);

      assertEquals(context + ": row " + (row + 1), expectedRow, actualRow);
    }
  }

  public static void assertEquals(String context, Object[] expected, Row actual) {
    assertThat(expected.length)
        .as("%s: Number of columns should match", context)
        .isEqualTo(actual.size());
    for (int i = 0; i < expected.length; i += 1) {
      assertThat(expected[i])
          .as("%s: Element does not match", context)
          .isEqualTo(actual.get(i));
    }
  }
}
