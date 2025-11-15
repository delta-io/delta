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

package org.apache.spark.sql.delta.catalog;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.spark.table.SparkTable;
import java.io.File;
import java.util.stream.Stream;
import org.apache.spark.sql.delta.DeltaDsv2EnableConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DeltaCatalogTest {

  private static SparkSession spark;

  @BeforeAll
  public static void setUpSpark() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("DeltaCatalogTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
  }

  @AfterAll
  public static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @AfterEach
  public void resetConfig() {
    spark.conf().unset(DeltaDsv2EnableConf.DATASOURCEV2_ENABLE_MODE.key());
  }

  static Stream<Arguments> modeTestCases() {
    return Stream.of(
        Arguments.of("STRICT", SparkTable.class, "Kernel SparkTable"),
        Arguments.of("AUTO", DeltaTableV2.class, "DeltaTableV2"),
        Arguments.of("NONE", DeltaTableV2.class, "DeltaTableV2"));
  }

  @ParameterizedTest(name = "mode={0} returns {2}")
  @MethodSource("modeTestCases")
  public void testCatalogBasedTableByMode(
      String mode, Class<?> expectedClass, String expectedClassName, @TempDir File tempDir) {
    String tableName = "test_catalog_" + mode.toLowerCase();
    String location = new File(tempDir, tableName).getAbsolutePath();

    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'",
            tableName, location));

    spark.conf().set(DeltaDsv2EnableConf.DATASOURCEV2_ENABLE_MODE.key(), mode);
    DeltaCatalog catalog =
        (DeltaCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
    Identifier ident = Identifier.of(new String[] {"default"}, tableName);
    Table table = catalog.loadTable(ident);

    assertEquals(
        expectedClass,
        table.getClass(),
        String.format("Mode %s should return %s", mode, expectedClassName));
  }

  @ParameterizedTest(name = "mode={0} returns {2} for path-based table")
  @MethodSource("modeTestCases")
  public void testPathBasedTableByMode(
      String mode, Class<?> expectedClass, String expectedClassName, @TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    spark.sql(String.format("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", path));

    spark.conf().set(DeltaDsv2EnableConf.DATASOURCEV2_ENABLE_MODE.key(), mode);
    DeltaCatalog catalog =
        (DeltaCatalog) spark.sessionState().catalogManager().v2SessionCatalog();
    Identifier ident = Identifier.of(new String[] {"delta"}, path);
    Table table = catalog.loadTable(ident);

    assertEquals(
        expectedClass,
        table.getClass(),
        String.format("Mode %s should return %s for path-based table", mode, expectedClassName));
  }
}

