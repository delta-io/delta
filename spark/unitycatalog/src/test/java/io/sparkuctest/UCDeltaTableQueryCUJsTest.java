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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.catalog.UCDeltaCatalogClientImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * End-to-end query tests that exercise the Delta REST API path ({@code deltaRestApi.enabled=true})
 * for reads/queries while using the legacy UC API for table creation.
 *
 * <p>This test uses a two-phase SparkSession approach:
 *
 * <ol>
 *   <li><b>Phase 1</b> — tables are created and populated using a SparkSession <em>without</em>
 *       {@code deltaRestApi.enabled}, so that DDL and initial writes go through the legacy UC
 *       catalog + commit coordinator path.
 *   <li><b>Phase 2</b> — a new SparkSession is created <em>with</em> {@code
 *       deltaRestApi.enabled=true}. All query tests run against this session, validating that the
 *       Delta REST API correctly serves table metadata for SELECTs, aggregations, joins, window
 *       functions, etc.
 * </ol>
 */
public class UCDeltaTableQueryCUJsTest extends UnityCatalogSupport {

  private SparkSession querySession;
  private long deltaRestApiLoadsBaseline;

  private List<String> createdTables = new ArrayList<>();

  // --------------- Lifecycle ---------------

  @BeforeAll
  public void setupTablesAndQuerySession() throws Exception {
    // Phase 1: Create and populate tables WITHOUT deltaRestApi.enabled
    SparkSession createSession = buildSparkSession(/* deltaRestApiEnabled= */ false);
    try {
      createAllTables(createSession);
    } finally {
      createSession.stop();
    }

    // Phase 2: Build query session WITH deltaRestApi.enabled
    querySession = buildSparkSession(/* deltaRestApiEnabled= */ true);
    deltaRestApiLoadsBaseline = UCDeltaCatalogClientImpl.SUCCESSFUL_DELTA_REST_API_LOADS().get();
  }

  @AfterAll
  public void tearDown() {
    if (querySession != null) {
      // Verify the Delta REST API was actually exercised during this suite
      long loadsAfter = UCDeltaCatalogClientImpl.SUCCESSFUL_DELTA_REST_API_LOADS().get();
      if (loadsAfter <= deltaRestApiLoadsBaseline) {
        throw new AssertionError(
            "No UCDeltaCatalogClientImpl.loadTable call returned a Delta table via the "
                + "Delta REST API during this suite. baseline="
                + deltaRestApiLoadsBaseline
                + ", after="
                + loadsAfter);
      }

      // Clean up tables using a session without deltaRestApi
      querySession.stop();
      querySession = null;

      SparkSession cleanupSession = buildSparkSession(/* deltaRestApiEnabled= */ false);
      try {
        for (String table : createdTables) {
          cleanupSession.sql("DROP TABLE IF EXISTS " + table);
        }
      } finally {
        cleanupSession.stop();
      }
    }
  }

  // --------------- SparkSession Builder ---------------

  private SparkSession buildSparkSession(boolean deltaRestApiEnabled) {
    UnityCatalogInfo uc = unityCatalogInfo();
    String catalogName = uc.catalogName();

    SparkConf conf =
        new SparkConf()
            .setAppName("Query CUJs " + (deltaRestApiEnabled ? "(Delta REST API)" : "(Legacy)"))
            .setMaster("local[2]")
            .set("spark.ui.enabled", "false")
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.sql.catalog." + catalogName, "io.unitycatalog.spark.UCSingleCatalog")
            .set("spark.sql.catalog." + catalogName + ".uri", uc.serverUri())
            .set("spark.sql.catalog." + catalogName + ".token", uc.serverToken());

    if (deltaRestApiEnabled) {
      conf.set("spark.sql.catalog." + catalogName + ".deltaRestApi.enabled", "true");
    }

    if (isUCRemoteConfigured()) {
      conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    } else {
      conf.set("spark.hadoop.fs.s3.impl", S3CredentialFileSystem.class.getName());
    }

    return SparkSession.builder().config(conf).getOrCreate();
  }

  // --------------- Table Setup (Phase 1, legacy path) ---------------

  private void createAllTables(SparkSession spark) {
    createTable(
        spark,
        "query_basic",
        "id INT, name STRING, amount DOUBLE",
        "(1, 'Alice', 100.5), (2, 'Bob', 200.0), (3, 'Carol', 150.75)");

    createTable(
        spark,
        "query_filter",
        "id INT, category STRING, price DOUBLE, in_stock BOOLEAN",
        "(1, 'electronics', 999.99, true), "
            + "(2, 'books', 29.99, true), "
            + "(3, 'electronics', 499.50, false), "
            + "(4, 'clothing', 79.99, true), "
            + "(5, 'books', 15.00, false)");

    createTable(
        spark,
        "query_agg",
        "dept STRING, employee STRING, salary INT",
        "('eng', 'Alice', 120000), ('eng', 'Bob', 110000), "
            + "('sales', 'Carol', 90000), ('sales', 'Dave', 95000), "
            + "('eng', 'Eve', 130000)");

    createTable(
        spark,
        "query_join_customers",
        "id INT, name STRING",
        "(1, 'Alice'), (2, 'Bob'), (3, 'Carol')");

    createTable(
        spark,
        "query_join_orders",
        "order_id INT, customer_id INT, amount DOUBLE",
        "(101, 1, 50.0), (102, 2, 75.0), (103, 1, 30.0), (104, 99, 10.0)");

    createTable(
        spark,
        "query_window",
        "dept STRING, employee STRING, salary INT",
        "('eng', 'Alice', 120), ('eng', 'Bob', 110), ('eng', 'Eve', 130), "
            + "('sales', 'Carol', 90), ('sales', 'Dave', 95)");

    createTable(spark, "query_set_a", "id INT, value STRING", "(1, 'a'), (2, 'b'), (3, 'c')");

    createTable(spark, "query_set_b", "id INT, value STRING", "(2, 'b'), (3, 'c'), (4, 'd')");

    createTable(
        spark,
        "query_nulls",
        "id INT, name STRING, score INT",
        "(1, 'Alice', 90), (2, null, 80), (3, 'Carol', null)");

    createTable(
        spark,
        "query_partitioned",
        "id INT, value INT, dt STRING",
        "dt",
        "(1, 10, '2025-01-01'), (2, 20, '2025-01-01'), "
            + "(3, 30, '2025-01-02'), (4, 40, '2025-01-02'), "
            + "(5, 50, '2025-01-03')");

    createTable(spark, "query_write_read", "id INT, value STRING", "(1, 'v1'), (2, 'v2')");
  }

  private void createTable(SparkSession spark, String name, String schema, String values) {
    createTable(spark, name, schema, null, values);
  }

  private void createTable(
      SparkSession spark, String name, String schema, String partitionCol, String values) {
    String fullName = fullTableName(name);
    createdTables.add(fullName);
    spark.sql("DROP TABLE IF EXISTS " + fullName);
    String partitionClause = partitionCol != null ? "PARTITIONED BY (" + partitionCol + ")" : "";
    spark.sql(
        String.format(
            "CREATE TABLE %s (%s) USING DELTA %s "
                + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
            fullName, schema, partitionClause));
    spark.sql(String.format("INSERT INTO %s VALUES %s", fullName, values));
  }

  // --------------- Helpers ---------------

  private String fullTableName(String simpleName) {
    UnityCatalogInfo uc = unityCatalogInfo();
    return uc.catalogName() + "." + uc.schemaName() + "." + simpleName;
  }

  private List<List<String>> sql(String query, Object... args) {
    String formatted = args.length > 0 ? String.format(query, args) : query;
    Row[] rows = (Row[]) querySession.sql(formatted).collect();
    return Arrays.stream(rows)
        .map(
            row -> {
              List<String> cells = new ArrayList<>();
              for (int i = 0; i < row.length(); i++) {
                cells.add(row.isNullAt(i) ? "null" : row.get(i).toString());
              }
              return cells;
            })
        .collect(Collectors.toList());
  }

  // --------------- Query Tests (Phase 2, Delta REST API) ---------------

  @Test
  public void testSelectAll() {
    String t = fullTableName("query_basic");
    List<List<String>> result = sql("SELECT * FROM %s ORDER BY id", t);
    assertThat(result)
        .containsExactly(
            List.of("1", "Alice", "100.5"),
            List.of("2", "Bob", "200.0"),
            List.of("3", "Carol", "150.75"));
  }

  @Test
  public void testColumnProjection() {
    String t = fullTableName("query_basic");
    List<List<String>> result = sql("SELECT name, amount FROM %s ORDER BY name", t);
    assertThat(result)
        .containsExactly(
            List.of("Alice", "100.5"), List.of("Bob", "200.0"), List.of("Carol", "150.75"));
  }

  @Test
  public void testFilterWithPredicates() {
    String t = fullTableName("query_filter");

    List<List<String>> inStockElectronics =
        sql(
            "SELECT id, price FROM %s WHERE category = 'electronics' AND in_stock = true ORDER BY id",
            t);
    assertThat(inStockElectronics).containsExactly(List.of("1", "999.99"));

    List<List<String>> cheapItems = sql("SELECT id FROM %s WHERE price < 50.0 ORDER BY id", t);
    assertThat(cheapItems).containsExactly(List.of("2"), List.of("5"));
  }

  @Test
  public void testFilterWithInAndBetween() {
    String t = fullTableName("query_filter");

    List<List<String>> inResult =
        sql("SELECT id FROM %s WHERE category IN ('books', 'clothing') ORDER BY id", t);
    assertThat(inResult).containsExactly(List.of("2"), List.of("4"), List.of("5"));

    List<List<String>> betweenResult =
        sql("SELECT id FROM %s WHERE id BETWEEN 2 AND 4 ORDER BY id", t);
    assertThat(betweenResult).containsExactly(List.of("2"), List.of("3"), List.of("4"));
  }

  @Test
  public void testAggregations() {
    String t = fullTableName("query_agg");

    List<List<String>> countByDept =
        sql("SELECT dept, COUNT(*) as cnt FROM %s GROUP BY dept ORDER BY dept", t);
    assertThat(countByDept).containsExactly(List.of("eng", "3"), List.of("sales", "2"));

    List<List<String>> avgSalary =
        sql(
            "SELECT dept, CAST(AVG(salary) AS LONG) as avg_sal FROM %s GROUP BY dept ORDER BY dept",
            t);
    assertThat(avgSalary).containsExactly(List.of("eng", "120000"), List.of("sales", "92500"));

    List<List<String>> havingClause =
        sql(
            "SELECT dept, SUM(salary) as total FROM %s GROUP BY dept HAVING SUM(salary) > 200000",
            t);
    assertThat(havingClause).containsExactly(List.of("eng", "360000"));
  }

  @Test
  public void testDistinctAndCountDistinct() {
    String t = fullTableName("query_filter");

    List<List<String>> distinctCategories =
        sql("SELECT DISTINCT category FROM %s ORDER BY category", t);
    assertThat(distinctCategories)
        .containsExactly(List.of("books"), List.of("clothing"), List.of("electronics"));

    List<List<String>> countDistinct = sql("SELECT COUNT(DISTINCT category) as uniq FROM %s", t);
    assertThat(countDistinct).containsExactly(List.of("3"));
  }

  @Test
  public void testInnerJoin() {
    String orders = fullTableName("query_join_orders");
    String customers = fullTableName("query_join_customers");

    List<List<String>> result =
        sql(
            "SELECT c.name, o.order_id, o.amount "
                + "FROM %s o JOIN %s c ON o.customer_id = c.id "
                + "ORDER BY o.order_id",
            orders, customers);
    assertThat(result)
        .containsExactly(
            List.of("Alice", "101", "50.0"),
            List.of("Bob", "102", "75.0"),
            List.of("Alice", "103", "30.0"));
  }

  @Test
  public void testLeftJoin() {
    String orders = fullTableName("query_join_orders");
    String customers = fullTableName("query_join_customers");

    List<List<String>> result =
        sql(
            "SELECT o.order_id, c.name "
                + "FROM %s o LEFT JOIN %s c ON o.customer_id = c.id "
                + "ORDER BY o.order_id",
            orders, customers);
    assertThat(result)
        .containsExactly(
            List.of("101", "Alice"),
            List.of("102", "Bob"),
            List.of("103", "Alice"),
            List.of("104", "null"));
  }

  @Test
  public void testScalarSubquery() {
    String t = fullTableName("query_agg");

    List<List<String>> aboveAvg =
        sql(
            "SELECT employee FROM %s WHERE salary > (SELECT AVG(salary) FROM %s) ORDER BY employee",
            t, t);
    assertThat(aboveAvg).containsExactly(List.of("Alice"), List.of("Bob"), List.of("Eve"));
  }

  @Test
  public void testInSubquery() {
    String orders = fullTableName("query_join_orders");
    String customers = fullTableName("query_join_customers");

    List<List<String>> result =
        sql(
            "SELECT name FROM %s WHERE id IN (SELECT customer_id FROM %s) ORDER BY name",
            customers, orders);
    assertThat(result).containsExactly(List.of("Alice"), List.of("Bob"));
  }

  @Test
  public void testWindowFunctions() {
    String t = fullTableName("query_window");

    List<List<String>> ranked =
        sql(
            "SELECT employee, dept, salary, "
                + "RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk "
                + "FROM %s ORDER BY dept, rnk",
            t);
    assertThat(ranked)
        .containsExactly(
            List.of("Eve", "eng", "130", "1"),
            List.of("Alice", "eng", "120", "2"),
            List.of("Bob", "eng", "110", "3"),
            List.of("Dave", "sales", "95", "1"),
            List.of("Carol", "sales", "90", "2"));
  }

  @Test
  public void testRunningSum() {
    String t = fullTableName("query_window");

    List<List<String>> result =
        sql(
            "SELECT employee, salary, "
                + "SUM(salary) OVER (ORDER BY salary) as running_total "
                + "FROM %s ORDER BY salary",
            t);
    assertThat(result)
        .containsExactly(
            List.of("Carol", "90", "90"),
            List.of("Dave", "95", "185"),
            List.of("Bob", "110", "295"),
            List.of("Alice", "120", "415"),
            List.of("Eve", "130", "545"));
  }

  @Test
  public void testUnionAll() {
    String a = fullTableName("query_set_a");
    String b = fullTableName("query_set_b");

    List<List<String>> result =
        sql("SELECT id FROM %s UNION ALL SELECT id FROM %s ORDER BY id", a, b);
    assertThat(result)
        .containsExactly(
            List.of("1"), List.of("2"), List.of("2"), List.of("3"), List.of("3"), List.of("4"));
  }

  @Test
  public void testUnionDistinct() {
    String a = fullTableName("query_set_a");
    String b = fullTableName("query_set_b");

    List<List<String>> result = sql("SELECT id FROM %s UNION SELECT id FROM %s ORDER BY id", a, b);
    assertThat(result).containsExactly(List.of("1"), List.of("2"), List.of("3"), List.of("4"));
  }

  @Test
  public void testIntersect() {
    String a = fullTableName("query_set_a");
    String b = fullTableName("query_set_b");

    List<List<String>> result =
        sql("SELECT id FROM %s INTERSECT SELECT id FROM %s ORDER BY id", a, b);
    assertThat(result).containsExactly(List.of("2"), List.of("3"));
  }

  @Test
  public void testExcept() {
    String a = fullTableName("query_set_a");
    String b = fullTableName("query_set_b");

    List<List<String>> result = sql("SELECT id FROM %s EXCEPT SELECT id FROM %s ORDER BY id", a, b);
    assertThat(result).containsExactly(List.of("1"));
  }

  @Test
  public void testNullHandling() {
    String t = fullTableName("query_nulls");

    List<List<String>> notNull = sql("SELECT id FROM %s WHERE name IS NOT NULL ORDER BY id", t);
    assertThat(notNull).containsExactly(List.of("1"), List.of("3"));

    List<List<String>> coalesced =
        sql("SELECT id, COALESCE(name, 'unknown') as name FROM %s ORDER BY id", t);
    assertThat(coalesced)
        .containsExactly(List.of("1", "Alice"), List.of("2", "unknown"), List.of("3", "Carol"));

    List<List<String>> countWithNulls =
        sql("SELECT COUNT(*) as total, COUNT(score) as with_score FROM %s", t);
    assertThat(countWithNulls).containsExactly(List.of("3", "2"));
  }

  @Test
  public void testPartitionPruning() {
    String t = fullTableName("query_partitioned");

    List<List<String>> singlePartition =
        sql("SELECT id, value FROM %s WHERE dt = '2025-01-02' ORDER BY id", t);
    assertThat(singlePartition).containsExactly(List.of("3", "30"), List.of("4", "40"));

    List<List<String>> aggByPartition =
        sql("SELECT dt, SUM(value) as total FROM %s GROUP BY dt ORDER BY dt", t);
    assertThat(aggByPartition)
        .containsExactly(
            List.of("2025-01-01", "30"), List.of("2025-01-02", "70"), List.of("2025-01-03", "50"));
  }

  @Test
  public void testOrderByLimit() {
    String t = fullTableName("query_basic");

    List<List<String>> top2 = sql("SELECT id, name FROM %s ORDER BY id LIMIT 2", t);
    assertThat(top2).containsExactly(List.of("1", "Alice"), List.of("2", "Bob"));

    List<List<String>> descTop1 = sql("SELECT id FROM %s ORDER BY id DESC LIMIT 1", t);
    assertThat(descTop1).containsExactly(List.of("3"));
  }

  @Test
  public void testDataFrameApi() {
    String t = fullTableName("query_agg");
    Dataset<Row> result = querySession.table(t).groupBy("dept").sum("salary").orderBy("dept");
    List<Row> rows = result.collectAsList();
    assertThat(rows.stream().map(r -> r.getString(0)).collect(Collectors.toList()))
        .containsExactly("eng", "sales");
    assertThat(rows.stream().map(r -> r.getLong(1)).collect(Collectors.toList()))
        .containsExactly(360000L, 185000L);
  }

  /*  @Test
  public void testDmlThenQueryWithDeltaRestApi() {
    String t = fullTableName("query_write_read");

    // DML operations go through the Delta REST API commit path
    querySession.sql(String.format("UPDATE %s SET value = 'updated' WHERE id = 1", t));

    List<List<String>> afterUpdate = sql("SELECT * FROM %s ORDER BY id", t);
    assertThat(afterUpdate).containsExactly(List.of("1", "updated"), List.of("2", "v2"));

    querySession.sql(String.format("DELETE FROM %s WHERE id = 2", t));

    List<List<String>> afterDelete = sql("SELECT * FROM %s ORDER BY id", t);
    assertThat(afterDelete).containsExactly(List.of("1", "updated"));

    querySession.sql(String.format("INSERT INTO %s VALUES (3, 'new')", t));

    List<List<String>> afterInsert = sql("SELECT * FROM %s ORDER BY id", t);
    assertThat(afterInsert).containsExactly(List.of("1", "updated"), List.of("3", "new"));
  }*/
}
