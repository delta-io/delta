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
package io.delta.spark.internal.v2;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.read.DeltaV2ScanBuilder;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class DeltaV2TestBase {

  protected static SparkSession spark;
  protected static Engine defaultEngine;

  @BeforeAll
  public static void setUpSparkAndEngine() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("SparkKernelDsv2Tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .config("spark.sql.catalog.dsv2", "io.delta.spark.internal.v2.catalog.TestCatalog")
            .config("spark.sql.catalog.dsv2.base_path", System.getProperty("java.io.tmpdir"))
            .getOrCreate();
    defaultEngine = DefaultEngine.create(spark.sessionState().newHadoopConf());
  }

  @AfterAll
  public static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /** Returns a fresh, unique table directory path under the JVM temp dir. */
  protected static String newTablePath(String prefix) {
    return Paths.get(System.getProperty("java.io.tmpdir"), prefix + "-" + System.nanoTime())
        .toString();
  }

  /** Returns the DSv2-catalog table reference for a path-based Delta table. */
  protected static String dsv2Table(String tablePath) {
    return String.format("dsv2.delta.`%s`", tablePath);
  }

  protected void createTestTableWithData(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta LOCATION '%s'",
            tableName, path));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.5), (3, 'Charlie', 30.5)",
            tableName));
  }

  protected void createEmptyTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
  }

  protected void createEmptyPartitionedTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta PARTITIONED BY (name) LOCATION '%s'",
            tableName, path));
  }

  protected void createSchemaEvolutionTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT NOT NULL, "
                + "name String, value FLOAT, "
                + "info STRUCT<col1: INT, col2: STRING>) USING delta LOCATION '%s'"
                + "TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableTypeWidening' = 'true')",
            tableName, path));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "(1, 'Alice', 10.5, named_struct('col1', 27, 'col2', 'LA')), "
                + "(2,'Bob', NULL, named_struct('col1', 30, 'col2', 'NYC'))",
            tableName));
  }

  /** A runnable that can throw checked exceptions, for use with {@link #withSQLConf}. */
  @FunctionalInterface
  protected interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * Runs the given action with a Spark SQL configuration temporarily set, then restores the
   * original value afterwards (similar to Scala's {@code withSQLConf}).
   */
  protected void withSQLConf(String key, String value, ThrowingRunnable action) throws Exception {
    scala.Option<String> original = spark.conf().getOption(key);
    spark.conf().set(key, value);
    try {
      action.run();
    } finally {
      if (original.isDefined()) {
        spark.conf().set(key, original.get());
      } else {
        spark.conf().unset(key);
      }
    }
  }

  /**
   * Runs the given action and drops the specified tables afterwards, similar to Scala's {@code
   * withTable}.
   */
  protected void withTable(String[] tableNames, ThrowingRunnable action) throws Exception {
    try {
      action.run();
    } finally {
      for (String tableName : tableNames) {
        spark.sql(String.format("DROP TABLE IF EXISTS %s", tableName));
      }
    }
  }

  /** Runs the given action and removes the table directory afterwards. */
  protected void withTable(String tablePath, ThrowingRunnable action) throws Exception {
    try {
      action.run();
    } finally {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(tablePath));
      } catch (java.io.IOException ignored) {
        // Test cleanup best-effort.
      }
    }
  }

  protected static void createPartitionedTable(String tableName, String path) {
    spark.sql(
        String.format(
            "CREATE TABLE `%s` (part INT, date STRING, city STRING, name STRING, cnt INT) USING delta LOCATION '%s' PARTITIONED BY (date, city, part)",
            tableName, path));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "('1', '20180520', 'hz', 'Alice', '10'),"
                + "('1', '20180718', 'hz', 'Bob', '20'),"
                + "('1', '20180512', 'sh', 'Charlie', '30'),"
                + "('2', '20180520', 'bj', 'David', '40'),"
                + "('2', '20181212', 'sz', 'Eve', '50')",
            tableName));
  }

  /** Pushes legacy test filters through the Catalyst filter API used by DeltaV2ScanBuilder. */
  protected static void pushFilters(DeltaV2ScanBuilder builder, Filter... filters) {
    Expression[] expressions =
        Arrays.stream(filters)
            .map(filter -> toCatalystFilter(builder, filter))
            .toArray(Expression[]::new);
    scala.collection.immutable.Seq<Expression> expressionSeq =
        scala.jdk.javaapi.CollectionConverters.asScala(Arrays.asList(expressions)).toList();
    builder.pushFilters(expressionSeq);
  }

  protected static Expression toCatalystFilter(DeltaV2ScanBuilder builder, Filter filter) {
    // Concatenate data + partition fields via add() rather than StructType.merge: of the two merge
    // overloads, one is not available in every build and the other is private[sql], so neither is
    // portable. add() exists everywhere and suffices here (the schemas share no column names).
    StructType tableSchema = builder.getDataSchema();
    for (StructField field : builder.getPartitionSchema().fields()) {
      tableSchema = tableSchema.add(field);
    }
    return toCatalystFilter(filter, tableSchema);
  }

  private static Expression toCatalystFilter(Filter filter, StructType tableSchema) {
    if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      AttributeReference attribute = catalystAttribute(equalTo.attribute(), tableSchema);
      return new org.apache.spark.sql.catalyst.expressions.EqualTo(
          attribute, Literal.create(equalTo.value(), attribute.dataType()));
    }
    if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      AttributeReference attribute = catalystAttribute(greaterThan.attribute(), tableSchema);
      return new org.apache.spark.sql.catalyst.expressions.GreaterThan(
          attribute, Literal.create(greaterThan.value(), attribute.dataType()));
    }
    if (filter instanceof And) {
      And and = (And) filter;
      return new org.apache.spark.sql.catalyst.expressions.And(
          toCatalystFilter(and.left(), tableSchema), toCatalystFilter(and.right(), tableSchema));
    }
    if (filter instanceof Or) {
      Or or = (Or) filter;
      return new org.apache.spark.sql.catalyst.expressions.Or(
          toCatalystFilter(or.left(), tableSchema), toCatalystFilter(or.right(), tableSchema));
    }
    if (filter instanceof Not) {
      return new org.apache.spark.sql.catalyst.expressions.Not(
          toCatalystFilter(((Not) filter).child(), tableSchema));
    }
    if (filter instanceof StringStartsWith) {
      StringStartsWith startsWith = (StringStartsWith) filter;
      AttributeReference attribute = catalystAttribute(startsWith.attribute(), tableSchema);
      return new org.apache.spark.sql.catalyst.expressions.StartsWith(
          attribute, Literal.create(startsWith.value(), attribute.dataType()));
    }
    if (filter instanceof StringEndsWith) {
      StringEndsWith endsWith = (StringEndsWith) filter;
      AttributeReference attribute = catalystAttribute(endsWith.attribute(), tableSchema);
      return new org.apache.spark.sql.catalyst.expressions.EndsWith(
          attribute, Literal.create(endsWith.value(), attribute.dataType()));
    }
    throw new IllegalArgumentException("Unsupported test filter: " + filter);
  }

  private static AttributeReference catalystAttribute(String name, StructType tableSchema) {
    StructField field = tableSchema.apply(name);
    return new AttributeReference(
        name,
        field.dataType(),
        field.nullable(),
        field.metadata(),
        org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId(),
        scala.jdk.javaapi.CollectionConverters.asScala(Collections.<String>emptyList()).toList());
  }
}
