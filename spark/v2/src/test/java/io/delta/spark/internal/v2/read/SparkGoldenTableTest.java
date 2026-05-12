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
package io.delta.spark.internal.v2.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.golden.GoldenTableUtils$;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.catalog.SparkTable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.QueryTest$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkGoldenTableTest {

  private SparkSession spark;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.spark.internal.v2.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .setMaster("local[*]")
            .setAppName("SparkGoldenTableTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  /** Helper method to check DataFrame results against expected rows. */
  private void checkAnswer(Dataset<Row> df, List<Row> expected) {
    QueryTest$.MODULE$.checkAnswer(df, expected);
  }

  @Test
  public void testDsv2Internal() throws Exception {
    String tableName = "deltatbl-partition-prune";
    String tablePath = goldenTablePath("hive/" + tableName);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            new java.util.HashMap<String, String>() {
              {
                put("key1", "value1");
                put("key2", "value2");
              }
            });
    SparkTable table =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, tableName),
            tablePath,
            options);
    StructType expectedDataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("cnt", DataTypes.IntegerType, true),
            });
    StructType expectedPartitionSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("date", DataTypes.StringType, true),
              DataTypes.createStructField("city", DataTypes.StringType, true),
            });
    StructType expectedSchema =
        DataTypes.createStructType(
            new StructField[] {
              expectedPartitionSchema.fields()[1],
              expectedPartitionSchema.fields()[0],
              expectedDataSchema.fields()[0],
              expectedDataSchema.fields()[1]
            });
    assertEquals(expectedSchema, table.schema());
    assertEquals(String.format("delta.`%s`", tablePath), table.name());
    // Check table columns
    assertEquals(4, table.columns().length);
    assertEquals("city", table.columns()[0].name());
    assertEquals("date", table.columns()[1].name());
    assertEquals("name", table.columns()[2].name());
    assertEquals("cnt", table.columns()[3].name());

    // Check table partitioning
    assertEquals(2, table.partitioning().length);
    assertEquals("identity(date)", table.partitioning()[0].toString());
    assertEquals("identity(city)", table.partitioning()[1].toString());

    // Check table properties
    assertEquals(Map.of(), table.properties());

    CaseInsensitiveStringMap scanOptions =
        new CaseInsensitiveStringMap(
            new java.util.HashMap<String, String>() {
              {
                put("key3", "value3");
                put("key2", "new_value2");
              }
            });
    ScanBuilder builder = table.newScanBuilder(scanOptions);
    assertTrue((builder instanceof SparkScanBuilder));
    SparkScanBuilder scanBuilder = (SparkScanBuilder) builder;
    assertEquals(expectedDataSchema, scanBuilder.getDataSchema());
    assertEquals(expectedPartitionSchema, scanBuilder.getPartitionSchema());
    CaseInsensitiveStringMap combinedOptions =
        new CaseInsensitiveStringMap(
            new java.util.HashMap<String, String>() {
              {
                put("key1", "value1");
                put("key2", "new_value2");
                put("key3", "value3");
              }
            });
    assertEquals(combinedOptions, scanBuilder.getOptions());

    Scan scan1 = scanBuilder.build();
    assertTrue(scan1 instanceof SparkScan);
    SparkScan sparkScan1 = (SparkScan) scan1;
    assertEquals(expectedDataSchema, sparkScan1.getDataSchema());
    assertEquals(expectedDataSchema, sparkScan1.getReadDataSchema());
    assertEquals(expectedPartitionSchema, sparkScan1.getPartitionSchema());
    assertEquals(combinedOptions, sparkScan1.getOptions());
    verifyHadoopConf(sparkScan1.getConfiguration());

    // check SupportsPushDownRequiredColumns
    StructType prunedSchema =
        DataTypes.createStructType(
            new StructField[] {
              expectedDataSchema.fields()[0], expectedPartitionSchema.fields()[0],
            });
    scanBuilder.pruneColumns(prunedSchema);
    Scan scan2 = scanBuilder.build();
    assertTrue(scan2 instanceof SparkScan);
    SparkScan sparkScan2 = (SparkScan) scan2;
    assertEquals(expectedDataSchema, sparkScan2.getDataSchema());
    StructType expectedReadDataSchemaAfterPrune =
        DataTypes.createStructType(new StructField[] {expectedDataSchema.fields()[0]});
    assertEquals(expectedReadDataSchemaAfterPrune, sparkScan2.getReadDataSchema());
    assertEquals(combinedOptions, sparkScan2.getOptions());
    verifyHadoopConf(sparkScan2.getConfiguration());

    // check SupportsPushDownFilters
    // case 1: mix of supported and unsupported, data and partition filters
    checkSupportsPushDownFilters(
        table,
        scanOptions,
        // input filters
        new Filter[] {
          new GreaterThan("cnt", 10), // supported data filter
          new StringStartsWith("name", "foo"), // supported data filter
          new EqualTo("date", "2025-09-01"), // supported partition filter
          new StringEndsWith("city", "York"), // unsupported partition filter
        },
        // expected post-scan filters
        new Filter[] {
          new GreaterThan("cnt", 10),
          new StringStartsWith("name", "foo"),
          new StringEndsWith("city", "York"),
        },
        // expected pushed filters
        new Filter[] {
          new GreaterThan("cnt", 10),
          new StringStartsWith("name", "foo"),
          new EqualTo("date", "2025-09-01")
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(">", new Column("cnt"), Literal.ofInt(10)),
          new Predicate("STARTS_WITH", new Column("name"), Literal.ofString("foo")),
          new Predicate("=", new Column("date"), Literal.ofString("2025-09-01"))
        },
        // expected data filters
        new Filter[] {new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")},
        // expected kernel scan builder predicate
        Optional.of(
            new Predicate(
                "AND",
                new Predicate(
                    "AND",
                    new Predicate(">", new Column("cnt"), Literal.ofInt(10)),
                    new Predicate("STARTS_WITH", new Column("name"), Literal.ofString("foo"))),
                new Predicate("=", new Column("date"), Literal.ofString("2025-09-01")))));

    // case 2: OR and NOT filters
    checkSupportsPushDownFilters(
        table,
        scanOptions,
        // input filters
        new Filter[] {
          new Or(new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")),
          new Or(new EqualTo("cnt", 50), new EqualTo("date", "2025-10-01")),
          new Not(new And(new GreaterThan("cnt", 100), new EqualTo("date", "2025-09-01"))),
          new Not(new Or(new EqualTo("name", "foo"), new StringStartsWith("city", "New")))
        },
        // expected post-scan filters
        new Filter[] {
          new Or(new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")),
          new Or(new EqualTo("cnt", 50), new EqualTo("date", "2025-10-01")),
          new Not(new And(new GreaterThan("cnt", 100), new EqualTo("date", "2025-09-01"))),
          new Not(new Or(new EqualTo("name", "foo"), new StringStartsWith("city", "New")))
        },
        // expected pushed filters
        new Filter[] {
          new Or(new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")),
          new Or(new EqualTo("cnt", 50), new EqualTo("date", "2025-10-01")),
          new Not(new And(new GreaterThan("cnt", 100), new EqualTo("date", "2025-09-01"))),
          new Not(new Or(new EqualTo("name", "foo"), new StringStartsWith("city", "New")))
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "OR",
              new Predicate(">", new Column("cnt"), Literal.ofInt(10)),
              new Predicate("STARTS_WITH", new Column("name"), Literal.ofString("foo"))),
          new Predicate(
              "OR",
              new Predicate("=", new Column("cnt"), Literal.ofInt(50)),
              new Predicate("=", new Column("date"), Literal.ofString("2025-10-01"))),
          new Predicate(
              "NOT",
              new Predicate(
                  "AND",
                  new Predicate(">", new Column("cnt"), Literal.ofInt(100)),
                  new Predicate("=", new Column("date"), Literal.ofString("2025-09-01")))),
          new Predicate(
              "NOT",
              new Predicate(
                  "OR",
                  new Predicate("=", new Column("name"), Literal.ofString("foo")),
                  new Predicate("STARTS_WITH", new Column("city"), Literal.ofString("New"))))
        },
        // expected data filters
        new Filter[] {
          new Or(new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")),
          new Or(new EqualTo("cnt", 50), new EqualTo("date", "2025-10-01")),
          new Not(new And(new GreaterThan("cnt", 100), new EqualTo("date", "2025-09-01"))),
          new Not(new Or(new EqualTo("name", "foo"), new StringStartsWith("city", "New")))
        },
        // expected kernel scan builder predicate
        // reduce(And::new) over 4 predicates gives left-associative nesting:
        // AND(AND(AND(pred1, pred2), pred3), pred4)
        Optional.of(
            new Predicate(
                "AND",
                new Predicate(
                    "AND",
                    new Predicate(
                        "AND",
                        new Predicate(
                            "OR",
                            new Predicate(">", new Column("cnt"), Literal.ofInt(10)),
                            new Predicate(
                                "STARTS_WITH", new Column("name"), Literal.ofString("foo"))),
                        new Predicate(
                            "OR",
                            new Predicate("=", new Column("cnt"), Literal.ofInt(50)),
                            new Predicate(
                                "=", new Column("date"), Literal.ofString("2025-10-01")))),
                    new Predicate(
                        "NOT",
                        new Predicate(
                            "AND",
                            new Predicate(">", new Column("cnt"), Literal.ofInt(100)),
                            new Predicate(
                                "=", new Column("date"), Literal.ofString("2025-09-01"))))),
                new Predicate(
                    "NOT",
                    new Predicate(
                        "OR",
                        new Predicate("=", new Column("name"), Literal.ofString("foo")),
                        new Predicate(
                            "STARTS_WITH", new Column("city"), Literal.ofString("New")))))));

    // check SupportsRuntimeV2Filtering
    // city = 'hz' AND date = '20180520'
    org.apache.spark.sql.connector.expressions.filter.Predicate andPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "AND", new Expression[] {SparkScanTest.cityPredicate, SparkScanTest.datePredicate});
    SparkScanTest.checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {andPredicate},
        Arrays.asList("date=20180520/city=hz"));

    // city = 'hz' OR date = '20180520'
    org.apache.spark.sql.connector.expressions.filter.Predicate orPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "OR", new Expression[] {SparkScanTest.cityPredicate, SparkScanTest.datePredicate});
    SparkScanTest.checkSupportsRuntimeFilters(
        table,
        scanOptions,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {orPredicate},
        Arrays.asList("city=hz", "date=20180520"));

    //  city = 'hz', cnt > 10
    SparkScanTest.checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
          SparkScanTest.cityPredicate, SparkScanTest.dataPredicate
        },
        Arrays.asList("city=hz"));

    //  city = 'hz' OR cnt > 10
    org.apache.spark.sql.connector.expressions.filter.Predicate orDataPredicate =
        new org.apache.spark.sql.connector.expressions.filter.Predicate(
            "OR", new Expression[] {SparkScanTest.cityPredicate, SparkScanTest.dataPredicate});
    SparkScanTest.checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {orDataPredicate},
        SparkScanTest.allCities);

    // city = date
    SparkScanTest.checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
          SparkScanTest.negativeInterColPredicate
        },
        Arrays.asList());

    // city <> date
    SparkScanTest.checkSupportsRuntimeFilters(
        table,
        options,
        new org.apache.spark.sql.connector.expressions.filter.Predicate[] {
          SparkScanTest.interColPredicate
        },
        SparkScanTest.allCities);
  }

  private void checkSupportsPushDownFilters(
      SparkTable table,
      CaseInsensitiveStringMap scanOptions,
      Filter[] inputFilters,
      Filter[] expectedPostScanFilters,
      Filter[] expectedPushedFilters,
      Predicate[] expectedPushedKernelPredicates,
      Filter[] expectedDataFilters,
      Optional<Predicate> expectedKernelScanBuilderPredicate)
      throws Exception {
    ScanBuilder newBuilder = table.newScanBuilder(scanOptions);
    SparkScanBuilder builder = (SparkScanBuilder) newBuilder;

    Filter[] postScanFilters = builder.pushFilters(inputFilters);

    assertEquals(
        new HashSet<>(Arrays.asList(expectedPostScanFilters)),
        new HashSet<>(Arrays.asList(postScanFilters)));

    assertEquals(
        new HashSet<>(Arrays.asList(expectedPushedFilters)),
        new HashSet<>(Arrays.asList(builder.pushedFilters())));

    Predicate[] pushedPredicates = getPushedKernelPredicates(builder);
    assertEquals(
        new HashSet<>(Arrays.asList(expectedPushedKernelPredicates)),
        new HashSet<>(Arrays.asList(pushedPredicates)));

    Filter[] dataFilters = getDataFilters(builder);
    assertEquals(
        new HashSet<>(Arrays.asList(expectedDataFilters)),
        new HashSet<>(Arrays.asList(dataFilters)));

    Optional<Predicate> predicateOpt = getKernelScanBuilderPredicate(builder);
    assertEquals(expectedKernelScanBuilderPredicate, predicateOpt);
  }

  private Predicate[] getPushedKernelPredicates(SparkScanBuilder builder) throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("pushedKernelPredicates");
    field.setAccessible(true);
    return (Predicate[]) field.get(builder);
  }

  private Filter[] getDataFilters(SparkScanBuilder builder) throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("dataFilters");
    field.setAccessible(true);
    return (Filter[]) field.get(builder);
  }

  private Optional<Predicate> getKernelScanBuilderPredicate(SparkScanBuilder builder)
      throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("kernelScanBuilder");
    field.setAccessible(true);
    Object kernelScanBuilder = field.get(builder);
    Field predicateField = kernelScanBuilder.getClass().getDeclaredField("predicate");
    predicateField.setAccessible(true);
    Object raw = predicateField.get(kernelScanBuilder);
    if (raw == null) {
      return Optional.empty();
    }
    Optional<?> opt = (Optional<?>) raw;
    return opt.map(Predicate.class::cast);
  }

  @Test
  public void testDsv2InteralWithNestedStruct() {
    String tableName = "data-reader-nested-struct";
    String tablePath = goldenTablePath(tableName);
    SparkTable table =
        new SparkTable(
            Identifier.of(new String[] {"spark_catalog", "default"}, tableName), tablePath);

    StructType expectedSchema =
        StructType.fromDDL(
            "a STRUCT<aa: STRING, ab: STRING, ac: STRUCT<aca: INT, acb: BIGINT>>,b INT");

    assertEquals(expectedSchema, table.schema());
    assertEquals(String.format("delta.`%s`", tablePath), table.name());
    assertEquals(0, table.partitioning().length);

    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(
            java.util.Collections.singletonMap("another_option_key", "another_option_value"));
    ScanBuilder builder = table.newScanBuilder(options);
    assertTrue((builder instanceof SparkScanBuilder));
    SparkScanBuilder scanBuilder = (SparkScanBuilder) builder;

    assertEquals(expectedSchema, scanBuilder.getDataSchema());
    assertTrue(scanBuilder.getPartitionSchema().isEmpty());
    assertEquals(options, scanBuilder.getOptions());

    // Initial scan (no pruning)
    Scan scan1 = scanBuilder.build();
    assertTrue(scan1 instanceof SparkScan);
    SparkScan sparkScan1 = (SparkScan) scan1;
    assertEquals(expectedSchema, sparkScan1.getDataSchema());
    assertEquals(expectedSchema, sparkScan1.getReadDataSchema());
    assertTrue(sparkScan1.getPartitionSchema().isEmpty());
    assertEquals(options, sparkScan1.getOptions());

    StructType prunedSchema = StructType.fromDDL("a STRUCT<aa: STRING, ab: STRING>");
    scanBuilder.pruneColumns(prunedSchema);

    Scan scan2 = scanBuilder.build();
    assertTrue(scan2 instanceof SparkScan);
    SparkScan sparkScan2 = (SparkScan) scan2;
    assertEquals(expectedSchema, sparkScan2.getDataSchema());
    assertEquals(prunedSchema, sparkScan2.getReadDataSchema());
    assertTrue(sparkScan2.getPartitionSchema().isEmpty());
    assertEquals(options, sparkScan2.getOptions());
  }

  @Test
  public void testTablePrimitives() throws Exception {
    List<Row> expected = new ArrayList<>();
    for (int i = 0; i <= 10; i++) {
      if (i == 10) {
        expected.add(
            new GenericRow(
                new Object[] {null, null, null, null, null, null, null, null, null, null}));
      } else {
        expected.add(
            new GenericRow(
                new Object[] {
                  i,
                  (long) i,
                  (byte) i,
                  (short) i,
                  i % 2 == 0,
                  (float) i,
                  (double) i,
                  Integer.toString(i),
                  new byte[] {(byte) i, (byte) i},
                  new BigDecimal(i)
                }));
      }
    }

    checkTable("data-reader-primitives", expected);
  }

  @Test
  public void testTableWithNestedStruct() {
    List<Row> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Row innerMost = new GenericRow(new Object[] {i, (long) i});
      Row middle =
          new GenericRow(new Object[] {Integer.toString(i), Integer.toString(i), innerMost});
      expected.add(new GenericRow(new Object[] {middle, i}));
    }
    // Assuming `checkTable` is made accessible (e.g., protected in base class)
    checkTable("data-reader-nested-struct", expected);
  }

  @Test
  public void testPartitionedTable() {
    // Build expected rows (excluding unsupported partition column `as_timestamp`)
    List<Row> expected = new ArrayList<>();
    java.sql.Date fixedDate = java.sql.Date.valueOf("2021-09-08");

    for (int i = 0; i < 2; i++) {
      // Array field: Seq(TestRow(i), TestRow(i), TestRow(i)) where TestRow(i) => struct(i)
      List<Row> arrElems =
          Arrays.asList(
              new GenericRow(new Object[] {i}),
              new GenericRow(new Object[] {i}),
              new GenericRow(new Object[] {i}));
      Object arrSeq = scala.collection.JavaConverters.asScalaBuffer(arrElems).toList();

      // Nested struct: TestRow(i.toString, i.toString, TestRow(i, i.toLong))
      Row innerMost = new GenericRow(new Object[] {i, (long) i});
      Row middle =
          new GenericRow(new Object[] {Integer.toString(i), Integer.toString(i), innerMost});

      expected.add(
          new GenericRow(
              new Object[] {
                i, // int
                (long) i, // long
                (byte) i, // byte
                (short) i, // short
                i % 2 == 0, // boolean
                (float) i, // float
                (double) i, // double
                Integer.toString(i), // string
                "null", // literal string
                fixedDate, // date (was daysSinceEpoch int)
                new BigDecimal(i), // decimal
                arrSeq, // array<struct>
                middle, // nested struct
                Integer.toString(i) // final string
              }));
    }

    // Null row variant with specific non-null complex fields (matches Scala test)
    List<Row> nullArrElems =
        Arrays.asList(
            new GenericRow(new Object[] {2}),
            new GenericRow(new Object[] {2}),
            new GenericRow(new Object[] {2}));
    Object nullArrSeq = scala.collection.JavaConverters.asScalaBuffer(nullArrElems).toList();
    Row nullInnerMost = new GenericRow(new Object[] {2, 2L});
    Row nullMiddle = new GenericRow(new Object[] {"2", "2", nullInnerMost});
    expected.add(
        new GenericRow(
            new Object[] {
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              null,
              nullArrSeq,
              nullMiddle,
              "2"
            }));

    // Read table, drop unsupported column `as_timestamp`
    String tablePath = goldenTablePath("data-reader-partition-values");
    Dataset<Row> full = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");

    List<String> projectedCols = new ArrayList<>();
    for (String f : full.schema().fieldNames()) {
      if (!f.equals("as_timestamp")) {
        projectedCols.add(f);
      }
    }
    Dataset<Row> df = full.selectExpr(projectedCols.toArray(new String[0]));

    checkAnswer(df, expected);
  }

  @Test
  public void testVariantTypeTable() {
    String tablePath = goldenTablePath("spark-variant-checkpoint");
    Dataset<Row> df = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");

    // Verify schema: id (long) + 6 variant/nested-variant columns
    StructType schema = df.schema();
    assertEquals(7, schema.fields().length);
    assertEquals(DataTypes.LongType, schema.apply("id").dataType());
    assertEquals(DataTypes.VariantType, schema.apply("v").dataType());
    assertEquals(
        DataTypes.createArrayType(DataTypes.VariantType, true),
        schema.apply("array_of_variants").dataType());
    assertEquals(
        DataTypes.createStructType(
            new StructField[] {
              new StructField(
                  "v", DataTypes.VariantType, true, org.apache.spark.sql.types.Metadata.empty())
            }),
        schema.apply("struct_of_variants").dataType());
    assertEquals(
        DataTypes.createMapType(DataTypes.StringType, DataTypes.VariantType, true),
        schema.apply("map_of_variants").dataType());

    // Verify row count: 100 base rows + 2 appended rows
    assertEquals(102, df.count());

    // Verify id values are readable (non-variant column)
    List<Row> ids = df.select("id").orderBy("id").limit(3).collectAsList();
    assertEquals(0L, ids.get(0).getLong(0));
    assertEquals(0L, ids.get(1).getLong(0));
    assertEquals(1L, ids.get(2).getLong(0));

    // Verify all variant column values. Each variant value is parse_json('{"key": id}'),
    // so variant_get(..., '$.key', 'long') must equal id for all rows.
    // - v:                                  direct variant
    // - array_of_variants[0]:               first element (indices 1, 3 are null)
    // - struct_of_variants.v:               struct field
    // - map_of_variants[CAST(id AS STRING)]: map value by string key
    // - array_of_struct_of_variants[0].v:   first struct element's variant field
    // - struct_of_array_of_variants.v[1]:   struct's array field at index 1 (index 0 is null)
    long matchingRows =
        df.where(
                "variant_get(v, '$.key', 'long') = id"
                    + " AND variant_get(array_of_variants[0], '$.key', 'long') = id"
                    + " AND variant_get(struct_of_variants.v, '$.key', 'long') = id"
                    + " AND variant_get(map_of_variants[CAST(id AS STRING)], '$.key', 'long') = id"
                    + " AND variant_get(array_of_struct_of_variants[0].v, '$.key', 'long') = id"
                    + " AND variant_get(struct_of_array_of_variants.v[1], '$.key', 'long') = id")
            .count();
    assertEquals(102, matchingRows);

    // Verify known null values within variant columns:
    // - array_of_variants[1] and [3]:          null array elements
    // - map_of_variants['nullKey']:             null map value
    // - array_of_struct_of_variants[1].v:       non-null struct but null variant field
    // - array_of_struct_of_variants[2]:         null struct element
    // - struct_of_array_of_variants.v[0]:       null first element of struct's array field
    long nullMatchingRows =
        df.where(
                "array_of_variants[1] IS NULL"
                    + " AND array_of_variants[3] IS NULL"
                    + " AND map_of_variants['nullKey'] IS NULL"
                    + " AND array_of_struct_of_variants[1].v IS NULL"
                    + " AND array_of_struct_of_variants[2] IS NULL"
                    + " AND struct_of_array_of_variants.v[0] IS NULL")
            .count();
    assertEquals(102, nullMatchingRows);
  }

  @Test
  public void testAllGoldenTables() {
    List<String> tableNames = getAllGoldenTableNames();
    List<String> unsupportedTables =
        Arrays.asList(
            "canonicalized-paths-normal-a",
            "canonicalized-paths-normal-b",
            "canonicalized-paths-special-a",
            "canonicalized-paths-special-b",
            "checkpoint",
            "corrupted-last-checkpoint",
            "data-reader-absolute-paths-escaped-chars",
            "data-reader-escaped-chars",
            // File delete-re-add-same-file-different-transactions/bar does not exist
            "delete-re-add-same-file-different-transactions",
            // Root node at key schemaString is null but field isn't nullable
            "deltalog-commit-info",
            // [DELTA_INVALID_PROTOCOL_VERSION] Unsupported Delta protocol version
            "deltalog-invalid-protocol-version",
            // [DELTA_STATE_RECOVER_ERROR] The metadata of your Delta table could not be recovered
            // while Reconstructing
            "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
            // [DELTA_STATE_RECOVER_ERROR] The protocol of your Delta table could not be recovered
            // while Reconstructing
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol");

    for (String tableName : tableNames) {
      if (unsupportedTables.contains(tableName)) {
        continue;
      }

      // For simplicity, just check that we can read the table and it has at least one row
      String tablePath = goldenTablePath(tableName);
      // Many golden tables only have corrupted _delta_log subdir. The new kernel table reader will
      // fail on some of those.
      // TODO: fix the read result of those tables.
      if (hasOnlyDeltaLogSubdir(tablePath)) {
        continue;
      }
      Dataset<Row> df = spark.sql("SELECT * FROM `spark_catalog`.`delta`.`" + tablePath + "`");
      Dataset<Row> df2 = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");
      assertEquals(df.schema(), df2.schema(), "Schema mismatch for table: " + tableName);
      checkAnswer(df2, df.collectAsList());
    }
  }

  /**
   * Differential streaming corpus test (gap class #7).
   *
   * <p>Mirrors {@link #testAllGoldenTables()} for streaming: for each golden table, runs DSv1
   * streaming and DSv2 streaming side by side with {@code Trigger.AvailableNow} and asserts both
   * paths return the same schema and rows. Every divergence is a real differential bug.
   *
   * <p>Skip strategy: option (a) — inspect the delta log directly. A table is skipped if any commit
   * JSON contains a {@code "remove"} action (delete/overwrite/restore/merge/update history). Such
   * tables would require {@code ignoreDeletes}/{@code ignoreChanges} which changes the contract
   * under test. We also skip the same {@code unsupportedTables} as the batch test, plus tables that
   * have no top-level data files (only a corrupt/synthetic _delta_log).
   */
  @Test
  public void testAllGoldenTablesStreaming() throws Exception {
    List<String> tableNames = getAllGoldenTableNames();
    // Same allowlist as testAllGoldenTables — corrupt-by-design or DSv2-batch-unsupported.
    List<String> unsupportedTables =
        Arrays.asList(
            "canonicalized-paths-normal-a",
            "canonicalized-paths-normal-b",
            "canonicalized-paths-special-a",
            "canonicalized-paths-special-b",
            "checkpoint",
            "corrupted-last-checkpoint",
            "data-reader-absolute-paths-escaped-chars",
            "data-reader-escaped-chars",
            "delete-re-add-same-file-different-transactions",
            "deltalog-commit-info",
            "deltalog-invalid-protocol-version",
            "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol");

    int tested = 0;
    int skippedUnsupported = 0;
    int skippedNoData = 0;
    int skippedNonAppend = 0;
    int skippedSetupFailure = 0;
    List<String> divergences = new ArrayList<>();
    List<String> testedTableNames = new ArrayList<>();

    for (String tableName : tableNames) {
      if (unsupportedTables.contains(tableName)) {
        skippedUnsupported++;
        continue;
      }
      String tablePath = goldenTablePath(tableName);
      if (hasOnlyDeltaLogSubdir(tablePath)) {
        skippedNoData++;
        continue;
      }
      // Detect non-append history by scanning commit JSON for "remove" actions. Streaming sources
      // require ignoreDeletes / ignoreChanges in that case; we skip to keep the basic contract
      // under test.
      try {
        if (hasNonAppendHistory(tablePath)) {
          skippedNonAppend++;
          continue;
        }
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }

      String safeName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
      String q1Name = "gt_v1_" + safeName + "_" + System.nanoTime();
      String q2Name = "gt_v2_" + safeName + "_" + System.nanoTime();

      List<Row> v1Rows;
      StructType v1Schema;
      try {
        Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
        v1Schema = v1Stream.schema();
        v1Rows = collectStream(v1Stream, q1Name);
      } catch (Throwable t) {
        // DSv1 itself failed — record as a divergence candidate only if DSv2 succeeds.
        StringBuilder sb = new StringBuilder();
        sb.append("[")
            .append(tableName)
            .append("] DSv1 streaming failed (DSv2 not yet attempted): ")
            .append(rootMessage(t));
        // Try DSv2 to see if it succeeds (= asymmetric failure, which is itself a divergence).
        try {
          Dataset<Row> v2Stream = spark.readStream().table("dsv2.delta.`" + tablePath + "`");
          List<Row> v2Rows = collectStream(v2Stream, q2Name);
          sb.append(" | DSv2 SUCCEEDED — DIVERGENCE: DSv2 returned ")
              .append(v2Rows.size())
              .append(" rows while DSv1 threw: ")
              .append(t.getClass().getSimpleName());
          divergences.add(sb.toString());
        } catch (Throwable t2) {
          // Both failed. Likely a setup/skip case; record as setup failure not divergence.
          skippedSetupFailure++;
        }
        continue;
      }

      List<Row> v2Rows;
      StructType v2Schema;
      try {
        Dataset<Row> v2Stream = spark.readStream().table("dsv2.delta.`" + tablePath + "`");
        v2Schema = v2Stream.schema();
        v2Rows = collectStream(v2Stream, q2Name);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] DIVERGENCE: DSv1 returned "
                + v1Rows.size()
                + " rows; DSv2 threw "
                + t.getClass().getName()
                + ": "
                + rootMessage(t));
        continue;
      }

      tested++;
      testedTableNames.add(tableName);

      if (!v1Schema.equals(v2Schema)) {
        divergences.add(
            "["
                + tableName
                + "] SCHEMA DIVERGENCE\n  DSv1: "
                + v1Schema.treeString()
                + "\n  DSv2: "
                + v2Schema.treeString());
        continue;
      }

      try {
        QueryTest$.MODULE$.checkAnswer(spark.createDataFrame(v2Rows, v2Schema), v1Rows);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] ROW DIVERGENCE\n  DSv1 rows ("
                + v1Rows.size()
                + "): "
                + truncate(v1Rows)
                + "\n  DSv2 rows ("
                + v2Rows.size()
                + "): "
                + truncate(v2Rows)
                + "\n  diff: "
                + rootMessage(t));
      }
    }

    System.out.println("=== testAllGoldenTablesStreaming summary ===");
    System.out.println("Total golden tables: " + tableNames.size());
    System.out.println("Tested: " + tested);
    System.out.println("Skipped (unsupportedTables allowlist): " + skippedUnsupported);
    System.out.println("Skipped (no top-level data dir): " + skippedNoData);
    System.out.println("Skipped (non-append history): " + skippedNonAppend);
    System.out.println("Skipped (setup failure / both sides failed): " + skippedSetupFailure);
    System.out.println("Tested tables: " + testedTableNames);
    System.out.println("Divergences found: " + divergences.size());
    for (String d : divergences) {
      System.out.println("---");
      System.out.println(d);
    }

    DeltaLog.clearCache();

    if (!divergences.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(divergences.size())
          .append(" DSv1 vs DSv2 streaming divergence(s) across ")
          .append(tested)
          .append(" tested golden tables:\n");
      for (String d : divergences) {
        sb.append("---\n").append(d).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  @Test
  public void testAllGoldenTablesStreamingMidRestart() throws Exception {
    // Curated subset hand-picked from Task G's TODO list. Each table targets a different bug shape:
    //   data-reader-partition-values            — partitions + complex types (the #6583 target)
    //   kernel-timestamp-PST                    — timestamp partition + non-canonical column order
    //   kernel-timestamp-INT96                  — timestamp partition (INT96 read path)
    //   dv-partitioned-with-checkpoint          — DV + partitioned + checkpoint (mid-stream)
    //   dv-with-columnmapping                   — DV + column mapping (#6606-class lifecycle)
    //   time-travel-partition-changes-a         — partition schema evolution
    //   spark-variant-checkpoint                — variant + checkpoint lifecycle
    //   data-reader-nested-struct (bonus)       — nested STRUCT lifecycle
    //   hive/deltatbl-partition-prune (bonus)   — Hive-style partitioned table
    List<String> subset =
        Arrays.asList(
            "data-reader-partition-values",
            "kernel-timestamp-PST",
            "kernel-timestamp-INT96",
            "dv-partitioned-with-checkpoint",
            "dv-with-columnmapping",
            "time-travel-partition-changes-a",
            "spark-variant-checkpoint",
            "data-reader-nested-struct",
            "hive/deltatbl-partition-prune");

    List<String> divergences = new ArrayList<>();
    List<String> passed = new ArrayList<>();
    List<String> skipped = new ArrayList<>();

    for (String tableName : subset) {
      String tablePath = goldenTablePath(tableName);
      File tableDir = new File(tablePath);
      if (!tableDir.exists() || hasOnlyDeltaLogSubdir(tablePath)) {
        skipped.add(tableName + " (no top-level data dir)");
        continue;
      }

      String safeName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
      File checkpointDir = Files.createTempDirectory("midrestart_ckpt_" + safeName).toFile();

      // 1. DSv1 oracle: one-shot AvailableNow on a separate (memory-sink) query.
      String oracleName = "midrestart_oracle_" + safeName + "_" + System.nanoTime();
      List<Row> oracleRows;
      StructType oracleSchema;
      try {
        Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
        oracleSchema = v1Stream.schema();
        oracleRows = collectStreamOnce(v1Stream, oracleName);
      } catch (Throwable t) {
        // If DSv1 itself can't read the table, we skip — there's no oracle.
        skipped.add(tableName + " (DSv1 oracle failed: " + rootMessage(t) + ")");
        continue;
      }

      // 2. DSv2 first half: stop after the first batch (maxFilesPerTrigger=1).
      List<Row> firstHalf = new ArrayList<>();
      AtomicInteger batchCounter = new AtomicInteger(0);
      int[] firstHalfBatches = new int[] {0};
      try {
        Dataset<Row> v2Stream =
            spark
                .readStream()
                .option("maxFilesPerTrigger", "1")
                .table("dsv2.delta.`" + tablePath + "`");

        // Holder for the query so the foreachBatch lambda can stop it after batch 0.
        StreamingQuery[] queryHolder = new StreamingQuery[1];
        VoidFunction2<Dataset<Row>, Long> writeFirstBatch =
            (Dataset<Row> batch, Long batchId) -> {
              if (batchCounter.get() == 0) {
                // Persist this batch's rows so we can union with second-half later.
                List<Row> rows = batch.collectAsList();
                synchronized (firstHalf) {
                  firstHalf.addAll(rows);
                }
              }
              int n = batchCounter.incrementAndGet();
              firstHalfBatches[0] = n;
              if (n >= 1 && queryHolder[0] != null) {
                // Stop after the first batch is committed. The next start() resumes from offset 1.
                new Thread(
                        () -> {
                          try {
                            queryHolder[0].stop();
                          } catch (Throwable ignored) {
                          }
                        })
                    .start();
              }
            };

        StreamingQuery q =
            v2Stream
                .writeStream()
                .foreachBatch(writeFirstBatch)
                .option("checkpointLocation", checkpointDir.getAbsolutePath())
                .trigger(Trigger.AvailableNow())
                .start();
        queryHolder[0] = q;
        try {
          q.awaitTermination(60_000);
        } catch (Throwable ignored) {
          // Timeout or stop()-induced exception — proceed to restart phase regardless.
        }
        try {
          q.stop();
        } catch (Throwable ignored) {
        }
      } catch (Throwable t) {
        divergences.add("[" + tableName + "] DSv2 FIRST-HALF FAILED: " + rootMessage(t));
        continue;
      }

      // 3. DSv2 second half: restart from the same checkpoint, no batch cap, run to completion.
      List<Row> secondHalf = new ArrayList<>();
      StructType secondHalfSchema = null;
      try {
        Dataset<Row> v2StreamRestart = spark.readStream().table("dsv2.delta.`" + tablePath + "`");
        secondHalfSchema = v2StreamRestart.schema();
        VoidFunction2<Dataset<Row>, Long> writeRest =
            (Dataset<Row> batch, Long batchId) -> {
              List<Row> rows = batch.collectAsList();
              synchronized (secondHalf) {
                secondHalf.addAll(rows);
              }
            };
        StreamingQuery q =
            v2StreamRestart
                .writeStream()
                .foreachBatch(writeRest)
                .option("checkpointLocation", checkpointDir.getAbsolutePath())
                .trigger(Trigger.AvailableNow())
                .start();
        try {
          q.processAllAvailable();
        } finally {
          try {
            q.stop();
          } catch (Throwable ignored) {
          }
        }
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] DSv2 RESTART FAILED after first-half had "
                + firstHalf.size()
                + " row(s): "
                + rootMessage(t));
        continue;
      }

      // 4. Compare (firstHalf ∪ secondHalf) to oracle.
      if (secondHalfSchema != null && !oracleSchema.equals(secondHalfSchema)) {
        divergences.add(
            "["
                + tableName
                + "] SCHEMA DIVERGENCE on restart\n  DSv1 oracle: "
                + oracleSchema.treeString()
                + "\n  DSv2 restart: "
                + secondHalfSchema.treeString());
        continue;
      }

      List<Row> combined = new ArrayList<>(firstHalf.size() + secondHalf.size());
      combined.addAll(firstHalf);
      combined.addAll(secondHalf);

      try {
        QueryTest$.MODULE$.checkAnswer(spark.createDataFrame(combined, oracleSchema), oracleRows);
        passed.add(
            tableName
                + " (firstHalfBatches="
                + firstHalfBatches[0]
                + ", firstHalfRows="
                + firstHalf.size()
                + ", secondHalfRows="
                + secondHalf.size()
                + ", oracleRows="
                + oracleRows.size()
                + ")");
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] ROW DIVERGENCE on restart\n  oracle ("
                + oracleRows.size()
                + " rows): "
                + truncate(oracleRows)
                + "\n  combined ("
                + combined.size()
                + " rows; firstHalf="
                + firstHalf.size()
                + ", secondHalf="
                + secondHalf.size()
                + "): "
                + truncate(combined)
                + "\n  diff: "
                + rootMessage(t));
      }
    }

    System.out.println("=== testAllGoldenTablesStreamingMidRestart summary ===");
    System.out.println("Subset size: " + subset.size());
    System.out.println("Passed: " + passed.size());
    for (String p : passed) System.out.println("  PASS  " + p);
    System.out.println("Skipped: " + skipped.size());
    for (String s : skipped) System.out.println("  SKIP  " + s);
    System.out.println("Divergences: " + divergences.size());
    for (String d : divergences) {
      System.out.println("---");
      System.out.println(d);
    }

    DeltaLog.clearCache();

    if (!divergences.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(divergences.size())
          .append(" mid-stream-restart divergence(s) across ")
          .append(subset.size())
          .append(" curated tables:\n");
      for (String d : divergences) {
        sb.append("---\n").append(d).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  /**
   * Differential streaming with {@code startingVersion=1}.
   *
   * <p>Mirrors {@link #testAllGoldenTablesStreaming()} but skips the initial-snapshot path and
   * exercises the incremental-only path. Tables with fewer than 2 commits are skipped.
   */
  @Test
  public void testAllGoldenTablesStreamingFromVersion1() throws Exception {
    List<String> tableNames = getAllGoldenTableNames();
    List<String> unsupportedTables =
        Arrays.asList(
            "canonicalized-paths-normal-a",
            "canonicalized-paths-normal-b",
            "canonicalized-paths-special-a",
            "canonicalized-paths-special-b",
            "checkpoint",
            "corrupted-last-checkpoint",
            "data-reader-absolute-paths-escaped-chars",
            "data-reader-escaped-chars",
            "delete-re-add-same-file-different-transactions",
            "deltalog-commit-info",
            "deltalog-invalid-protocol-version",
            "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol");

    int tested = 0;
    int skippedUnsupported = 0;
    int skippedNoData = 0;
    int skippedNonAppend = 0;
    int skippedSingleVersion = 0;
    int skippedSetupFailure = 0;
    List<String> divergences = new ArrayList<>();
    List<String> testedTableNames = new ArrayList<>();

    for (String tableName : tableNames) {
      if (unsupportedTables.contains(tableName)) {
        skippedUnsupported++;
        continue;
      }
      String tablePath = goldenTablePath(tableName);
      if (hasOnlyDeltaLogSubdir(tablePath)) {
        skippedNoData++;
        continue;
      }
      try {
        if (hasNonAppendHistory(tablePath)) {
          skippedNonAppend++;
          continue;
        }
        if (countCommitJsonFiles(tablePath) < 2) {
          skippedSingleVersion++;
          continue;
        }
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }

      String safeName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
      String q1Name = "gtv1_v1_" + safeName + "_" + System.nanoTime();
      String q2Name = "gtv1_v2_" + safeName + "_" + System.nanoTime();

      List<Row> v1Rows;
      StructType v1Schema;
      try {
        Dataset<Row> v1Stream =
            spark.readStream().format("delta").option("startingVersion", "1").load(tablePath);
        v1Schema = v1Stream.schema();
        v1Rows = collectStream(v1Stream, q1Name);
      } catch (Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
            .append(tableName)
            .append("] DSv1 streaming(startingVersion=1) failed: ")
            .append(rootMessage(t));
        try {
          Dataset<Row> v2Stream =
              spark
                  .readStream()
                  .option("startingVersion", "1")
                  .table("dsv2.delta.`" + tablePath + "`");
          List<Row> v2Rows = collectStream(v2Stream, q2Name);
          sb.append(" | DSv2 SUCCEEDED — DIVERGENCE: DSv2 returned ")
              .append(v2Rows.size())
              .append(" rows while DSv1 threw: ")
              .append(t.getClass().getSimpleName());
          divergences.add(sb.toString());
        } catch (Throwable t2) {
          skippedSetupFailure++;
        }
        continue;
      }

      List<Row> v2Rows;
      StructType v2Schema;
      try {
        Dataset<Row> v2Stream =
            spark
                .readStream()
                .option("startingVersion", "1")
                .table("dsv2.delta.`" + tablePath + "`");
        v2Schema = v2Stream.schema();
        v2Rows = collectStream(v2Stream, q2Name);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] DIVERGENCE(startingVersion=1): DSv1 returned "
                + v1Rows.size()
                + " rows; DSv2 threw "
                + t.getClass().getName()
                + ": "
                + rootMessage(t));
        continue;
      }

      tested++;
      testedTableNames.add(tableName);

      if (!v1Schema.equals(v2Schema)) {
        divergences.add(
            "["
                + tableName
                + "] SCHEMA DIVERGENCE(startingVersion=1)\n  DSv1: "
                + v1Schema.treeString()
                + "\n  DSv2: "
                + v2Schema.treeString());
        continue;
      }

      try {
        QueryTest$.MODULE$.checkAnswer(spark.createDataFrame(v2Rows, v2Schema), v1Rows);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] ROW DIVERGENCE(startingVersion=1)\n  DSv1 rows ("
                + v1Rows.size()
                + "): "
                + truncate(v1Rows)
                + "\n  DSv2 rows ("
                + v2Rows.size()
                + "): "
                + truncate(v2Rows)
                + "\n  diff: "
                + rootMessage(t));
      }
    }

    System.out.println("=== testAllGoldenTablesStreamingFromVersion1 summary ===");
    System.out.println("Total golden tables: " + tableNames.size());
    System.out.println("Tested: " + tested);
    System.out.println("Skipped (unsupportedTables allowlist): " + skippedUnsupported);
    System.out.println("Skipped (no top-level data dir): " + skippedNoData);
    System.out.println("Skipped (non-append history): " + skippedNonAppend);
    System.out.println("Skipped (single version): " + skippedSingleVersion);
    System.out.println("Skipped (setup failure / both sides failed): " + skippedSetupFailure);
    System.out.println("Tested tables: " + testedTableNames);
    System.out.println("Divergences found: " + divergences.size());
    for (String d : divergences) {
      System.out.println("---");
      System.out.println(d);
    }

    DeltaLog.clearCache();

    if (!divergences.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(divergences.size())
          .append(" DSv1 vs DSv2 streaming(startingVersion=1) divergence(s) across ")
          .append(tested)
          .append(" tested golden tables:\n");
      for (String d : divergences) {
        sb.append("---\n").append(d).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  /**
   * Differential streaming with single-column projection.
   *
   * <p>Picks one non-partition leaf column per table and applies {@code .select(col)} to both DSv1
   * and DSv2 streams. Diffs the returned rows AND schema. Targets {@code
   * SupportsPushDownRequiredColumns} interaction with streaming (column pruning + partition column
   * injection).
   */
  @Test
  public void testAllGoldenTablesStreamingWithProjection() throws Exception {
    List<String> tableNames = getAllGoldenTableNames();
    List<String> unsupportedTables =
        Arrays.asList(
            "canonicalized-paths-normal-a",
            "canonicalized-paths-normal-b",
            "canonicalized-paths-special-a",
            "canonicalized-paths-special-b",
            "checkpoint",
            "corrupted-last-checkpoint",
            "data-reader-absolute-paths-escaped-chars",
            "data-reader-escaped-chars",
            "delete-re-add-same-file-different-transactions",
            "deltalog-commit-info",
            "deltalog-invalid-protocol-version",
            "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol");

    int tested = 0;
    int skippedUnsupported = 0;
    int skippedNoData = 0;
    int skippedNonAppend = 0;
    int skippedNoColumn = 0;
    int skippedSetupFailure = 0;
    List<String> divergences = new ArrayList<>();
    List<String> testedTableNames = new ArrayList<>();

    for (String tableName : tableNames) {
      if (unsupportedTables.contains(tableName)) {
        skippedUnsupported++;
        continue;
      }
      String tablePath = goldenTablePath(tableName);
      if (hasOnlyDeltaLogSubdir(tablePath)) {
        skippedNoData++;
        continue;
      }
      try {
        if (hasNonAppendHistory(tablePath)) {
          skippedNonAppend++;
          continue;
        }
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }

      // Pick a non-partition leaf column. Fall back to first column if everything is partition.
      String selectedCol;
      Set<String> partitionCols;
      try {
        partitionCols = readPartitionColumns(tablePath);
        StructType schema = spark.readStream().format("delta").load(tablePath).schema();
        selectedCol = pickProjectionColumn(schema, partitionCols);
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }
      if (selectedCol == null) {
        skippedNoColumn++;
        continue;
      }

      String safeName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
      String q1Name = "gtproj_v1_" + safeName + "_" + System.nanoTime();
      String q2Name = "gtproj_v2_" + safeName + "_" + System.nanoTime();

      List<Row> v1Rows;
      StructType v1Schema;
      try {
        Dataset<Row> v1Stream =
            spark.readStream().format("delta").load(tablePath).select(selectedCol);
        v1Schema = v1Stream.schema();
        v1Rows = collectStream(v1Stream, q1Name);
      } catch (Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
            .append(tableName)
            .append("] DSv1 streaming(.select(")
            .append(selectedCol)
            .append(")) failed: ")
            .append(rootMessage(t));
        try {
          Dataset<Row> v2Stream =
              spark.readStream().table("dsv2.delta.`" + tablePath + "`").select(selectedCol);
          List<Row> v2Rows = collectStream(v2Stream, q2Name);
          sb.append(" | DSv2 SUCCEEDED — DIVERGENCE: DSv2 returned ")
              .append(v2Rows.size())
              .append(" rows while DSv1 threw: ")
              .append(t.getClass().getSimpleName());
          divergences.add(sb.toString());
        } catch (Throwable t2) {
          skippedSetupFailure++;
        }
        continue;
      }

      List<Row> v2Rows;
      StructType v2Schema;
      try {
        Dataset<Row> v2Stream =
            spark.readStream().table("dsv2.delta.`" + tablePath + "`").select(selectedCol);
        v2Schema = v2Stream.schema();
        v2Rows = collectStream(v2Stream, q2Name);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] DIVERGENCE(.select("
                + selectedCol
                + ")): DSv1 returned "
                + v1Rows.size()
                + " rows; DSv2 threw "
                + t.getClass().getName()
                + ": "
                + rootMessage(t));
        continue;
      }

      tested++;
      testedTableNames.add(tableName + "[" + selectedCol + "]");

      if (!v1Schema.equals(v2Schema)) {
        divergences.add(
            "["
                + tableName
                + "] SCHEMA DIVERGENCE(.select("
                + selectedCol
                + "))\n  DSv1: "
                + v1Schema.treeString()
                + "\n  DSv2: "
                + v2Schema.treeString());
        continue;
      }

      try {
        QueryTest$.MODULE$.checkAnswer(spark.createDataFrame(v2Rows, v2Schema), v1Rows);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] ROW DIVERGENCE(.select("
                + selectedCol
                + "))\n  DSv1 rows ("
                + v1Rows.size()
                + "): "
                + truncate(v1Rows)
                + "\n  DSv2 rows ("
                + v2Rows.size()
                + "): "
                + truncate(v2Rows)
                + "\n  diff: "
                + rootMessage(t));
      }
    }

    System.out.println("=== testAllGoldenTablesStreamingWithProjection summary ===");
    System.out.println("Total golden tables: " + tableNames.size());
    System.out.println("Tested: " + tested);
    System.out.println("Skipped (unsupportedTables allowlist): " + skippedUnsupported);
    System.out.println("Skipped (no top-level data dir): " + skippedNoData);
    System.out.println("Skipped (non-append history): " + skippedNonAppend);
    System.out.println("Skipped (no projectable column): " + skippedNoColumn);
    System.out.println("Skipped (setup failure / both sides failed): " + skippedSetupFailure);
    System.out.println("Tested tables: " + testedTableNames);
    System.out.println("Divergences found: " + divergences.size());
    for (String d : divergences) {
      System.out.println("---");
      System.out.println(d);
    }

    DeltaLog.clearCache();

    if (!divergences.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(divergences.size())
          .append(" DSv1 vs DSv2 streaming(.select(col)) divergence(s) across ")
          .append(tested)
          .append(" tested golden tables:\n");
      for (String d : divergences) {
        sb.append("---\n").append(d).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  /**
   * Differential streaming with a trivially-true filter pushed down.
   *
   * <p>For each table, picks a leaf column and applies {@code col IS NOT NULL} as a streaming
   * {@code .where(...)} on both DSv1 and DSv2. The trivially-permissive variant should still return
   * all rows whose value is not null, so the row counts must match between the two readers
   * (regardless of how many rows are non-null). Targets {@code SupportsPushDownFilters} interaction
   * with streaming offset management.
   */
  @Test
  public void testAllGoldenTablesStreamingWithFilter() throws Exception {
    List<String> tableNames = getAllGoldenTableNames();
    List<String> unsupportedTables =
        Arrays.asList(
            "canonicalized-paths-normal-a",
            "canonicalized-paths-normal-b",
            "canonicalized-paths-special-a",
            "canonicalized-paths-special-b",
            "checkpoint",
            "corrupted-last-checkpoint",
            "data-reader-absolute-paths-escaped-chars",
            "data-reader-escaped-chars",
            "delete-re-add-same-file-different-transactions",
            "deltalog-commit-info",
            "deltalog-invalid-protocol-version",
            "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol");

    int tested = 0;
    int skippedUnsupported = 0;
    int skippedNoData = 0;
    int skippedNonAppend = 0;
    int skippedNoColumn = 0;
    int skippedSetupFailure = 0;
    List<String> divergences = new ArrayList<>();
    List<String> testedTableNames = new ArrayList<>();

    for (String tableName : tableNames) {
      if (unsupportedTables.contains(tableName)) {
        skippedUnsupported++;
        continue;
      }
      String tablePath = goldenTablePath(tableName);
      if (hasOnlyDeltaLogSubdir(tablePath)) {
        skippedNoData++;
        continue;
      }
      try {
        if (hasNonAppendHistory(tablePath)) {
          skippedNonAppend++;
          continue;
        }
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }

      String filterCol;
      try {
        StructType schema = spark.readStream().format("delta").load(tablePath).schema();
        filterCol = pickFilterColumn(schema);
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }
      if (filterCol == null) {
        skippedNoColumn++;
        continue;
      }
      // Backtick-quote so columns with hyphens / spaces / dots still parse.
      String predicate = "`" + filterCol.replace("`", "``") + "` IS NOT NULL";

      String safeName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
      String q1Name = "gtfilt_v1_" + safeName + "_" + System.nanoTime();
      String q2Name = "gtfilt_v2_" + safeName + "_" + System.nanoTime();

      List<Row> v1Rows;
      StructType v1Schema;
      try {
        Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath).where(predicate);
        v1Schema = v1Stream.schema();
        v1Rows = collectStream(v1Stream, q1Name);
      } catch (Throwable t) {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
            .append(tableName)
            .append("] DSv1 streaming(.where(")
            .append(predicate)
            .append(")) failed: ")
            .append(rootMessage(t));
        try {
          Dataset<Row> v2Stream =
              spark.readStream().table("dsv2.delta.`" + tablePath + "`").where(predicate);
          List<Row> v2Rows = collectStream(v2Stream, q2Name);
          sb.append(" | DSv2 SUCCEEDED — DIVERGENCE: DSv2 returned ")
              .append(v2Rows.size())
              .append(" rows while DSv1 threw: ")
              .append(t.getClass().getSimpleName());
          divergences.add(sb.toString());
        } catch (Throwable t2) {
          skippedSetupFailure++;
        }
        continue;
      }

      List<Row> v2Rows;
      StructType v2Schema;
      try {
        Dataset<Row> v2Stream =
            spark.readStream().table("dsv2.delta.`" + tablePath + "`").where(predicate);
        v2Schema = v2Stream.schema();
        v2Rows = collectStream(v2Stream, q2Name);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] DIVERGENCE(.where("
                + predicate
                + ")): DSv1 returned "
                + v1Rows.size()
                + " rows; DSv2 threw "
                + t.getClass().getName()
                + ": "
                + rootMessage(t));
        continue;
      }

      tested++;
      testedTableNames.add(tableName + "[" + filterCol + "]");

      if (!v1Schema.equals(v2Schema)) {
        divergences.add(
            "["
                + tableName
                + "] SCHEMA DIVERGENCE(.where("
                + predicate
                + "))\n  DSv1: "
                + v1Schema.treeString()
                + "\n  DSv2: "
                + v2Schema.treeString());
        continue;
      }

      try {
        QueryTest$.MODULE$.checkAnswer(spark.createDataFrame(v2Rows, v2Schema), v1Rows);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] ROW DIVERGENCE(.where("
                + predicate
                + "))\n  DSv1 rows ("
                + v1Rows.size()
                + "): "
                + truncate(v1Rows)
                + "\n  DSv2 rows ("
                + v2Rows.size()
                + "): "
                + truncate(v2Rows)
                + "\n  diff: "
                + rootMessage(t));
      }
    }

    System.out.println("=== testAllGoldenTablesStreamingWithFilter summary ===");
    System.out.println("Total golden tables: " + tableNames.size());
    System.out.println("Tested: " + tested);
    System.out.println("Skipped (unsupportedTables allowlist): " + skippedUnsupported);
    System.out.println("Skipped (no top-level data dir): " + skippedNoData);
    System.out.println("Skipped (non-append history): " + skippedNonAppend);
    System.out.println("Skipped (no filterable column): " + skippedNoColumn);
    System.out.println("Skipped (setup failure / both sides failed): " + skippedSetupFailure);
    System.out.println("Tested tables: " + testedTableNames);
    System.out.println("Divergences found: " + divergences.size());
    for (String d : divergences) {
      System.out.println("---");
      System.out.println(d);
    }

    DeltaLog.clearCache();

    if (!divergences.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(divergences.size())
          .append(" DSv1 vs DSv2 streaming(.where(col IS NOT NULL)) divergence(s) across ")
          .append(tested)
          .append(" tested golden tables:\n");
      for (String d : divergences) {
        sb.append("---\n").append(d).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  /** Number of {@code N.json} commit files under the table's _delta_log. */
  private int countCommitJsonFiles(String tablePath) {
    File logDir = new File(tablePath, "_delta_log");
    if (!logDir.isDirectory()) return 0;
    File[] files = logDir.listFiles((d, n) -> n.endsWith(".json"));
    return files == null ? 0 : files.length;
  }

  /**
   * Read the partition columns from the table's most recent commit-time metadata. Returns the empty
   * set on any error.
   */
  private Set<String> readPartitionColumns(String tablePath) {
    Set<String> result = new LinkedHashSet<>();
    try {
      DeltaLog log = DeltaLog.forTable(spark, tablePath);
      scala.collection.immutable.List<String> partCols =
          log.unsafeVolatileSnapshot().metadata().partitionColumns().toList();
      scala.collection.Iterator<String> it = partCols.iterator();
      while (it.hasNext()) result.add(it.next());
    } catch (Throwable ignored) {
    }
    return result;
  }

  /**
   * Pick the first non-partition leaf column that's safe to project. Falls back to the first leaf
   * column if everything is a partition column. Returns {@code null} if no leaf columns exist.
   */
  private String pickProjectionColumn(StructType schema, Set<String> partitionCols) {
    StructField fallback = null;
    for (StructField f : schema.fields()) {
      if (fallback == null) fallback = f;
      if (!partitionCols.contains(f.name())) {
        return f.name();
      }
    }
    return fallback == null ? null : fallback.name();
  }

  /**
   * Pick a leaf column suitable for an {@code IS NOT NULL} streaming filter. We prefer top-level
   * primitive columns to keep the filter pushdown unambiguous; any column type works for the
   * trivial-true case but nested types tend to produce noisier diffs.
   */
  private String pickFilterColumn(StructType schema) {
    for (StructField f : schema.fields()) {
      if (f.dataType() instanceof org.apache.spark.sql.types.NumericType
          || f.dataType() instanceof org.apache.spark.sql.types.StringType
          || f.dataType() instanceof org.apache.spark.sql.types.BooleanType
          || f.dataType() instanceof org.apache.spark.sql.types.DateType
          || f.dataType() instanceof org.apache.spark.sql.types.TimestampType) {
        return f.name();
      }
    }
    // Fall back to first column (any type) so we still exercise filter pushdown.
    return schema.fields().length > 0 ? schema.fields()[0].name() : null;
  }

  /** Collect a streaming DataFrame end-to-end with Trigger.AvailableNow into a list of rows. */
  private List<Row> collectStreamOnce(Dataset<Row> streamingDF, String queryName) throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      query.processAllAvailable();
      return spark.sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      if (query != null) {
        try {
          query.stop();
        } catch (Throwable ignored) {
        }
      }
      try {
        spark.sql("DROP VIEW IF EXISTS " + queryName);
      } catch (Throwable ignored) {
      }
    }
  }

  /** Collect a streaming DataFrame end-to-end with Trigger.AvailableNow into a list of rows. */
  private List<Row> collectStream(Dataset<Row> streamingDF, String queryName) throws Exception {
    StreamingQuery query = null;
    try {
      query =
          streamingDF
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      // AvailableNow + processAllAvailable terminates after draining all currently-available data.
      query.processAllAvailable();
      return spark.sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      if (query != null) {
        try {
          query.stop();
        } catch (Throwable ignored) {
        }
      }
      try {
        spark.sql("DROP VIEW IF EXISTS " + queryName);
      } catch (Throwable ignored) {
      }
    }
  }

  /**
   * Returns true if any commit JSON in the table's _delta_log contains a "remove" action,
   * indicating delete/overwrite/restore/merge/update history. Such tables aren't usable as basic
   * append-only streaming sources without ignoreDeletes/ignoreChanges.
   */
  private boolean hasNonAppendHistory(String tablePath) throws Exception {
    File logDir = new File(tablePath, "_delta_log");
    if (!logDir.isDirectory()) return false;
    File[] files = logDir.listFiles((d, n) -> n.endsWith(".json"));
    if (files == null) return false;
    for (File f : files) {
      try (BufferedReader r = new BufferedReader(new FileReader(f))) {
        String line;
        while ((line = r.readLine()) != null) {
          // Lines are one-action-per-line JSON; "remove" appears as a top-level key only when the
          // line encodes a RemoveFile action.
          if (line.startsWith("{\"remove\"") || line.contains("\"remove\":{")) {
            return true;
          }
        }
      }
    }
    return false;
  }

  private String rootMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    String msg = cur.getMessage();
    return cur.getClass().getName() + ": " + (msg == null ? "(no message)" : msg);
  }

  private String truncate(List<Row> rows) {
    int max = 10;
    if (rows.size() <= max) return rows.toString();
    return rows.subList(0, max).toString() + "... (+" + (rows.size() - max) + " more)";
  }

  private void verifyHadoopConf(Configuration conf) {
    assertEquals("value1", conf.get("key1"));
    assertEquals("new_value2", conf.get("key2"));
    assertEquals("value3", conf.get("key3"));
  }

  private boolean hasOnlyDeltaLogSubdir(String path) {
    File dir = new File(path);
    if (!dir.exists() || !dir.isDirectory()) {
      return false;
    }

    File[] subFiles = dir.listFiles(File::isDirectory);
    if (subFiles == null) {
      return false;
    }

    // Check: only one subdirectory, and it is "_delta_log"
    return subFiles.length == 1 && "_delta_log".equals(subFiles[0].getName());
  }

  private void checkTable(String path, List<Row> expected) {
    String tablePath = goldenTablePath(path);
    Dataset<Row> df = spark.sql("SELECT * FROM `dsv2`.`delta`.`" + tablePath + "`");
    checkAnswer(df, expected);
  }

  private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
  }

  private List<String> getAllGoldenTableNames() {
    return scala.collection.JavaConverters.seqAsJavaList(GoldenTableUtils$.MODULE$.allTableNames());
  }

  @Test
  public void testAllGoldenTablesStreamingWithMaxFilesPerTrigger() throws Exception {
    runRateLimitedDifferential(
        "maxFilesPerTrigger", "1", "testAllGoldenTablesStreamingWithMaxFilesPerTrigger");
  }

  /**
   * Differential streaming corpus test with {@code maxBytesPerTrigger=1b} on the DSv2 side. Even
   * tighter than {@code maxFilesPerTrigger=1} — Delta's rate limiter still includes at least one
   * file per batch, but byte-budget bookkeeping runs per batch which is a separate code path.
   */
  @Test
  public void testAllGoldenTablesStreamingWithMaxBytesPerTrigger() throws Exception {
    runRateLimitedDifferential(
        "maxBytesPerTrigger", "1b", "testAllGoldenTablesStreamingWithMaxBytesPerTrigger");
  }

  /**
   * Shared body of the two rate-limited differential corpus tests. Iterates the same set of golden
   * tables as {@link #testAllGoldenTablesStreaming}, applies the given DSv2 reader option, and
   * compares the union of all DSv2 micro-batches against an unrestricted DSv1 one-shot oracle.
   */
  private void runRateLimitedDifferential(String optionName, String optionValue, String tagForLogs)
      throws Exception {
    List<String> tableNames = getAllGoldenTableNames();
    // Same allowlist as testAllGoldenTablesStreaming.
    List<String> unsupportedTables =
        Arrays.asList(
            "canonicalized-paths-normal-a",
            "canonicalized-paths-normal-b",
            "canonicalized-paths-special-a",
            "canonicalized-paths-special-b",
            "checkpoint",
            "corrupted-last-checkpoint",
            "data-reader-absolute-paths-escaped-chars",
            "data-reader-escaped-chars",
            "delete-re-add-same-file-different-transactions",
            "deltalog-commit-info",
            "deltalog-invalid-protocol-version",
            "deltalog-state-reconstruction-from-checkpoint-missing-metadata",
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol");

    int tested = 0;
    int skippedUnsupported = 0;
    int skippedNoData = 0;
    int skippedNonAppend = 0;
    int skippedSetupFailure = 0;
    List<String> divergences = new ArrayList<>();
    List<String> testedTableNames = new ArrayList<>();

    for (String tableName : tableNames) {
      if (unsupportedTables.contains(tableName)) {
        skippedUnsupported++;
        continue;
      }
      String tablePath = goldenTablePath(tableName);
      if (hasOnlyDeltaLogSubdir(tablePath)) {
        skippedNoData++;
        continue;
      }
      try {
        if (hasNonAppendHistory(tablePath)) {
          skippedNonAppend++;
          continue;
        }
      } catch (Throwable t) {
        skippedSetupFailure++;
        continue;
      }

      String safeName = tableName.replaceAll("[^a-zA-Z0-9]", "_");
      String q1Name = "gt_v1_rl_" + safeName + "_" + System.nanoTime();
      String q2Name = "gt_v2_rl_" + safeName + "_" + System.nanoTime();

      // 1. DSv1 oracle — no rate limit, single AvailableNow batch.
      List<Row> v1Rows;
      StructType v1Schema;
      try {
        Dataset<Row> v1Stream = spark.readStream().format("delta").load(tablePath);
        v1Schema = v1Stream.schema();
        v1Rows = collectStream(v1Stream, q1Name);
      } catch (Throwable t) {
        // DSv1 can't read this table — try DSv2 with the rate limit anyway: asymmetric failure
        // is itself a divergence.
        StringBuilder sb = new StringBuilder();
        sb.append("[")
            .append(tableName)
            .append("] DSv1 streaming failed (DSv2 not yet attempted): ")
            .append(rootMessage(t));
        try {
          Dataset<Row> v2Stream =
              spark
                  .readStream()
                  .option(optionName, optionValue)
                  .table("dsv2.delta.`" + tablePath + "`");
          List<Row> v2Rows = collectStream(v2Stream, q2Name);
          sb.append(" | DSv2 (")
              .append(optionName)
              .append("=")
              .append(optionValue)
              .append(") SUCCEEDED — DIVERGENCE: DSv2 returned ")
              .append(v2Rows.size())
              .append(" rows while DSv1 threw: ")
              .append(t.getClass().getSimpleName());
          divergences.add(sb.toString());
        } catch (Throwable t2) {
          skippedSetupFailure++;
        }
        continue;
      }

      // 2. DSv2 with rate limit — multiple micro-batches expected.
      List<Row> v2Rows;
      StructType v2Schema;
      try {
        Dataset<Row> v2Stream =
            spark
                .readStream()
                .option(optionName, optionValue)
                .table("dsv2.delta.`" + tablePath + "`");
        v2Schema = v2Stream.schema();
        v2Rows = collectStream(v2Stream, q2Name);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] DIVERGENCE: DSv1 returned "
                + v1Rows.size()
                + " rows; DSv2 ("
                + optionName
                + "="
                + optionValue
                + ") threw "
                + t.getClass().getName()
                + ": "
                + rootMessage(t));
        continue;
      }

      tested++;
      testedTableNames.add(tableName);

      if (!v1Schema.equals(v2Schema)) {
        divergences.add(
            "["
                + tableName
                + "] SCHEMA DIVERGENCE\n  DSv1: "
                + v1Schema.treeString()
                + "\n  DSv2: "
                + v2Schema.treeString());
        continue;
      }

      try {
        QueryTest$.MODULE$.checkAnswer(spark.createDataFrame(v2Rows, v2Schema), v1Rows);
      } catch (Throwable t) {
        divergences.add(
            "["
                + tableName
                + "] ROW DIVERGENCE\n  DSv1 rows ("
                + v1Rows.size()
                + "): "
                + truncate(v1Rows)
                + "\n  DSv2 rows ("
                + v2Rows.size()
                + "): "
                + truncate(v2Rows)
                + "\n  diff: "
                + rootMessage(t));
      }
    }

    System.out.println("=== " + tagForLogs + " summary ===");
    System.out.println("Total golden tables: " + tableNames.size());
    System.out.println("Tested: " + tested);
    System.out.println("Skipped (unsupportedTables allowlist): " + skippedUnsupported);
    System.out.println("Skipped (no top-level data dir): " + skippedNoData);
    System.out.println("Skipped (non-append history): " + skippedNonAppend);
    System.out.println("Skipped (setup failure / both sides failed): " + skippedSetupFailure);
    System.out.println("Tested tables: " + testedTableNames);
    System.out.println("Divergences found: " + divergences.size());
    for (String d : divergences) {
      System.out.println("---");
      System.out.println(d);
    }

    DeltaLog.clearCache();

    if (!divergences.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(divergences.size())
          .append(" DSv1 vs DSv2 streaming divergence(s) (")
          .append(optionName)
          .append("=")
          .append(optionValue)
          .append(") across ")
          .append(tested)
          .append(" tested golden tables:\n");
      for (String d : divergences) {
        sb.append("---\n").append(d).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }
}
