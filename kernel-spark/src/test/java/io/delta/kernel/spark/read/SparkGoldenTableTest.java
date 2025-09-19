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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.golden.GoldenTableUtils$;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.spark.table.SparkTable;
import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.QueryTest;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import scala.Function0;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkGoldenTableTest extends QueryTest {

  private SparkSession spark;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .setMaster("local[*]")
            .setAppName("SparkGoldenTableTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @Override
  public SparkSession spark() {
    return spark;
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
    assertEquals(tableName, table.name());
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
    assertEquals(options.asCaseSensitiveMap(), table.properties());

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
          new StringStartsWith("name", "foo"), // unsupported data filter
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
        new Filter[] {new GreaterThan("cnt", 10), new EqualTo("date", "2025-09-01")},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(">", new Column("cnt"), Literal.ofInt(10)),
          new Predicate("=", new Column("date"), Literal.ofString("2025-09-01"))
        },
        // expected data filters
        new Filter[] {new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")},
        // expected kernel scan builder predicate
        Optional.of(
            new Predicate(
                "AND",
                new Predicate(">", new Column("cnt"), Literal.ofInt(10)),
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
          new Or(new EqualTo("cnt", 50), new EqualTo("date", "2025-10-01")),
          new Not(new And(new GreaterThan("cnt", 100), new EqualTo("date", "2025-09-01"))),
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "OR",
              new Predicate("=", new Column("cnt"), Literal.ofInt(50)),
              new Predicate("=", new Column("date"), Literal.ofString("2025-10-01"))),
          new Predicate(
              "NOT",
              new Predicate(
                  "AND",
                  new Predicate(">", new Column("cnt"), Literal.ofInt(100)),
                  new Predicate("=", new Column("date"), Literal.ofString("2025-09-01"))))
        },
        // expected data filters
        new Filter[] {
          new Or(new GreaterThan("cnt", 10), new StringStartsWith("name", "foo")),
          new Or(new EqualTo("cnt", 50), new EqualTo("date", "2025-10-01")),
          new Not(new And(new GreaterThan("cnt", 100), new EqualTo("date", "2025-09-01"))),
          new Not(new Or(new EqualTo("name", "foo"), new StringStartsWith("city", "New")))
        },
        // expected kernel scan builder predicate
        Optional.of(
            new Predicate(
                "AND",
                new Predicate(
                    "OR",
                    new Predicate("=", new Column("cnt"), Literal.ofInt(50)),
                    new Predicate("=", new Column("date"), Literal.ofString("2025-10-01"))),
                new Predicate(
                    "NOT",
                    new Predicate(
                        "AND",
                        new Predicate(">", new Column("cnt"), Literal.ofInt(100)),
                        new Predicate("=", new Column("date"), Literal.ofString("2025-09-01")))))));
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
    assertEquals(tableName, table.name());
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

    Function0<Dataset<Row>> dfFunc =
        new Function0<Dataset<Row>>() {
          @Override
          public Dataset<Row> apply() {
            return df;
          }
        };
    scala.collection.immutable.Seq<Row> expectedSeq =
        scala.collection.JavaConverters.asScalaBuffer(expected).toList();
    checkAnswer(dfFunc, expectedSeq);
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
            "data-reader-timestamp_ntz-id-mode",
            "data-reader-timestamp_ntz-name-mode",
            "data-skipping-basic-stats-all-types-columnmapping-id",
            "data-skipping-basic-stats-all-types-columnmapping-name",
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
            "deltalog-state-reconstruction-from-checkpoint-missing-protocol",
            // Answer mismatch
            "dv-partitioned-with-checkpoint",
            "dv-with-columnmapping");

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
      checkAnswer(
          new Function0<Dataset<Row>>() {
            @Override
            public Dataset<Row> apply() {
              return df;
            }
          },
          df2);
    }
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
    Function0<Dataset<Row>> dfFunc =
        new Function0<Dataset<Row>>() {
          @Override
          public Dataset<Row> apply() {
            return df;
          }
        };

    scala.collection.immutable.Seq<Row> expectedSeq =
        scala.collection.JavaConverters.asScalaBuffer(expected).toList();
    checkAnswer(dfFunc, expectedSeq);
  }

  private String goldenTablePath(String name) {
    return GoldenTableUtils$.MODULE$.goldenTablePath(name);
  }

  private List<String> getAllGoldenTableNames() {
    return scala.collection.JavaConverters.seqAsJavaList(GoldenTableUtils$.MODULE$.allTableNames());
  }
}
