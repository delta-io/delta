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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanBuilderTest extends DeltaV2TestBase {

  @Test
  public void testBuild_returnsScanWithExpectedSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            snapshot,
            snapshotManager,
            dataSchema,
            partitionSchema,
            tableSchema,
            Optional.empty(),
            CaseInsensitiveStringMap.empty());

    StructType expectedSparkSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true /*nullable*/),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });

    builder.pruneColumns(expectedSparkSchema);
    Scan scan = builder.build();

    assertTrue(scan instanceof SparkScan);
    assertEquals(expectedSparkSchema, scan.readSchema());
  }

  @Test
  public void testPruneColumns_filtersMixedCaseCDCColumn(@TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_mixed_case_cdc_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            snapshot,
            snapshotManager,
            dataSchema,
            partitionSchema,
            tableSchema,
            Optional.empty(),
            CaseInsensitiveStringMap.empty());

    StructType requiredSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("_Change_Type", DataTypes.StringType, true)
            });
    builder.pruneColumns(requiredSchema);

    StructType requiredDataSchema = getRequiredDataSchema(builder);
    for (StructField f : requiredDataSchema.fields()) {
      assertTrue(
          !f.name().equalsIgnoreCase("_change_type"),
          "Mixed-case CDC column survived pruneColumns: " + f.name());
    }
  }

  private StructType getRequiredDataSchema(SparkScanBuilder builder) throws Exception {
    Field field = SparkScanBuilder.class.getDeclaredField("requiredDataSchema");
    field.setAccessible(true);
    return (StructType) field.get(builder);
  }

  @Test
  public void testToMicroBatchStream_returnsSparkMicroBatchStream(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "microbatch_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            snapshot,
            snapshotManager,
            dataSchema,
            partitionSchema,
            tableSchema,
            Optional.empty(),
            CaseInsensitiveStringMap.empty());
    Scan scan = builder.build();

    String checkpointLocation = "/tmp/checkpoint";
    MicroBatchStream microBatchStream = scan.toMicroBatchStream(checkpointLocation);

    assertNotNull(microBatchStream, "MicroBatchStream should not be null");
    assertTrue(
        microBatchStream instanceof SparkMicroBatchStream,
        "MicroBatchStream should be an instance of SparkMicroBatchStream");
  }

  @Test
  public void testPushFilters_singleSupportedDataFilter(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new EqualTo("id", 100)},
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100)},
        // expected pushed filters
        new Filter[] {new EqualTo("id", 100)},
        // expected pushed kernel predicates
        new Predicate[] {new Predicate("=", new Column("id"), Literal.ofInt(100))},
        // expected data filters
        new Filter[] {new EqualTo("id", 100)},
        // expected kernelScanBuilder.predicate
        Optional.of(new Predicate("=", new Column("id"), Literal.ofInt(100))));
  }

  @Test
  public void testPushFilters_singleUnsupportedDataFilter(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        new Filter[] {new StringEndsWith("name", "test")}, // input filters
        new Filter[] {
          new StringEndsWith("name", "test")
        }, // expected post-scan filters (unsupported, stays for row-level eval)
        new Filter[] {}, // expected pushed filters (nothing pushed)
        new Predicate[] {}, // expected pushed kernel predicates
        new Filter[] {new StringEndsWith("name", "test")}, // expected data filters
        Optional.empty() // expected kernelScanBuilder.predicate
        );
  }

  @Test
  public void testPushFilters_singleSupportedDataFilter_StringStartsWith(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        new Filter[] {new StringStartsWith("name", "test")}, // input filters
        new Filter[] {
          new StringStartsWith("name", "test")
        }, // expected post-scan filters (data filter still needs row-level eval)
        new Filter[] {new StringStartsWith("name", "test")}, // expected pushed filters
        new Predicate[] {
          new Predicate("STARTS_WITH", new Column("name"), Literal.ofString("test"))
        }, // expected pushed kernel predicates
        new Filter[] {new StringStartsWith("name", "test")}, // expected data filters
        Optional.of(
            new Predicate(
                "STARTS_WITH",
                new Column("name"),
                Literal.ofString("test"))) // expected kernelScanBuilder.predicate
        );
  }

  @Test
  public void testPushFilters_multiSupportedDataFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new EqualTo("id", 100), new GreaterThan("id", 50)},
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100), new GreaterThan("id", 50)},
        // expected pushed filters
        new Filter[] {new EqualTo("id", 100), new GreaterThan("id", 50)},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("id"), Literal.ofInt(100)),
          new Predicate(">", new Column("id"), Literal.ofInt(50))
        },
        // expected data filters
        new Filter[] {new EqualTo("id", 100), new GreaterThan("id", 50)},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                Arrays.asList(
                    new Predicate("=", new Column("id"), Literal.ofInt(100)),
                    new Predicate(">", new Column("id"), Literal.ofInt(50))))));
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new EqualTo("id", 100), // supported
          new StringEndsWith("name", "test") // unsupported
        },
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100), new StringEndsWith("name", "test")},
        // expected pushed filters (only the supported EqualTo is pushed)
        new Filter[] {new EqualTo("id", 100)},
        // expected pushed kernel predicates
        new Predicate[] {new Predicate("=", new Column("id"), Literal.ofInt(100))},
        // expected data filters
        new Filter[] {new EqualTo("id", 100), new StringEndsWith("name", "test")},
        // expected kernelScanBuilder.predicate (only EqualTo was pushed to kernel)
        Optional.of(new Predicate("=", new Column("id"), Literal.ofInt(100))));
  }

  @Test
  public void testPushFilters_singleSupportedPartitionFilter(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new EqualTo("dep_id", 1)},
        // expected post-scan filters
        new Filter[] {},
        // expected pushed filters
        new Filter[] {new EqualTo("dep_id", 1)},
        // expected pushed kernel predicates
        new Predicate[] {new Predicate("=", new Column("dep_id"), Literal.ofInt(1))},
        // expected data filters
        new Filter[] {},
        // expected kernelScanBuilder.predicate
        Optional.of(new Predicate("=", new Column("dep_id"), Literal.ofInt(1))));
  }

  @Test
  public void testPushFilters_singleUnsupportedPartitionFilter(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        new Filter[] {new StringStartsWith("dep_id", "1")}, // input filters
        new Filter[] {}, // expected post-scan filters (partition filter, fully pushed)
        new Filter[] {new StringStartsWith("dep_id", "1")}, // expected pushed filters
        new Predicate[] {
          new Predicate("STARTS_WITH", new Column("dep_id"), Literal.ofString("1"))
        }, // expected pushed kernel predicates
        new Filter[] {}, // expected data filters
        Optional.of(
            new Predicate(
                "STARTS_WITH",
                new Column("dep_id"),
                Literal.ofString("1"))) // expected kernelScanBuilder.predicate
        );
  }

  @Test
  public void testPushFilters_multiSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new EqualTo("dep_id", 2), new GreaterThan("dep_id", 1)},
        // expected post-scan filters
        new Filter[] {},
        // expected pushed filters
        new Filter[] {new EqualTo("dep_id", 2), new GreaterThan("dep_id", 1)},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("dep_id"), Literal.ofInt(2)),
          new Predicate(">", new Column("dep_id"), Literal.ofInt(1))
        },
        // expected data filters
        new Filter[] {},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                Arrays.asList(
                    new Predicate("=", new Column("dep_id"), Literal.ofInt(2)),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(1))))));
  }

  @Test
  public void testPushFilters_mixedSupportedAndUnsupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new EqualTo("dep_id", 1), // supported
          new StringStartsWith("dep_id", "1") // unsupported
        },
        // expected post-scan filters
        new Filter[] {},
        // expected pushed filters
        new Filter[] {new EqualTo("dep_id", 1), new StringStartsWith("dep_id", "1")},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("dep_id"), Literal.ofInt(1)),
          new Predicate("STARTS_WITH", new Column("dep_id"), Literal.ofString("1"))
        },
        // expected data filters
        new Filter[] {},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                new Predicate("=", new Column("dep_id"), Literal.ofInt(1)),
                new Predicate("STARTS_WITH", new Column("dep_id"), Literal.ofString("1")))));
  }

  @Test
  public void testPushFilters_mixedFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new EqualTo("id", 100), // data filter, supported
          new StringStartsWith("name", "foo"), // data filter, unsupported
          new GreaterThan("dep_id", 1), // partition filter, supported
          new StringEndsWith("dep_id", "1") // partition filter, unsupported
        },
        // expected post-scan filters
        new Filter[] {
          new EqualTo("id", 100), // data filter, supported
          new StringStartsWith("name", "foo"), // data filter, unsupported
          new StringEndsWith("dep_id", "1") // partition filter, unsupported
        },
        // expected pushed filters
        new Filter[] {
          new EqualTo("id", 100), // data filter, supported
          new StringStartsWith("name", "foo"), // data filter, supported
          new GreaterThan("dep_id", 1) // partition filter, supported
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("id"), Literal.ofInt(100)),
          new Predicate("STARTS_WITH", new Column("name"), Literal.ofString("foo")),
          new Predicate(">", new Column("dep_id"), Literal.ofInt(1))
        },
        // expected data filters
        new Filter[] {
          new EqualTo("id", 100), // data filter, supported
          new StringStartsWith("name", "foo") // data filter, supported
        },
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                new Predicate(
                    "AND",
                    new Predicate("=", new Column("id"), Literal.ofInt(100)),
                    new Predicate("STARTS_WITH", new Column("name"), Literal.ofString("foo"))),
                new Predicate(">", new Column("dep_id"), Literal.ofInt(1)))));
  }

  @Test
  public void testPushFilters_ORFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("id", 50))},
        // expected post-scan filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("id", 50))},
        // expected pushed filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("id", 50))},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "OR",
              Arrays.asList(
                  new Predicate("=", new Column("id"), Literal.ofInt(100)),
                  new Predicate(">", new Column("id"), Literal.ofInt(50))))
        },
        // expected data filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("id", 50))},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "OR",
                Arrays.asList(
                    new Predicate("=", new Column("id"), Literal.ofInt(100)),
                    new Predicate(">", new Column("id"), Literal.ofInt(50))))));
  }

  @Test
  public void testPushFilters_ORSupportedAndUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    // OR(supported, unsupported) cannot be partially pushed: if one branch is unsupported,
    // the whole OR must remain for post-scan evaluation and nothing is pushed to the kernel.
    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new Or(new EqualTo("id", 100), new StringEndsWith("name", "foo"))},
        // expected post-scan filters (whole OR stays, since one branch is unsupported)
        new Filter[] {new Or(new EqualTo("id", 100), new StringEndsWith("name", "foo"))},
        // expected pushed filters (nothing pushed)
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {new Or(new EqualTo("id", 100), new StringEndsWith("name", "foo"))},
        // expected kernelScanBuilder.predicate
        Optional.empty());
  }

  @Test
  public void testPushFilters_ORSupportedDataAndPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1)),
        },
        // expected post-scan filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1))},
        // expected pushed filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1))},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "OR",
              Arrays.asList(
                  new Predicate("=", new Column("id"), Literal.ofInt(100)),
                  new Predicate(">", new Column("dep_id"), Literal.ofInt(1))))
        },
        // expected data filters
        new Filter[] {new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1))},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "OR",
                Arrays.asList(
                    new Predicate("=", new Column("id"), Literal.ofInt(100)),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(1))))));
  }

  /*
   * (partitionFilterA AND partitionFilterB) OR partitionFilterC
   * where A = EqualTo("dep_id", 1), B = StringStartsWith("dep_id", "1"), C = GreaterThan("dep_id", 2)
   * All three are fully supported partition filters, so the whole expression is pushed down.
   *
   * Expected post-scan filters: none (all partition filters, fully pushed)
   * Expected pushed filters: (A AND B) OR C
   * Expected pushed kernel predicates: (predicateA AND predicateB) OR predicateC
   * Expected data filters: none
   * Expected kernelScanBuilder.predicate: (predicateA AND predicateB) OR predicateC
   */
  @Test
  public void testPushFilters_mixedORandAND(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new Or(
              new And(new EqualTo("dep_id", 1), new StringStartsWith("dep_id", "1")),
              new GreaterThan("dep_id", 2))
        },
        // expected post-scan filters
        new Filter[] {},
        // expected pushed filters
        new Filter[] {
          new Or(
              new And(new EqualTo("dep_id", 1), new StringStartsWith("dep_id", "1")),
              new GreaterThan("dep_id", 2))
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "OR",
              Arrays.asList(
                  new Predicate(
                      "AND",
                      Arrays.asList(
                          new Predicate("=", new Column("dep_id"), Literal.ofInt(1)),
                          new Predicate(
                              "STARTS_WITH", new Column("dep_id"), Literal.ofString("1")))),
                  new Predicate(">", new Column("dep_id"), Literal.ofInt(2))))
        },
        // expected data filters
        new Filter[] {},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "OR",
                Arrays.asList(
                    new Predicate(
                        "AND",
                        Arrays.asList(
                            new Predicate("=", new Column("dep_id"), Literal.ofInt(1)),
                            new Predicate(
                                "STARTS_WITH", new Column("dep_id"), Literal.ofString("1")))),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(2))))));
  }

  @Test
  public void testPushFilters_NOTFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new Not(new EqualTo("id", 100))},
        // expected post-scan filters
        new Filter[] {new Not(new EqualTo("id", 100))},
        // expected pushed filters
        new Filter[] {new Not(new EqualTo("id", 100))},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("NOT", new Predicate("=", new Column("id"), Literal.ofInt(100)))
        },
        // expected data filters
        new Filter[] {new Not(new EqualTo("id", 100))},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate("NOT", new Predicate("=", new Column("id"), Literal.ofInt(100)))));
  }

  @Test
  public void testPushFilters_NOTSupportedDataANDSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new Not(new And(new EqualTo("id", 100), new GreaterThan("dep_id", 1))),
        },
        // expected post-scan filters
        new Filter[] {
          new Not(new And(new EqualTo("id", 100), new GreaterThan("dep_id", 1))),
        },
        // expected pushed filters
        new Filter[] {
          new Not(new And(new EqualTo("id", 100), new GreaterThan("dep_id", 1))),
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "NOT",
              new Predicate(
                  "AND",
                  Arrays.asList(
                      new Predicate("=", new Column("id"), Literal.ofInt(100)),
                      new Predicate(">", new Column("dep_id"), Literal.ofInt(1)))))
        },
        // expected data filters
        new Filter[] {
          new Not(new And(new EqualTo("id", 100), new GreaterThan("dep_id", 1))),
        },
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "NOT",
                new Predicate(
                    "AND",
                    Arrays.asList(
                        new Predicate("=", new Column("id"), Literal.ofInt(100)),
                        new Predicate(">", new Column("dep_id"), Literal.ofInt(1)))))));
  }

  @Test
  public void testPushFilters_NOTSupportedDataANDUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new Not(new And(new EqualTo("id", 100), new StringEndsWith("name", "bar")))},
        // expected post-scan filters
        new Filter[] {new Not(new And(new EqualTo("id", 100), new StringEndsWith("name", "bar")))},
        // expected pushed filters
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {new Not(new And(new EqualTo("id", 100), new StringEndsWith("name", "bar")))},
        // expected kernelScanBuilder.predicate
        Optional.empty());
  }

  @Test
  public void testPushFilters_NOTSupportedDataORSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new Not(new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1))),
        },
        // expected post-scan filters
        new Filter[] {new Not(new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1)))},
        // expected pushed filters
        new Filter[] {new Not(new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1)))},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(
              "NOT",
              new Predicate(
                  "OR",
                  Arrays.asList(
                      new Predicate("=", new Column("id"), Literal.ofInt(100)),
                      new Predicate(">", new Column("dep_id"), Literal.ofInt(1)))))
        },
        // expected data filters
        new Filter[] {new Not(new Or(new EqualTo("id", 100), new GreaterThan("dep_id", 1)))},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "NOT",
                new Predicate(
                    "OR",
                    Arrays.asList(
                        new Predicate("=", new Column("id"), Literal.ofInt(100)),
                        new Predicate(">", new Column("dep_id"), Literal.ofInt(1)))))));
  }

  @Test
  public void testPushFilters_NOTSupportedDataORUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    // NOT(OR(supported, unsupported)): the OR branch contains an unsupported filter
    // (StringEndsWith),
    // so the whole NOT(OR(...)) cannot be pushed to the kernel.
    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new Not(new Or(new EqualTo("id", 100), new StringEndsWith("name", "foo"))),
        },
        // expected post-scan filters (whole NOT(OR) stays, unsupported branch blocks pushdown)
        new Filter[] {new Not(new Or(new EqualTo("id", 100), new StringEndsWith("name", "foo")))},
        // expected pushed filters (nothing pushed)
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {new Not(new Or(new EqualTo("id", 100), new StringEndsWith("name", "foo")))},
        // expected kernelScanBuilder.predicate
        Optional.empty());
  }

  private void checkSupportsPushDownFilters(
      SparkScanBuilder builder,
      Filter[] inputFilters,
      Filter[] expectedPostScanFilters,
      Filter[] expectedPushedFilters,
      Predicate[] expectedPushedKernelPredicates,
      Filter[] expectedDataFilters,
      Optional<Predicate> expectedKernelScanBuilderPredicate)
      throws Exception {
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

  private SparkScanBuilder createTestScanBuilder(File tempDir) {
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    return createScanBuilder(tempDir, dataSchema, partitionSchema, tableSchema);
  }

  /**
   * Integration test: decimal widening end-to-end through pushFilters() → classifyFilter() →
   * convertComparisonLiteral(). Verifies that a decimal literal Decimal(5,2) is widened to match
   * column type Decimal(7,2) when pushed through the full filter pushdown path.
   */
  @Test
  public void testPushFilters_decimalWideningEndToEnd(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createDecimalTestScanBuilder(tempDir);

    // price = 100.00 where literal is Decimal(5,2) and column is Decimal(7,2)
    Filter[] sparkFilter = new Filter[] {new EqualTo("price", new BigDecimal("100.00"))};
    Predicate kernelPredicate =
        new Predicate("=", new Column("price"), Literal.ofDecimal(new BigDecimal("100.00"), 7, 2));

    checkSupportsPushDownFilters(
        builder,
        sparkFilter, // input filters
        sparkFilter, // expected post-scan filters (data filter, stays for row-level eval)
        sparkFilter, // expected pushed filters
        new Predicate[] {kernelPredicate}, // expected pushed kernel predicates (widened)
        sparkFilter, // expected data filters
        Optional.of(kernelPredicate)); // expected kernelScanBuilder.predicate
  }

  /**
   * Integration test: decimal literal with scale exceeding column scale is rejected during
   * pushFilters(). The filter AND(price > 99.999, price < 200.00) should partially push only the
   * right side because 99.999 has scale=3 exceeding column's Decimal(7,2) scale=2.
   */
  @Test
  public void testPushFilters_decimalRejectionPartialPushDown(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createDecimalTestScanBuilder(tempDir);

    // AND(price > 99.999, price < 200.00): left side has scale=3 exceeding column's scale=2
    Filter[] sparkFilter =
        new Filter[] {
          new And(
              new GreaterThan("price", new BigDecimal("99.999")),
              new LessThan("price", new BigDecimal("200.00")))
        };
    // Only the right side (price < 200.00) is pushed as a kernel predicate
    Predicate kernelPredicate =
        new Predicate("<", new Column("price"), Literal.ofDecimal(new BigDecimal("200.00"), 7, 2));

    checkSupportsPushDownFilters(
        builder,
        sparkFilter, // input filters
        sparkFilter, // expected post-scan filters (partial conversion, stays for row-level eval)
        new Filter[] {}, // expected pushed filters (partial: Spark filter not added)
        new Predicate[] {kernelPredicate}, // expected pushed kernel predicates
        sparkFilter, // expected data filters
        Optional.of(kernelPredicate)); // expected kernelScanBuilder.predicate
  }

  private SparkScanBuilder createDecimalTestScanBuilder(File tempDir) {
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("price", DataTypes.createDecimalType(7, 2), true),
              DataTypes.createStructField("quantity", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    StructType tableSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("price", DataTypes.createDecimalType(7, 2), true),
              DataTypes.createStructField("quantity", DataTypes.IntegerType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    return createScanBuilder(tempDir, dataSchema, partitionSchema, tableSchema);
  }

  /**
   * Shared helper for creating a SparkScanBuilder with the given schemas. Both
   * createTestScanBuilder and createDecimalTestScanBuilder delegate to this method to avoid
   * duplicating snapshot loading and builder instantiation logic.
   */
  private SparkScanBuilder createScanBuilder(
      File tempDir, StructType dataSchema, StructType partitionSchema, StructType tableSchema) {
    String path = tempDir.getAbsolutePath();
    String tableName = String.format("test_%d", System.currentTimeMillis());
    // Build CREATE TABLE SQL from the tableSchema and partitionSchema
    StringBuilder columns = new StringBuilder();
    Set<String> partitionCols = new HashSet<>();
    for (StructField f : partitionSchema.fields()) {
      partitionCols.add(f.name());
    }
    for (StructField f : tableSchema.fields()) {
      if (columns.length() > 0) columns.append(", ");
      columns.append(f.name()).append(" ").append(f.dataType().sql());
    }
    String partitionColNames = String.join(", ", partitionCols);
    spark.sql(
        String.format(
            "CREATE OR REPLACE TABLE %s (%s) USING delta PARTITIONED BY (%s) LOCATION '%s'",
            tableName, columns, partitionColNames, path));
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(path, spark.sessionState().newHadoopConf());
    Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    return new SparkScanBuilder(
        tableName,
        snapshot,
        snapshotManager,
        dataSchema,
        partitionSchema,
        tableSchema,
        Optional.empty(),
        CaseInsensitiveStringMap.empty());
  }

  private Predicate[] getPushedKernelPredicates(SparkScanBuilder builder) throws Exception {
    // TODO: replace reflection with other testing manners, possibly Mockito ArgumentCaptor
    Field field = SparkScanBuilder.class.getDeclaredField("pushedKernelPredicates");
    field.setAccessible(true);
    return (Predicate[]) field.get(builder);
  }

  private Filter[] getDataFilters(SparkScanBuilder builder) throws Exception {
    // TODO: replace reflection with other testing manners, possibly Mockito ArgumentCaptor
    Field field = SparkScanBuilder.class.getDeclaredField("dataFilters");
    field.setAccessible(true);
    return (Filter[]) field.get(builder);
  }

  private Optional<Predicate> getKernelScanBuilderPredicate(SparkScanBuilder builder)
      throws Exception {
    // TODO: replace reflection with other testing manners, possibly Mockito ArgumentCaptor
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
}
