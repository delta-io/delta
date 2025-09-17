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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.SparkDsv2TestBase;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SparkScanBuilderTest extends SparkDsv2TestBase {

  @Test
  public void testBuild_returnsScanWithExpectedSchema(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "scan_builder_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            path,
            dataSchema,
            partitionSchema,
            (SnapshotImpl) snapshot,
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
  public void testToMicroBatchStream_returnsSparkMicroBatchStream(@TempDir File tempDir) {
    String path = tempDir.getAbsolutePath();
    String tableName = "microbatch_test";
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    SparkScanBuilder builder =
        new SparkScanBuilder(
            tableName,
            path,
            dataSchema,
            partitionSchema,
            (SnapshotImpl) snapshot,
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
        new Filter[] {new StringStartsWith("name", "test")}, // input filters
        new Filter[] {new StringStartsWith("name", "test")}, // expected post-scan filters
        new Filter[] {}, // expected pushed filters
        new Predicate[] {}, // expected pushed kernel predicates
        new Filter[] {new StringStartsWith("name", "test")}, // expected data filters
        Optional.empty() // expected kernelScanBuilder.predicate
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
          new StringStartsWith("name", "test") // unsupported
        },
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100), new StringStartsWith("name", "test")},
        // expected pushed filters
        new Filter[] {new EqualTo("id", 100)},
        // expected pushed kernel predicates
        new Predicate[] {new Predicate("=", new Column("id"), Literal.ofInt(100))},
        // expected data filters
        new Filter[] {new EqualTo("id", 100), new StringStartsWith("name", "test")},
        // expected kernelScanBuilder.predicate
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
        new Filter[] {new StringStartsWith("dep_id", "1")}, // expected
        new Filter[] {}, // expected pushed filters
        new Predicate[] {}, // expected pushed kernel predicates
        new Filter[] {}, // expected data filters
        Optional.empty() // expected kernelScanBuilder.predicate
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
        new Filter[] {new StringStartsWith("dep_id", "1")},
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
          new GreaterThan("dep_id", 1) // partition filter, supported
        },
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("id"), Literal.ofInt(100)),
          new Predicate(">", new Column("dep_id"), Literal.ofInt(1))
        },
        // expected data filters
        new Filter[] {
          new EqualTo("id", 100), // data filter, supported
          new StringStartsWith("name", "foo") // data filter, unsupported
        },
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                Arrays.asList(
                    new Predicate("=", new Column("id"), Literal.ofInt(100)),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(1))))));
  }

  @Test
  public void testPushFilters_ANDSupportedDataFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new EqualTo("id", 100), new GreaterThan("id", 50)), // supported
        },
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
  public void testPushFilters_ANDUnsupportedDataFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new StringStartsWith("name", "foo"), new StringEndsWith("name", "bar"))
        },
        // expected post-scan filters
        new Filter[] {new StringStartsWith("name", "foo"), new StringEndsWith("name", "bar")},
        // expected pushed filters
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {new StringStartsWith("name", "foo"), new StringEndsWith("name", "bar")},
        // expected kernelScanBuilder.predicate
        Optional.empty());
  }

  @Test
  public void testPushFilters_ANDSupportedAndUnsupportedDataFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new And(new EqualTo("id", 100), new StringStartsWith("name", "foo"))},
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100), new StringStartsWith("name", "foo")},
        // expected pushed filters
        new Filter[] {new EqualTo("id", 100)},
        // expected pushed kernel predicates
        new Predicate[] {new Predicate("=", new Column("id"), Literal.ofInt(100))},
        // expected data filters
        new Filter[] {new EqualTo("id", 100), new StringStartsWith("name", "foo")},
        // expected kernelScanBuilder.predicate
        Optional.of(new Predicate("=", new Column("id"), Literal.ofInt(100))));
  }

  @Test
  public void testPushFilters_ANDSupportedPartitionFilters(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new EqualTo("dep_id", 1), new GreaterThan("dep_id", 0)), // supported
        },
        // expected post-scan filters
        new Filter[] {},
        // expected pushed filters
        new Filter[] {new EqualTo("dep_id", 1), new GreaterThan("dep_id", 0)},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("dep_id"), Literal.ofInt(1)),
          new Predicate(">", new Column("dep_id"), Literal.ofInt(0))
        },
        // expected data filters
        new Filter[] {},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                Arrays.asList(
                    new Predicate("=", new Column("dep_id"), Literal.ofInt(1)),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(0))))));
  }

  @Test
  public void testPushFilters_ANDUnsupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new StringStartsWith("dep_id", "1"), new StringEndsWith("dep_id", "0"))
        },
        // expected post-scan filters
        new Filter[] {new StringStartsWith("dep_id", "1"), new StringEndsWith("dep_id", "0")},
        // expected pushed filters
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {},
        // expected kernelScanBuilder.predicate
        Optional.empty());
  }

  @Test
  public void testPushFilters_ANDSupportedAndUnsupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new And(new EqualTo("dep_id", 1), new StringStartsWith("dep_id", "1"))},
        // expected post-scan filters
        new Filter[] {new StringStartsWith("dep_id", "1")},
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
  public void testPushFilters_ANDSupportedDataAndPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new EqualTo("id", 100), new GreaterThan("dep_id", 1)),
        },
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100)},
        // expected pushed filters
        new Filter[] {new EqualTo("id", 100), new GreaterThan("dep_id", 1)},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate("=", new Column("id"), Literal.ofInt(100)),
          new Predicate(">", new Column("dep_id"), Literal.ofInt(1))
        },
        // expected data filters
        new Filter[] {new EqualTo("id", 100)},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                Arrays.asList(
                    new Predicate("=", new Column("id"), Literal.ofInt(100)),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(1))))));
  }

  @Test
  public void testPushFilters_ANDSupportedDataAndUnsupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new EqualTo("id", 100), new StringStartsWith("dep_id", "1")),
        },
        // expected post-scan filters
        new Filter[] {new EqualTo("id", 100), new StringStartsWith("dep_id", "1")},
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
  public void testPushFilters_ANDUnsupportedDataAndSupportedPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new StringStartsWith("name", "foo"), new GreaterThan("dep_id", 1)),
        },
        // expected post-scan filters
        new Filter[] {new StringStartsWith("name", "foo")},
        // expected pushed filters
        new Filter[] {new GreaterThan("dep_id", 1)},
        // expected pushed kernel predicates
        new Predicate[] {new Predicate(">", new Column("dep_id"), Literal.ofInt(1))},
        // expected data filters
        new Filter[] {new StringStartsWith("name", "foo")},
        // expected kernelScanBuilder.predicate
        Optional.of(new Predicate(">", new Column("dep_id"), Literal.ofInt(1))));
  }

  @Test
  public void testPushFilters_ANDUnsupportedDataAndPartitionFilters(@TempDir File tempDir)
      throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(new StringStartsWith("name", "foo"), new StringEndsWith("dep_id", "1")),
        },
        // expected post-scan filters
        new Filter[] {new StringStartsWith("name", "foo"), new StringEndsWith("dep_id", "1")},
        // expected pushed filters
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {new StringStartsWith("name", "foo")},
        // expected kernelScanBuilder.predicate
        Optional.empty());
  }

  @Test
  public void testPushFilters_nestedAnd(@TempDir File tempDir) throws Exception {
    SparkScanBuilder builder = createTestScanBuilder(tempDir);

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {
          new And(
              new And(new GreaterThan("id", 50), new StringEndsWith("dep_id", "1")),
              new And(new StringStartsWith("name", "foo"), new GreaterThan("dep_id", 1)))
        },
        // expected post-scan filters
        new Filter[] {
          new GreaterThan("id", 50),
          new StringEndsWith("dep_id", "1"),
          new StringStartsWith("name", "foo")
        },
        // expected pushed filters
        new Filter[] {new GreaterThan("id", 50), new GreaterThan("dep_id", 1)},
        // expected pushed kernel predicates
        new Predicate[] {
          new Predicate(">", new Column("id"), Literal.ofInt(50)),
          new Predicate(">", new Column("dep_id"), Literal.ofInt(1))
        },
        // expected data filters
        new Filter[] {new GreaterThan("id", 50), new StringStartsWith("name", "foo")},
        // expected kernelScanBuilder.predicate
        Optional.of(
            new Predicate(
                "AND",
                Arrays.asList(
                    new Predicate(">", new Column("id"), Literal.ofInt(50)),
                    new Predicate(">", new Column("dep_id"), Literal.ofInt(1))))));
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

    checkSupportsPushDownFilters(
        builder,
        // input filters
        new Filter[] {new Or(new EqualTo("id", 100), new StringStartsWith("name", "foo"))},
        // expected post-scan filters
        new Filter[] {new Or(new EqualTo("id", 100), new StringStartsWith("name", "foo"))},
        // expected pushed filters
        new Filter[] {},
        // expected pushed kernel predicates
        new Predicate[] {},
        // expected data filters
        new Filter[] {new Or(new EqualTo("id", 100), new StringStartsWith("name", "foo"))},
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
    String path = tempDir.getAbsolutePath();
    String tableName = String.format("predicate_test_%d", System.currentTimeMillis());
    spark.sql(
        String.format(
            "CREATE OR REPLACE TABLE %s (id INT, name STRING, dep_id INT) USING delta PARTITIONED BY (dep_id) LOCATION '%s'",
            tableName, path));
    Snapshot snapshot = TableManager.loadSnapshot(path).build(defaultEngine);
    StructType dataSchema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)
            });
    StructType partitionSchema =
        DataTypes.createStructType(
            new StructField[] {DataTypes.createStructField("dep_id", DataTypes.IntegerType, true)});
    return new SparkScanBuilder(
        tableName,
        path,
        dataSchema,
        partitionSchema,
        (SnapshotImpl) snapshot,
        CaseInsensitiveStringMap.empty());
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
    return (Optional<Predicate>) predicateField.get(kernelScanBuilder);
  }
}
