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

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unity Catalog port of {@code V2PartitionValueBoundaryTest}: round-trips partition values through
 * batch and streaming reads on EXTERNAL and MANAGED UC tables.
 *
 * <p>The source DSv2 path-based test ran each boundary case as a parameterized test exercising
 * dsv1/dsv2 batch and streaming. UC catalog reads use the spark_catalog -> UC route, so this port
 * exercises only catalog-level batch and streaming reads, but doubles the matrix by running each
 * case across both EXTERNAL and MANAGED via {@link UCDeltaTableIntegrationBaseTest.TableType}.
 *
 * <p>Each {@code @TestAllTableTypes} method iterates over all boundary cases and gathers per-case
 * mismatches into a single failure message so MANAGED-only regressions surface as a list.
 */
public class UCDeltaPartitionValueBoundaryTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** A single boundary case: name, partition data type, partition value. */
  static class BoundaryCase {
    final String name;
    final DataType partType;
    final Object partValue;

    BoundaryCase(String name, DataType partType, Object partValue) {
      this.name = name;
      this.partType = partType;
      this.partValue = partValue;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private static List<BoundaryCase> boundaryCases() {
    String veryLong = repeat("x", 4096);
    List<BoundaryCase> cases = new ArrayList<>();
    // STRING partition column.
    cases.add(new BoundaryCase("01_empty_string", DataTypes.StringType, ""));
    cases.add(new BoundaryCase("02_literal_NULL", DataTypes.StringType, "NULL"));
    cases.add(
        new BoundaryCase(
            "03_sentinel_literal", DataTypes.StringType, "__HIVE_DEFAULT_PARTITION__"));
    cases.add(new BoundaryCase("05a_newline", DataTypes.StringType, "\n"));
    cases.add(new BoundaryCase("05b_tab", DataTypes.StringType, "\t"));
    cases.add(new BoundaryCase("05c_cr", DataTypes.StringType, "\r"));
    cases.add(new BoundaryCase("06a_leading_space", DataTypes.StringType, " foo"));
    cases.add(new BoundaryCase("06b_trailing_space", DataTypes.StringType, "foo "));
    cases.add(new BoundaryCase("07_forward_slash", DataTypes.StringType, "a/b"));
    cases.add(new BoundaryCase("08_equals_sign", DataTypes.StringType, "a=b"));
    cases.add(new BoundaryCase("09a_percent", DataTypes.StringType, "%"));
    cases.add(new BoundaryCase("09b_percent_20", DataTypes.StringType, "%20"));
    cases.add(new BoundaryCase("09c_percent_25", DataTypes.StringType, "%25"));
    cases.add(new BoundaryCase("10_emoji", DataTypes.StringType, "😀"));
    cases.add(new BoundaryCase("11_combining_acute", DataTypes.StringType, "é"));
    cases.add(new BoundaryCase("13_very_long_4096", DataTypes.StringType, veryLong));
    cases.add(new BoundaryCase("14_just_whitespace", DataTypes.StringType, " "));
    cases.add(new BoundaryCase("15_backslash", DataTypes.StringType, "a\\b"));
    // NUMERIC partition column.
    cases.add(new BoundaryCase("16a_int_min", DataTypes.IntegerType, Integer.MIN_VALUE));
    cases.add(new BoundaryCase("16b_int_max", DataTypes.IntegerType, Integer.MAX_VALUE));
    cases.add(new BoundaryCase("17_neg_zero_double", DataTypes.DoubleType, -0.0));
    return cases;
  }

  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder(s.length() * n);
    for (int i = 0; i < n; i++) sb.append(s);
    return sb.toString();
  }

  /** Repr that disambiguates null, empty-string, whitespace, control chars, surrogate pairs. */
  static String repr(Object v) {
    if (v == null) return "<NULL>";
    if (v instanceof String) {
      String s = (String) v;
      StringBuilder sb = new StringBuilder("\"");
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (c < 0x20 || c == 0x7F) {
          sb.append("\\u").append(String.format("%04X", (int) c));
        } else if (c == '"' || c == '\\') {
          sb.append('\\').append(c);
        } else if (c >= 0xD800 && c <= 0xDFFF) {
          sb.append("\\u").append(String.format("%04X", (int) c));
        } else {
          sb.append(c);
        }
      }
      sb.append("\" (len=").append(s.length()).append(")");
      return sb.toString();
    }
    return String.valueOf(v);
  }

  static boolean partitionEquals(Object actual, Object expected) {
    if (expected == null) return actual == null;
    if (actual == null) return false;
    if (expected instanceof Double) {
      if (!(actual instanceof Double)) return false;
      return Double.compare((Double) actual, (Double) expected) == 0;
    }
    return expected.equals(actual);
  }

  static String rootMessage(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    String m = cur.getClass().getSimpleName() + ": " + cur.getMessage();
    return m.length() > 300 ? m.substring(0, 300) + "..." : m;
  }

  /** Round-trips each boundary case via UC catalog batch read on the given table type. */
  @TestAllTableTypes
  public void testPartitionValueRoundTrip_batch(TableType tableType) throws Exception {
    List<String> failures = new ArrayList<>();
    for (BoundaryCase bc : boundaryCases()) {
      String tableName = "pvbt_b_" + bc.name.toLowerCase();
      try {
        runOneCaseBatch(tableName, tableType, bc, failures);
      } catch (Throwable t) {
        failures.add(bc.name + " (setup): " + rootMessage(t));
      }
    }
    if (!failures.isEmpty()) {
      fail(
          "MANAGED-aware partition-value round-trip via UC batch reads failed for tableType="
              + tableType
              + ":\n  - "
              + String.join("\n  - ", failures));
    }
  }

  /** Round-trips each boundary case via UC catalog streaming read on the given table type. */
  @TestAllTableTypes
  public void testPartitionValueRoundTrip_stream(TableType tableType) throws Exception {
    List<String> failures = new ArrayList<>();
    for (BoundaryCase bc : boundaryCases()) {
      String tableName = "pvbt_s_" + bc.name.toLowerCase();
      try {
        runOneCaseStream(tableName, tableType, bc, failures);
      } catch (Throwable t) {
        failures.add(bc.name + " (setup): " + rootMessage(t));
      }
    }
    if (!failures.isEmpty()) {
      fail(
          "MANAGED-aware partition-value round-trip via UC streaming reads failed for tableType="
              + tableType
              + ":\n  - "
              + String.join("\n  - ", failures));
    }
  }

  private void runOneCaseBatch(
      String simpleName, TableType tableType, BoundaryCase bc, List<String> failures)
      throws Exception {
    String partTypeSql = sqlTypeForDataType(bc.partType);
    withNewTable(
        simpleName,
        "id INT, p " + partTypeSql,
        "p",
        tableType,
        null,
        tableName -> {
          writeOneRow(tableName, bc);
          try {
            Dataset<Row> df = spark().sql("SELECT id, p FROM " + tableName);
            judgeAndRecord("uc_batch", df.collectAsList(), bc, failures);
          } catch (Throwable t) {
            failures.add(bc.name + " (uc_batch): " + rootMessage(t));
          }
        });
  }

  private void runOneCaseStream(
      String simpleName, TableType tableType, BoundaryCase bc, List<String> failures)
      throws Exception {
    String partTypeSql = sqlTypeForDataType(bc.partType);
    withNewTable(
        simpleName,
        "id INT, p " + partTypeSql,
        "p",
        tableType,
        null,
        tableName -> {
          writeOneRow(tableName, bc);
          String queryName =
              "pvbt_uc_stream_" + bc.name + "_" + UUID.randomUUID().toString().replace('-', '_');
          StreamingQuery query = null;
          try {
            query =
                spark()
                    .readStream()
                    .format("delta")
                    .table(tableName)
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .option("checkpointLocation", checkpoint())
                    .outputMode("append")
                    .start();
            query.processAllAvailable();
            List<Row> actual = spark().sql("SELECT * FROM " + queryName).collectAsList();
            judgeAndRecord("uc_stream", actual, bc, failures);
          } catch (Throwable t) {
            failures.add(bc.name + " (uc_stream): " + rootMessage(t));
          } finally {
            if (query != null) {
              try {
                query.stop();
              } catch (Throwable ignored) {
                // ignore stop errors
              }
            }
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }
        });
  }

  private void writeOneRow(String tableName, BoundaryCase bc) {
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("p", bc.partType, true)));
    List<Row> rows = Collections.singletonList(RowFactory.create(1, bc.partValue));
    spark()
        .createDataFrame(rows, schema)
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(tableName);
  }

  private void judgeAndRecord(
      String engine, List<Row> actualRows, BoundaryCase bc, List<String> failures) {
    if (actualRows.size() != 1) {
      failures.add(
          bc.name
              + " ("
              + engine
              + "): expected 1 row, got "
              + actualRows.size()
              + ": "
              + actualRows);
      return;
    }
    Row r = actualRows.get(0);
    if (r.length() != 2) {
      failures.add(bc.name + " (" + engine + "): expected 2 cols, got " + r.length() + ": " + r);
      return;
    }
    Object actualP = r.get(1);
    if (!partitionEquals(actualP, bc.partValue)) {
      failures.add(
          bc.name
              + " ("
              + engine
              + "): expected p="
              + repr(bc.partValue)
              + ", got p="
              + repr(actualP));
    }
  }

  private static String sqlTypeForDataType(DataType dt) {
    if (dt.equals(DataTypes.StringType)) return "STRING";
    if (dt.equals(DataTypes.IntegerType)) return "INT";
    if (dt.equals(DataTypes.DoubleType)) return "DOUBLE";
    throw new IllegalArgumentException("Unsupported partition type: " + dt);
  }
}
