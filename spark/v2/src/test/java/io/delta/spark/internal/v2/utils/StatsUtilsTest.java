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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat;
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;

class StatsUtilsTest {

  @Test
  void testToV2Statistics_sizeAndRowCount() {
    StructType dataSchema =
        new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
    StructType partitionSchema = new StructType();

    CatalogStatistics catalogStats =
        new CatalogStatistics(
            BigInt(1024L),
            Option.apply(BigInt(100L)),
            scala.collection.immutable.Map$.MODULE$.empty());

    Statistics v2Stats = StatsUtils.toV2Statistics(catalogStats, dataSchema, partitionSchema);

    assertEquals(1024L, v2Stats.sizeInBytes().getAsLong(), "sizeInBytes should match");
    assertTrue(v2Stats.numRows().isPresent(), "numRows should be present");
    assertEquals(100L, v2Stats.numRows().getAsLong(), "numRows should match");
    assertTrue(v2Stats.columnStats().isEmpty(), "columnStats should be empty");
  }

  @Test
  void testToV2Statistics_sizeOnlyNoRowCount() {
    StructType dataSchema = new StructType().add("id", DataTypes.IntegerType);
    StructType partitionSchema = new StructType();

    CatalogStatistics catalogStats =
        new CatalogStatistics(
            BigInt(512L), Option.empty(), scala.collection.immutable.Map$.MODULE$.empty());

    Statistics v2Stats = StatsUtils.toV2Statistics(catalogStats, dataSchema, partitionSchema);

    assertEquals(512L, v2Stats.sizeInBytes().getAsLong(), "sizeInBytes should match");
    assertFalse(v2Stats.numRows().isPresent(), "numRows should be empty");
  }

  @Test
  void testToV2Statistics_withColumnStats() {
    StructType dataSchema =
        new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
    StructType partitionSchema = new StructType().add("part", DataTypes.IntegerType);

    // Create column stats for "id" column
    CatalogColumnStat idColStat =
        new CatalogColumnStat(
            Option.apply(BigInt(10L)), // distinctCount
            Option.apply("1"), // min
            Option.apply("100"), // max
            Option.apply(BigInt(0L)), // nullCount
            Option.apply((Object) 4L), // avgLen
            Option.apply((Object) 4L), // maxLen
            Option.empty(), // histogram
            CatalogColumnStat.VERSION());

    scala.collection.immutable.Map<String, CatalogColumnStat> colStatsMap =
        buildScalaMap(new String[] {"id"}, new CatalogColumnStat[] {idColStat});

    CatalogStatistics catalogStats =
        new CatalogStatistics(BigInt(2048L), Option.apply(BigInt(50L)), colStatsMap);

    Statistics v2Stats = StatsUtils.toV2Statistics(catalogStats, dataSchema, partitionSchema);

    assertEquals(2048L, v2Stats.sizeInBytes().getAsLong());
    assertEquals(50L, v2Stats.numRows().getAsLong());

    Map<NamedReference, ColumnStatistics> colStats = v2Stats.columnStats();
    assertEquals(1, colStats.size(), "Should have 1 column stat");

    ColumnStatistics idStats = colStats.get(FieldReference.apply("id"));
    assertNotNull(idStats, "id column stats should be present");
    assertEquals(10L, idStats.distinctCount().getAsLong(), "distinctCount should be 10");
    assertEquals(0L, idStats.nullCount().getAsLong(), "nullCount should be 0");
    assertEquals(4L, idStats.avgLen().getAsLong(), "avgLen should be 4");
    assertEquals(4L, idStats.maxLen().getAsLong(), "maxLen should be 4");
    assertTrue(idStats.min().isPresent(), "min should be present");
    assertTrue(idStats.max().isPresent(), "max should be present");
    assertEquals(1, idStats.min().get(), "min should be 1");
    assertEquals(100, idStats.max().get(), "max should be 100");
  }

  @Test
  void testToV2Statistics_skipsColumnsNotInSchema() {
    // Only "id" is in schema, "unknown" should be skipped
    StructType dataSchema = new StructType().add("id", DataTypes.IntegerType);
    StructType partitionSchema = new StructType();

    CatalogColumnStat colStat =
        new CatalogColumnStat(
            Option.apply(BigInt(5L)),
            Option.empty(),
            Option.empty(),
            Option.apply(BigInt(1L)),
            Option.empty(),
            Option.empty(),
            Option.empty(),
            CatalogColumnStat.VERSION());

    scala.collection.immutable.Map<String, CatalogColumnStat> colStatsMap =
        buildScalaMap(new String[] {"id", "unknown"}, new CatalogColumnStat[] {colStat, colStat});

    CatalogStatistics catalogStats =
        new CatalogStatistics(BigInt(100L), Option.empty(), colStatsMap);

    Statistics v2Stats = StatsUtils.toV2Statistics(catalogStats, dataSchema, partitionSchema);

    Map<NamedReference, ColumnStatistics> result = v2Stats.columnStats();
    assertEquals(1, result.size(), "Should only have 1 column stat (unknown skipped)");
    assertNotNull(result.get(FieldReference.apply("id")), "id stats should be present");
  }

  @Test
  void testToV2Statistics_columnNameWithSpace() {
    // Regression test: under Delta column mapping a column name may contain a space (e.g.
    // "Extract Year"). buildColumnStats used to call FieldReference.apply(colName),
    // which re-parses the name as a multi-part SQL identifier and throws a ParseException on the
    // space -- failing the entire v2 scan even when the query never references that column.
    String colName = "Extract Year";
    StructType dataSchema = new StructType().add(colName, DataTypes.LongType);
    StructType partitionSchema = new StructType();

    CatalogColumnStat colStat =
        new CatalogColumnStat(
            Option.apply(BigInt(3L)), // distinctCount
            Option.apply("2020"), // min
            Option.apply("2022"), // max
            Option.apply(BigInt(0L)), // nullCount
            Option.apply((Object) 8L), // avgLen
            Option.apply((Object) 8L), // maxLen
            Option.empty(), // histogram
            CatalogColumnStat.VERSION());

    scala.collection.immutable.Map<String, CatalogColumnStat> colStatsMap =
        buildScalaMap(new String[] {colName}, new CatalogColumnStat[] {colStat});

    CatalogStatistics catalogStats =
        new CatalogStatistics(BigInt(256L), Option.apply(BigInt(3L)), colStatsMap);

    // Must not throw: before the fix this raised a ParseException on the space in "Extract Year".
    Statistics v2Stats = StatsUtils.toV2Statistics(catalogStats, dataSchema, partitionSchema);

    Map<NamedReference, ColumnStatistics> result = v2Stats.columnStats();
    assertEquals(1, result.size(), "Should have 1 column stat");

    // The reference must be a single literal part, not split into ["Extract", "Year"].
    NamedReference ref = FieldReference.column(colName);
    assertEquals(1, ref.fieldNames().length, "column name must stay a single identifier part");
    assertEquals(colName, ref.fieldNames()[0]);

    ColumnStatistics stats = result.get(ref);
    assertNotNull(stats, "stats for the space-containing column should be present");
    assertEquals(3L, stats.distinctCount().getAsLong(), "distinctCount should be 3");
  }

  private static scala.math.BigInt BigInt(long value) {
    return scala.math.BigInt.apply(value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static scala.collection.immutable.Map<String, CatalogColumnStat> buildScalaMap(
      String[] keys, CatalogColumnStat[] values) {
    scala.collection.mutable.Builder<
            scala.Tuple2<String, CatalogColumnStat>,
            scala.collection.immutable.Map<String, CatalogColumnStat>>
        b = (scala.collection.mutable.Builder) scala.collection.immutable.Map$.MODULE$.newBuilder();
    for (int i = 0; i < keys.length; i++) {
      b.$plus$eq(new scala.Tuple2<>(keys[i], values[i]));
    }
    return b.result();
  }
}
