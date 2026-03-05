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

import static io.delta.spark.internal.v2.utils.ScalaUtils.toJavaOptional;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat;
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat$;
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.colstats.ColumnStatistics;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/** Utilities for converting catalog statistics to V2 connector statistics. */
public final class StatsUtils {

  private StatsUtils() {}

  /**
   * Convert {@link CatalogStatistics} to V2 connector {@link Statistics}.
   *
   * @param catalogStats the catalog statistics to convert
   * @param dataSchema the data schema (non-partition columns)
   * @param partitionSchema the partition schema
   * @return V2 Statistics representation
   */
  public static Statistics toV2Statistics(
      CatalogStatistics catalogStats, StructType dataSchema, StructType partitionSchema) {
    // Build a map of column name -> DataType from both schemas
    Map<String, DataType> columnTypes = new HashMap<>();
    for (StructField field : dataSchema.fields()) {
      columnTypes.put(field.name(), field.dataType());
    }
    for (StructField field : partitionSchema.fields()) {
      columnTypes.put(field.name(), field.dataType());
    }

    Map<NamedReference, ColumnStatistics> colStatsMap = buildColumnStats(catalogStats, columnTypes);

    return new Statistics() {
      @Override
      public OptionalLong sizeInBytes() {
        return OptionalLong.of(catalogStats.sizeInBytes().longValue());
      }

      @Override
      public OptionalLong numRows() {
        return toJavaOptional(catalogStats.rowCount())
            .map(r -> OptionalLong.of(r.longValue()))
            .orElse(OptionalLong.empty());
      }

      @Override
      public Map<NamedReference, ColumnStatistics> columnStats() {
        return colStatsMap;
      }
    };
  }

  private static Map<NamedReference, ColumnStatistics> buildColumnStats(
      CatalogStatistics catalogStats, Map<String, DataType> columnTypes) {
    Map<String, CatalogColumnStat> colStats =
        scala.collection.JavaConverters.mapAsJavaMapConverter(catalogStats.colStats()).asJava();

    if (colStats.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<NamedReference, ColumnStatistics> result = new HashMap<>();
    for (Map.Entry<String, CatalogColumnStat> entry : colStats.entrySet()) {
      String colName = entry.getKey();
      CatalogColumnStat stat = entry.getValue();
      DataType dataType = columnTypes.get(colName);

      if (dataType == null) {
        continue;
      }

      NamedReference ref = FieldReference.apply(colName);
      int version = stat.version();

      // Eagerly parse min/max to avoid repeated fromExternalString calls
      Optional<Object> minValue =
          toJavaOptional(stat.min())
              .map(
                  minStr ->
                      CatalogColumnStat$.MODULE$.fromExternalString(
                          minStr, colName, dataType, version));
      Optional<Object> maxValue =
          toJavaOptional(stat.max())
              .map(
                  maxStr ->
                      CatalogColumnStat$.MODULE$.fromExternalString(
                          maxStr, colName, dataType, version));
      OptionalLong distinctCount =
          toJavaOptional(stat.distinctCount())
              .map(d -> OptionalLong.of(d.longValue()))
              .orElse(OptionalLong.empty());
      OptionalLong nullCount =
          toJavaOptional(stat.nullCount())
              .map(n -> OptionalLong.of(n.longValue()))
              .orElse(OptionalLong.empty());
      OptionalLong avgLen =
          toJavaOptional(stat.avgLen())
              .map(v -> OptionalLong.of((Long) v))
              .orElse(OptionalLong.empty());
      OptionalLong maxLen =
          toJavaOptional(stat.maxLen())
              .map(v -> OptionalLong.of((Long) v))
              .orElse(OptionalLong.empty());

      ColumnStatistics v2ColStats =
          new ColumnStatistics() {
            @Override
            public OptionalLong distinctCount() {
              return distinctCount;
            }

            @Override
            public Optional<Object> min() {
              return minValue;
            }

            @Override
            public Optional<Object> max() {
              return maxValue;
            }

            @Override
            public OptionalLong nullCount() {
              return nullCount;
            }

            @Override
            public OptionalLong avgLen() {
              return avgLen;
            }

            @Override
            public OptionalLong maxLen() {
              return maxLen;
            }
          };
      result.put(ref, v2ColStats);
    }
    return Collections.unmodifiableMap(result);
  }
}
