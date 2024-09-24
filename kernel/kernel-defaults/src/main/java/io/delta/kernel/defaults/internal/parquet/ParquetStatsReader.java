/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet;

import static io.delta.kernel.defaults.internal.DefaultKernelUtils.getDataType;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.UnaryOperator.identity;
import static org.apache.hadoop.shaded.com.google.common.collect.ImmutableMap.toImmutableMap;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatistics;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.com.google.common.collect.ImmutableMultimap;
import org.apache.hadoop.shaded.com.google.common.collect.Multimap;
import org.apache.parquet.column.statistics.*;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

/** Helper class to read statistics from Parquet files. */
public class ParquetStatsReader {
  /**
   * Read the statistics for the given Parquet file.
   *
   * @param parquetFilePath The path to the Parquet file.
   * @param hadoopConf The Hadoop configuration to use for reading the file.
   * @param dataSchema The schema of the Parquet file. Type info is used to decode statistics.
   * @param statsColumns The columns for which statistics should be collected and returned.
   * @return File/column level statistics as {@link DataFileStatistics} instance.
   */
  public static DataFileStatistics readDataFileStatistics(
      Path parquetFilePath,
      Configuration hadoopConf,
      StructType dataSchema,
      List<Column> statsColumns)
      throws IOException {
    // Read the Parquet footer to compute the statistics
    ParquetMetadata footer = ParquetFileReader.readFooter(hadoopConf, parquetFilePath);
    ImmutableMultimap.Builder<Column, ColumnChunkMetaData> metadataForColumn =
        ImmutableMultimap.builder();

    long rowCount = 0;
    for (BlockMetaData blockMetaData : footer.getBlocks()) {
      rowCount += blockMetaData.getRowCount();
      for (ColumnChunkMetaData columnChunkMetaData : blockMetaData.getColumns()) {
        Column column = new Column(columnChunkMetaData.getPath().toArray());
        metadataForColumn.put(column, columnChunkMetaData);
      }
    }

    return constructFileStats(metadataForColumn.build(), dataSchema, statsColumns, rowCount);
  }

  /**
   * Merge statistics from multiple rowgroups into a single set of statistics for each column.
   *
   * @return Stats for each column in the file as {@link DataFileStatistics}.
   */
  private static DataFileStatistics constructFileStats(
      Multimap<Column, ColumnChunkMetaData> metadataForColumn,
      StructType dataSchema,
      List<Column> statsColumns,
      long rowCount) {
    Map<Column, Optional<Statistics<?>>> statsForColumn =
        metadataForColumn.keySet().stream()
            .collect(
                toImmutableMap(identity(), key -> mergeMetadataList(metadataForColumn.get(key))));

    Map<Column, Literal> minValues = new HashMap<>();
    Map<Column, Literal> maxValues = new HashMap<>();
    Map<Column, Long> nullCounts = new HashMap<>();
    for (Column statsColumn : statsColumns) {
      Optional<Statistics<?>> stats = statsForColumn.get(statsColumn);
      DataType columnType = getDataType(dataSchema, statsColumn);
      if (stats == null || !stats.isPresent() || !isStatsSupportedDataType(columnType)) {
        continue;
      }
      Statistics<?> statistics = stats.get();

      Long numNulls = statistics.isNumNullsSet() ? statistics.getNumNulls() : null;
      nullCounts.put(statsColumn, numNulls);

      if (numNulls != null && rowCount == numNulls) {
        // If all values are null, then min and max are also null
        minValues.put(statsColumn, Literal.ofNull(columnType));
        maxValues.put(statsColumn, Literal.ofNull(columnType));
        continue;
      }

      Literal minValue = decodeMinMaxStat(columnType, statistics, true /* decodeMin */);
      minValues.put(statsColumn, minValue);

      Literal maxValue = decodeMinMaxStat(columnType, statistics, false /* decodeMin */);
      maxValues.put(statsColumn, maxValue);
    }

    return new DataFileStatistics(rowCount, minValues, maxValues, nullCounts);
  }

  private static Literal decodeMinMaxStat(
      DataType dataType, Statistics<?> statistics, boolean decodeMin) {
    Object statValue = decodeMin ? statistics.genericGetMin() : statistics.genericGetMax();
    if (statValue == null) {
      return null;
    }

    if (dataType instanceof BooleanType) {
      return Literal.ofBoolean((Boolean) statValue);
    } else if (dataType instanceof ByteType) {
      return Literal.ofByte(((Number) statValue).byteValue());
    } else if (dataType instanceof ShortType) {
      return Literal.ofShort(((Number) statValue).shortValue());
    } else if (dataType instanceof IntegerType) {
      return Literal.ofInt(((Number) statValue).intValue());
    } else if (dataType instanceof LongType) {
      return Literal.ofLong(((Number) statValue).longValue());
    } else if (dataType instanceof FloatType) {
      return Literal.ofFloat(((Number) statValue).floatValue());
    } else if (dataType instanceof DoubleType) {
      return Literal.ofDouble(((Number) statValue).doubleValue());
    } else if (dataType instanceof DecimalType) {
      LogicalTypeAnnotation logicalType = statistics.type().getLogicalTypeAnnotation();
      checkArgument(
          logicalType instanceof DecimalLogicalTypeAnnotation,
          "Physical decimal column has invalid Parquet Logical Type: %s",
          logicalType);
      int scale = ((DecimalLogicalTypeAnnotation) logicalType).getScale();

      DecimalType decimalType = (DecimalType) dataType;

      // Check the scale is same in both the Delta data type and the Parquet Logical Type
      checkArgument(
          scale == decimalType.getScale(),
          "Physical decimal type has different scale than the logical type: %s",
          scale);

      // Decimal is stored either as int, long or binary. Decode the stats accordingly.
      BigDecimal decimalStatValue;
      if (statistics instanceof IntStatistics) {
        decimalStatValue = BigDecimal.valueOf((Integer) statValue).movePointLeft(scale);
      } else if (statistics instanceof LongStatistics) {
        decimalStatValue = BigDecimal.valueOf((Long) statValue).movePointLeft(scale);
      } else if (statistics instanceof BinaryStatistics) {
        BigInteger base = new BigInteger(getBinaryStat(statistics, decodeMin));
        decimalStatValue = new BigDecimal(base, scale);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported stats type for Decimal: " + statistics.getClass());
      }
      return Literal.ofDecimal(
          decimalStatValue, decimalType.getPrecision(), decimalType.getScale());
    } else if (dataType instanceof DateType) {
      checkArgument(
          statistics instanceof IntStatistics,
          "Column with DATE type contained invalid statistics: %s",
          statistics);
      return Literal.ofDate((Integer) statValue); // stats are stored as epoch days in Parquet
    } else if (dataType instanceof TimestampType) {
      // Kernel Parquet writer always writes timestamps in INT64 format
      checkArgument(
          statistics instanceof LongStatistics,
          "Column with TIMESTAMP type contained invalid statistics: %s",
          statistics);
      return Literal.ofTimestamp((Long) statValue);
    } else if (dataType instanceof TimestampNTZType) {
      checkArgument(
          statistics instanceof LongStatistics,
          "Column with TIMESTAMP_NTZ type contained invalid statistics: %s",
          statistics);
      return Literal.ofTimestampNtz((Long) statValue);
    } else if (dataType instanceof StringType) {
      byte[] binaryStat = getBinaryStat(statistics, decodeMin);
      return Literal.ofString(new String(binaryStat, UTF_8), "UTF8_BINARY");
    } else if (dataType instanceof BinaryType) {
      return Literal.ofBinary(getBinaryStat(statistics, decodeMin));
    }

    throw new IllegalArgumentException("Unsupported stats data type: " + statValue);
  }

  private static Optional<Statistics<?>> mergeMetadataList(
      Collection<ColumnChunkMetaData> metadataList) {
    if (hasInvalidStatistics(metadataList)) {
      return Optional.empty();
    }

    return metadataList.stream()
        .<Statistics<?>>map(ColumnChunkMetaData::getStatistics)
        .reduce(
            (statsA, statsB) -> {
              statsA.mergeStatistics(statsB);
              return statsA;
            });
  }

  private static boolean hasInvalidStatistics(Collection<ColumnChunkMetaData> metadataList) {
    // If any row group does not have stats collected, stats for the file will not be valid
    return metadataList.stream()
        .anyMatch(
            metadata -> {
              Statistics<?> stats = metadata.getStatistics();
              if (stats == null || stats.isEmpty() || !stats.isNumNullsSet()) {
                return true;
              }

              // Columns with NaN values are marked by `hasNonNullValue` = false by the Parquet
              // reader
              // See issue: https://issues.apache.org/jira/browse/PARQUET-1246
              return !stats.hasNonNullValue() && stats.getNumNulls() != metadata.getValueCount();
            });
  }

  private static boolean isStatsSupportedDataType(DataType dataType) {
    return dataType instanceof BooleanType
        || dataType instanceof ByteType
        || dataType instanceof ShortType
        || dataType instanceof IntegerType
        || dataType instanceof LongType
        || dataType instanceof FloatType
        || dataType instanceof DoubleType
        || dataType instanceof DecimalType
        || dataType instanceof DateType
        || dataType instanceof TimestampType
        || dataType instanceof TimestampNTZType
        || dataType instanceof StringType
        || dataType instanceof BinaryType;
  }

  private static byte[] getBinaryStat(Statistics<?> statistics, boolean decodeMin) {
    return decodeMin ? statistics.getMinBytes() : statistics.getMaxBytes();
  }
}
