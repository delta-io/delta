/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.util;

import org.apache.parquet.schema.MessageType;

import io.delta.standalone.types.StructType;
import io.delta.standalone.internal.util.SparkToParquetSchemaConverter;

/**
 * Converter class to convert {@link StructType} to Parquet {@link MessageType}.
 */
public class ParquetSchemaConverter {

    /**
     * Represents Parquet timestamp types.
     * - INT96 is a non-standard but commonly used timestamp type in Parquet.
     * - TIMESTAMP_MICROS is a standard timestamp type in Parquet, which stores number of
     *   microseconds from the Unix epoch.
     * - TIMESTAMP_MILLIS is also standard, but with millisecond precision, which means the
     *   microsecond portion of the timestamp value is truncated.
     */
    public enum ParquetOutputTimestampType {
        INT96,
        TIMESTAMP_MICROS,
        TIMESTAMP_MILLIS
    }

    private static final Boolean writeLegacyParquetFormatDefault = false;
    private static final ParquetOutputTimestampType outputTimestampTypeDefault =
            ParquetOutputTimestampType.INT96;

    /**
     * Convert a {@link StructType} to Parquet {@link MessageType}.
     *
     * @param schema  the schema to convert
     * @return {@code schema} as a Parquet {@link MessageType}
     * @throws IllegalArgumentException if a {@code StructField} name contains invalid character(s)
     */
    public static MessageType sparkToParquet(StructType schema) {
        return new SparkToParquetSchemaConverter(
                writeLegacyParquetFormatDefault,
                outputTimestampTypeDefault).convert(schema);
    }

    /**
     * Convert a {@link StructType} to Parquet {@link MessageType}.
     *
     * @param schema  the schema to convert
     * @param writeLegacyParquetFormat  Whether to use legacy Parquet format compatible with Spark
     *        1.4 and prior versions when converting a {@link StructType} to a Parquet
     *        {@link MessageType}. When set to false, use standard format defined in parquet-format
     *        spec.
     * @return {@code schema} as a Parquet {@link MessageType}
     * @throws IllegalArgumentException if a {@code StructField} name contains invalid character(s)
     */
    public static MessageType sparkToParquet(StructType schema, Boolean writeLegacyParquetFormat) {
        return new SparkToParquetSchemaConverter(
                writeLegacyParquetFormat,
                outputTimestampTypeDefault).convert(schema);
    }

    /**
     * Convert a {@link StructType} to Parquet {@link MessageType}.
     *
     * @param schema  the schema to convert
     * @param outputTimestampType  which parquet timestamp type to use when writing
     * @return {@code schema} as a Parquet {@link MessageType}
     * @throws IllegalArgumentException if a {@code StructField} name contains invalid character(s)
     */
    public static MessageType sparkToParquet(
            StructType schema,
            ParquetOutputTimestampType outputTimestampType) {
        return new SparkToParquetSchemaConverter(
                writeLegacyParquetFormatDefault,
                outputTimestampType).convert(schema);
    }

    /**
     * Convert a {@link StructType} to Parquet {@link MessageType}.
     *
     * @param schema  the schema to convert
     * @param writeLegacyParquetFormat  Whether to use legacy Parquet format compatible with Spark
     *        1.4 and prior versions when converting a {@link StructType} to a Parquet
     *        {@link MessageType}. When set to false, use standard format defined in parquet-format
     *        spec.
     * @param outputTimestampType  which parquet timestamp type to use when writing
     * @return {@code schema} as a Parquet {@link MessageType}
     * @throws IllegalArgumentException if a {@code StructField} name contains invalid character(s)
     */
    public static MessageType sparkToParquet(
            StructType schema,
            Boolean writeLegacyParquetFormat,
            ParquetOutputTimestampType outputTimestampType) {
        return new SparkToParquetSchemaConverter(
                writeLegacyParquetFormat,
                outputTimestampType).convert(schema);
    }
}
