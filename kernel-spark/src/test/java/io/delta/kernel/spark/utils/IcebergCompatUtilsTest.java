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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StructType;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergCompatUtilsTest {

  private Metadata createMetadata(Map<String, String> configuration) {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    ArrayValue emptyPartitionColumns =
        new ArrayValue() {
          @Override
          public int getSize() {
            return 0;
          }

          @Override
          public ColumnVector getElements() {
            return null;
          }
        };
    return new Metadata(
        "id",
        Optional.empty() /* name */,
        Optional.empty() /* description */,
        new Format(),
        schema.toJson(),
        schema,
        emptyPartitionColumns,
        Optional.empty() /* createdTime */,
        io.delta.kernel.internal.util.VectorUtils.stringStringMapValue(configuration));
  }

  private static Stream<Arguments> isAnyEnabledTestProvider() {
    return Stream.of(
        // configKey, expectedResult
        Arguments.of(null, false),
        Arguments.of("delta.enableIcebergCompatV1", true),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey(), true),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey(), false));
  }

  @ParameterizedTest
  @MethodSource("isAnyEnabledTestProvider")
  public void testIsAnyEnabled(String configKey, boolean expectedResult) {
    Map<String, String> config = new HashMap<>();
    if (configKey != null) {
      config.put(configKey, "true");
    }
    Metadata metadata = createMetadata(config);
    assertEquals(expectedResult, IcebergCompatUtils.isAnyEnabled(metadata));
  }

  private static Stream<Arguments> isVersionGeqEnabledTestProvider() {
    return Stream.of(
        // configKey, requiredVersion, expectedResult
        Arguments.of(null, 1, false),
        Arguments.of(null, 2, false),
        Arguments.of("delta.enableIcebergCompatV1", 1, true),
        Arguments.of("delta.enableIcebergCompatV1", 2, false),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey(), 1, true),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey(), 2, true),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey(), 3, false),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey(), 1, false),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey(), 2, false),
        Arguments.of(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey(), 3, false));
  }

  @ParameterizedTest
  @MethodSource("isVersionGeqEnabledTestProvider")
  public void testIsVersionGeqEnabled(
      String configKey, int enabledVersion, int requiredVersion, boolean expectedResult) {
    Map<String, String> config = new HashMap<>();
    if (configKey != null) {
      config.put(configKey, "true");
    }
    Metadata metadata = createMetadata(config);
    assertEquals(expectedResult, IcebergCompatUtils.isVersionGeqEnabled(metadata, requiredVersion));
  }
}
