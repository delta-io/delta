/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.sink;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public interface FlinkTypeTests {

  LogicalType[] ALL_FLINK_PRIMITIVE_TYPES =
      new LogicalType[] {
        // Boolean
        new BooleanType(),

        // Integers
        new TinyIntType(),
        new SmallIntType(),
        new IntType(),
        new BigIntType(),

        // Floating point
        new FloatType(),
        new DoubleType(),

        // Exact numeric
        new DecimalType(10, 0),
        new DecimalType(38, 18),

        // Character / binary
        new CharType(1),
        new VarCharType(VarCharType.MAX_LENGTH),
        new BinaryType(1),
        new VarBinaryType(VarBinaryType.MAX_LENGTH),

        // Temporal
        new DateType(),
        new TimeType(0),
        new TimeType(3),
        new TimestampType(3),
        new LocalZonedTimestampType(3),

        // Interval
        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR),
        new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND)
      };

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface TestAllFlinkTypes {}

  @TestFactory
  default Stream<DynamicTest> runTestsWithAllFlinkTypes() {
    return Arrays.stream(getClass().getDeclaredMethods())
        .filter(m -> m.getAnnotation(TestAllFlinkTypes.class) != null)
        .flatMap(
            m ->
                Arrays.stream(ALL_FLINK_PRIMITIVE_TYPES)
                    .map(
                        type ->
                            DynamicTest.dynamicTest(
                                String.format("%s:%s", m.getName(), type.toString()),
                                () -> m.invoke(this, type))));
  }
}
