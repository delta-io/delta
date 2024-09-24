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
package io.delta.kernel.expressions;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * A literal value.
 *
 * <p>Definition:
 *
 * <ul>
 *   <li>Represents literal of primitive types as defined in the protocol <a
 *       href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">Delta
 *       Transaction Log Protocol: Primitive Types</a>
 *   <li>Use {@link #getValue()} to fetch the literal value. Returned value type depends on the type
 *       of the literal data type. See the {@link #getValue()} for further details.
 * </ul>
 *
 * @since 3.0.0
 */
@Evolving
public final class Literal implements Expression {
  /**
   * Create a {@code boolean} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link BooleanType}
   */
  public static Literal ofBoolean(boolean value) {
    return new Literal(value, BooleanType.BOOLEAN);
  }

  /**
   * Create a {@code byte} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link ByteType}
   */
  public static Literal ofByte(byte value) {
    return new Literal(value, ByteType.BYTE);
  }

  /**
   * Create a {@code short} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link ShortType}
   */
  public static Literal ofShort(short value) {
    return new Literal(value, ShortType.SHORT);
  }

  /**
   * Create a {@code integer} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link IntegerType}
   */
  public static Literal ofInt(int value) {
    return new Literal(value, IntegerType.INTEGER);
  }

  /**
   * Create a {@code long} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link LongType}
   */
  public static Literal ofLong(long value) {
    return new Literal(value, LongType.LONG);
  }

  /**
   * Create a {@code float} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link FloatType}
   */
  public static Literal ofFloat(float value) {
    return new Literal(value, FloatType.FLOAT);
  }

  /**
   * Create a {@code double} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link DoubleType}
   */
  public static Literal ofDouble(double value) {
    return new Literal(value, DoubleType.DOUBLE);
  }

  /**
   * Create a {@code string} type literal expression.
   *
   * @param value literal value
   * @return a {@link Literal} of type {@link StringType}
   */
  public static Literal ofString(String value, String collationName) {
    return new Literal(value, new StringType(CollationIdentifier.fromString(collationName)));
  }

  public static Literal ofString(String value, CollationIdentifier identifier) {
    return new Literal(value, new StringType(identifier));
  }

  /**
   * Create a {@code binary} type literal expression.
   *
   * @param value binary literal value as an array of bytes
   * @return a {@link Literal} of type {@link BinaryType}
   */
  public static Literal ofBinary(byte[] value) {
    return new Literal(value, BinaryType.BINARY);
  }

  /**
   * Create a {@code date} type literal expression.
   *
   * @param daysSinceEpochUTC number of days since the epoch in UTC timezone.
   * @return a {@link Literal} of type {@link DateType}
   */
  public static Literal ofDate(int daysSinceEpochUTC) {
    return new Literal(daysSinceEpochUTC, DateType.DATE);
  }

  /**
   * Create a {@code timestamp} type literal expression.
   *
   * @param microsSinceEpochUTC microseconds since epoch time in UTC timezone.
   * @return a {@link Literal} with data type {@link TimestampType}
   */
  public static Literal ofTimestamp(long microsSinceEpochUTC) {
    return new Literal(microsSinceEpochUTC, TimestampType.TIMESTAMP);
  }

  /**
   * Create a {@code timestamp_ntz} type literal expression.
   *
   * @param microSecondsEpoch Microseconds since epoch with no timezone.
   * @return a {@link Literal} with data type {@link TimestampNTZType}
   */
  public static Literal ofTimestampNtz(long microSecondsEpoch) {
    return new Literal(microSecondsEpoch, TimestampNTZType.TIMESTAMP_NTZ);
  }

  /**
   * Create a {@code decimal} type literal expression.
   *
   * @param value decimal literal value
   * @param precision precision of the decimal literal
   * @param scale scale of the decimal literal
   * @return a {@link Literal} with data type {@link DecimalType} with given {@code precision} and
   *     {@code scale}.
   */
  public static Literal ofDecimal(BigDecimal value, int precision, int scale) {
    // throws an error if rounding is required to set the specified scale
    BigDecimal valueToStore = value.setScale(scale);
    checkArgument(
        valueToStore.precision() <= precision,
        String.format(
            "Decimal precision=%s for decimal %s exceeds max precision %s",
            valueToStore.precision(), valueToStore, precision));
    return new Literal(valueToStore, new DecimalType(precision, scale));
  }

  /**
   * Create {@code null} value literal.
   *
   * @param dataType {@link DataType} of the null literal.
   * @return a null {@link Literal} with the given data type
   */
  public static Literal ofNull(DataType dataType) {
    return new Literal(null, dataType);
  }

  private final Object value;
  private final DataType dataType;

  private Literal(Object value, DataType dataType) {
    if (dataType instanceof ArrayType
        || dataType instanceof MapType
        || dataType instanceof StructType) {
      throw new IllegalArgumentException(dataType + " is an invalid data type for Literal.");
    }
    this.value = value;
    this.dataType = dataType;
  }

  /**
   * Get the literal value. If the value is null a {@code null} is returned. For non-null literal
   * the returned value is one of the following types based on the literal data type.
   *
   * <ul>
   *   <li>BOOLEAN: {@link Boolean}
   *   <li>BYTE: {@link Byte}
   *   <li>SHORT: {@link Short}
   *   <li>INTEGER: {@link Integer}
   *   <li>LONG: {@link Long}
   *   <li>FLOAT: {@link Float}
   *   <li>DOUBLE: {@link Double}
   *   <li>DATE: {@link Integer} represents the number of days since epoch in UTC
   *   <li>TIMESTAMP: {@link Long} represents the microseconds since epoch in UTC
   *   <li>TIMESTAMP_NTZ: {@link Long} represents the microseconds since epoch with no timezone
   *   <li>DECIMAL: {@link BigDecimal}.Use {@link #getDataType()} to find the precision and scale
   * </ul>
   *
   * @return Literal value.
   */
  public Object getValue() {
    return value;
  }

  /**
   * Get the datatype of the literal object. Datatype lets the caller interpret the value of the
   * literal object returned by {@link #getValue()}
   *
   * @return Datatype of the literal object.
   */
  public DataType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }

  @Override
  public List<Expression> getChildren() {
    return Collections.emptyList();
  }
}
