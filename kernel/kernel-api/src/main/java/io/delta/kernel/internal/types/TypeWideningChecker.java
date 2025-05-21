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

package io.delta.kernel.internal.types;

import io.delta.kernel.types.*;

/**
 * Utility class for checking type widening compatibility according to the Delta protocol.
 *
 * <p>The Type Widening feature enables changing the type of a column or field in an existing Delta
 * table to a wider type. This class implements the checks required by the protocol to ensure that
 * only supported type changes are allowed.
 *
 * <p>Supported type changes as per protocol:
 *
 * <ul>
 *   <li>Integer widening: {@code Byte -> Short -> Int -> Long}
 *   <li>Floating-point widening: {@code Float -> Double}
 *   <li>Floating-point widening: {@code Byte, Short or Int -> Double}
 *   <li>Date widening: {@code Date -> Timestamp without timezone}
 *   <li>Decimal widening: {@code Decimal(p, s) -> Decimal(p + k1, s + k2)} where {@code k1 >= k2 >=
 *       0}
 *   <li>Decimal widening: {@code Byte, Short or Int -> Decimal(10 + k1, k2)} where {@code k1 >= k2
 *       >= 0}
 *   <li>Decimal widening: {@code Long -> Decimal(20 + k1, k2)} where {@code k1 >= k2 >= 0}
 * </ul>
 */
public class TypeWideningChecker {
  private TypeWideningChecker() {}

  /**
   * Checks if a type change from sourceType to targetType is a supported widening operation.
   *
   * @param sourceType The original data type
   * @param targetType The target data type to widen to
   * @return true if the type change is a supported widening operation (or the types are equal),
   *     false otherwise
   */
  public static boolean isWideningSupported(DataType sourceType, DataType targetType) {
    // Iceberg V2 type widening is a strict subset of Delta type widening
    if (isIcebergV2Compatible(sourceType, targetType)) {
      return true;
    }

    // Floating-point widening: Byte, Short or Int -> Double
    if (isIntegerToDoubleWidening(sourceType, targetType)) {
      return true;
    }

    // Date widening: Date -> Timestamp without timezone
    if (isDateToTimestampNtzWidening(sourceType, targetType)) {
      return true;
    }

    // Decimal widening
    if (isDecimalWidening(sourceType, targetType)) {
      return true;
    }

    // No supported widening found
    return false;
  }

  /**
   * Checks if the type change is supported by Iceberg V2 schema evolution. This is required when
   * Iceberg compatibility is enabled.
   *
   * @param sourceType The original data type
   * @param targetType The target data type to widen to
   * @return true if the type change is supported by Iceberg V2 (sourceType == targetType returns
   *     true), false otherwise
   */
  public static boolean isIcebergV2Compatible(DataType sourceType, DataType targetType) {
    // If types are the same, it's not a widening operation
    if (sourceType.equals(targetType)) {
      return true;
    }

    // Integer widening: Byte -> Short -> Int -> Long
    if (isIntegerWidening(sourceType, targetType)) {
      return true;
    }

    // Floating-point widening: Float -> Double
    if (isFloatToDoubleWidening(sourceType, targetType)) {
      return true;
    }

    // Check if it's a decimal widening with scale increase
    if (sourceType instanceof DecimalType && targetType instanceof DecimalType) {
      DecimalType sourceDecimal = (DecimalType) sourceType;
      DecimalType targetDecimal = (DecimalType) targetType;

      // If scale changes, are not supported by Iceberg
      if (targetDecimal.getScale() != sourceDecimal.getScale()) {
        return false;
      }

      // Precision increase with same scale is supported.
      return targetDecimal.getPrecision() >= sourceDecimal.getPrecision();
    }

    // No other supported widening for Iceberg
    return false;
  }

  /** Checks if the type change is an integer widening (Byte -> Short -> Int -> Long). */
  private static boolean isIntegerWidening(DataType sourceType, DataType targetType) {
    if (sourceType instanceof ByteType) {
      return targetType instanceof ShortType
          || targetType instanceof IntegerType
          || targetType instanceof LongType;
    } else if (sourceType instanceof ShortType) {
      return targetType instanceof IntegerType || targetType instanceof LongType;
    } else if (sourceType instanceof IntegerType) {
      return targetType instanceof LongType;
    }
    return false;
  }

  /** Checks if the type change is a Float to Double widening. */
  private static boolean isFloatToDoubleWidening(DataType sourceType, DataType targetType) {
    return sourceType instanceof FloatType && targetType instanceof DoubleType;
  }

  /** Checks if the type change is an integer to Double widening. */
  private static boolean isIntegerToDoubleWidening(DataType sourceType, DataType targetType) {
    return (sourceType instanceof ByteType
            || sourceType instanceof ShortType
            || sourceType instanceof IntegerType)
        && targetType instanceof DoubleType;
  }

  /** Checks if the type change is a Date to TimestampNTZ widening. */
  private static boolean isDateToTimestampNtzWidening(DataType sourceType, DataType targetType) {
    return sourceType instanceof DateType && targetType instanceof TimestampNTZType;
  }

  /** Checks if the type change is a supported decimal widening. */
  private static boolean isDecimalWidening(DataType sourceType, DataType targetType) {
    // Decimal(p, s) -> Decimal(p + k1, s + k2) where k1 >= k2 >= 0
    if (sourceType instanceof DecimalType && targetType instanceof DecimalType) {
      DecimalType sourceDecimal = (DecimalType) sourceType;
      DecimalType targetDecimal = (DecimalType) targetType;

      int precisionDiff = targetDecimal.getPrecision() - sourceDecimal.getPrecision();
      int scaleDiff = targetDecimal.getScale() - sourceDecimal.getScale();

      return precisionDiff >= scaleDiff && scaleDiff >= 0;
    }

    // Byte, Short or Int -> Decimal(10 + k1, k2) where k1 >= k2 >= 0
    if ((sourceType instanceof ByteType
            || sourceType instanceof ShortType
            || sourceType instanceof IntegerType)
        && targetType instanceof DecimalType) {

      DecimalType targetDecimal = (DecimalType) targetType;
      int basePrecision = 10;

      return targetDecimal.getPrecision() >= basePrecision
          && (targetDecimal.getPrecision() - basePrecision) >= targetDecimal.getScale()
          && targetDecimal.getScale() >= 0;
    }

    // Long -> Decimal(20 + k1, k2) where k1 >= k2 >= 0
    if (sourceType instanceof LongType && targetType instanceof DecimalType) {
      DecimalType targetDecimal = (DecimalType) targetType;
      int basePrecision = 20;

      return targetDecimal.getPrecision() >= basePrecision
          && (targetDecimal.getPrecision() - basePrecision) >= targetDecimal.getScale()
          && targetDecimal.getScale() >= 0;
    }

    return false;
  }
}
