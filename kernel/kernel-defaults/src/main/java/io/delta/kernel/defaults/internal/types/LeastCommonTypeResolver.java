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
package io.delta.kernel.defaults.internal.types;

import java.util.*;

import io.delta.kernel.types.*;

/**
 * The least common type from a set of types is the narrowest type reachable from the
 *     precedence list by all elements of the set of types.
 * This class contains helper function to resolve a list of type into their least
 *     common type
 * Source of truth for type precedence is defined here
 * <a href="https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html#least-common-type-resolution">
 */
public class LeastCommonTypeResolver {

    private LeastCommonTypeResolver() {
    }

    public static DataType resolveLeastCommonType(List<DataType> dataTypes) {
        byte distinctOutputGroups = 0;
        boolean hasOutputGroupWithNoTypePromotion = false;
        boolean hasGroup1 = false;
        boolean hasGroup2 = false;
        boolean hasGroup3 = false;
        boolean hasGroup4 = false;
        boolean hasGroup5 = false;
        boolean hasGroup6 = false;
        boolean hasGroup7 = false;
        boolean hasGroup8 = false;
        boolean hasGroup9 = false;
        byte maxPrecedence = 0;
        for (DataType type : dataTypes) {
            byte group = type.getPromotionGroup(type);
            byte precedence;
            switch (group) {
                case PromotionGroup.NUMBER_GROUP:
                    precedence = type.getPromotionPrecedence(type);
                    if (!hasGroup1) {
                        distinctOutputGroups += 1;
                        hasGroup1 = true;
                    }
                    if (maxPrecedence < precedence) {
                        maxPrecedence = precedence;
                    }
                    break;
                case PromotionGroup.STRING_GROUP:
                    if (!hasGroup2) {
                        distinctOutputGroups += 1;
                        hasGroup2 = true;
                    }
                    break;
                case PromotionGroup.TIME_GROUP:
                    precedence = type.getPromotionPrecedence(type);
                    if (!hasGroup3) {
                        distinctOutputGroups += 1;
                        hasGroup3 = true;
                    }
                    if (maxPrecedence < precedence) {
                        maxPrecedence = precedence;
                    }
                    break;
                case PromotionGroup.BINARY_GROUP:
                    if (!hasGroup4) {
                        distinctOutputGroups += 1;
                        hasGroup4 = true;
                    }
                    break;
                case PromotionGroup.BOOLEAN_GROUP:
                    if (!hasGroup5) {
                        distinctOutputGroups += 1;
                        hasGroup5 = true;
                    }
                    break;
                case PromotionGroup.INTERVAL_GROUP:
                    if (!hasGroup6) {
                        distinctOutputGroups += 1;
                        hasGroup6 = true;
                    }
                    break;
                case PromotionGroup.ARRAY_GROUP:
                    if (!hasGroup7) {
                        distinctOutputGroups += 1;
                        hasGroup7 = true;
                        hasOutputGroupWithNoTypePromotion = true;
                    }
                    break;
                case PromotionGroup.MAP_GROUP:
                    if (!hasGroup8) {
                        distinctOutputGroups += 1;
                        hasGroup8 = true;
                        hasOutputGroupWithNoTypePromotion = true;
                    }
                    break;
                case PromotionGroup.STRUCT_GROUP:
                    if (!hasGroup9) {
                        distinctOutputGroups += 1;
                        hasGroup9 = true;
                        hasOutputGroupWithNoTypePromotion = true;
                    }
                    break;
                default:
                    String msg = String.format(
                            "`%s` Does not have least common type resolution`",
                            group);
                    throw new IllegalStateException(msg);
            }
        }

        if (distinctOutputGroups > 2) {
            throw new IllegalStateException(
                    "There are not least common type for group greater than 2");
        } else if (distinctOutputGroups == 2) {
            if (hasOutputGroupWithNoTypePromotion) {
                throw new IllegalStateException("There are no least common type resolution");
            }
            if (hasGroup1 && hasGroup2) {
                if (maxPrecedence > PromotionGroup.NUMBER_PRECEDENCE_LONG) {
                    return DoubleType.DOUBLE;
                }
                return LongType.LONG;
            } else if (hasGroup2 && hasGroup3) {
                return getDateTypeByPrecedence(maxPrecedence);
            } else if (hasGroup2 && hasGroup4) {
                return BinaryType.BINARY;
            } else if (hasGroup2 && hasGroup5) {
                return BooleanType.BOOLEAN;
            } else {
                throw new IllegalStateException("There are no least common type resolution");
            }
        } else if (distinctOutputGroups == 1) {
            if (hasGroup1) {
                return getNumberTypeByPrecedence(maxPrecedence);
            } else if (hasGroup2) {
                return StringType.STRING;
            } else if (hasGroup3) {
                return getDateTypeByPrecedence(maxPrecedence);
            } else if (hasGroup4) {
                return BinaryType.BINARY;
            } else if (hasGroup5) {
                return BooleanType.BOOLEAN;
            } else if (hasGroup6) {
                throw new IllegalStateException(
                        "Interval is not supported yet for least common type resolution");
            } else if (hasGroup7) {
                throw new IllegalStateException(
                        "ArrayType is not supported yet for least common type resolution");
            } else if (hasGroup8) {
                throw new IllegalStateException(
                        "MapType is not supported yet for least common type resolution");
            } else if (hasGroup9) {
                throw new IllegalStateException(
                        "StructType is not supported yet for least common type resolution");
            } else {
                throw new IllegalStateException(
                        "There are no least common type resolution");
            }
        } else {
            throw new IllegalStateException("There are no least common type resolution");
        }
    }

    protected static DataType getDateTypeByPrecedence(byte precedence) {
        switch (precedence) {
            case PromotionGroup.TIME_PRECEDENCE_DATE:
                return DateType.DATE;
            case PromotionGroup.TIME_PRECEDENCE_TIMESTAMP:
                return TimestampType.TIMESTAMP;
            default:
                String msg = String.format(
                        "`%s` is not supported for least common type " +
                                "resolution for time related type`",
                        precedence);
                throw new IllegalStateException(msg);
        }
    }

    protected static DataType getNumberTypeByPrecedence(byte precedence) {
        switch (precedence) {
            case PromotionGroup.NUMBER_PRECEDENCE_BYTE:
                return ByteType.BYTE;
            case PromotionGroup.NUMBER_PRECEDENCE_SHORT:
                return ShortType.SHORT;
            case PromotionGroup.NUMBER_PRECEDENCE_INT:
                return IntegerType.INTEGER;
            case PromotionGroup.NUMBER_PRECEDENCE_LONG:
                return LongType.LONG;
            case PromotionGroup.NUMBER_PRECEDENCE_DECIMAL:
                return DecimalType.USER_DEFAULT;
            case PromotionGroup.NUMBER_PRECEDENCE_FLOAT:
                return FloatType.FLOAT;
            case PromotionGroup.NUMBER_PRECEDENCE_DOUBLE:
                return DoubleType.DOUBLE;
            default:
                String msg = String.format(
                        "`%s` is not supported for least common type " +
                                "resolution for number related type`",
                        precedence);
                throw new IllegalStateException(msg);
        }
    }
    public static boolean isCompatible(DataType a, DataType b) {
        if (a instanceof ByteType) {
            return (b instanceof ByteType) ||
                    (b instanceof ShortType) ||
                    (b instanceof IntegerType) ||
                    (b instanceof LongType) ||
                    (b instanceof DecimalType) ||
                    (b instanceof FloatType) ||
                    (b instanceof DoubleType);
        } else if (a instanceof  ShortType) {
            return (b instanceof ShortType) ||
                    (b instanceof IntegerType) ||
                    (b instanceof LongType) ||
                    (b instanceof DecimalType) ||
                    (b instanceof FloatType) ||
                    (b instanceof DoubleType);
        } else if (a instanceof  IntegerType) {
            return (b instanceof IntegerType) ||
                    (b instanceof LongType) ||
                    (b instanceof DecimalType) ||
                    (b instanceof FloatType) ||
                    (b instanceof DoubleType);
        } else if (a instanceof  LongType) {
            return (b instanceof LongType) ||
                    (b instanceof DecimalType) ||
                    (b instanceof FloatType) ||
                    (b instanceof DoubleType);
        } else if (a instanceof  DecimalType) {
            return  (b instanceof DecimalType) ||
                    (b instanceof FloatType) ||
                    (b instanceof DoubleType);
        } else if (a instanceof  FloatType) {
            return (b instanceof FloatType) ||
                    (b instanceof DoubleType);
        } else if (a instanceof  DoubleType) {
            return (b instanceof DoubleType);
        } else if (a instanceof  DateType) {
            return (b instanceof DateType) ||
                    (b instanceof TimestampType);
        } else if (a instanceof  TimestampType) {
            return (b instanceof TimestampType);
        } else if (a instanceof  StringType) {
            return  (b instanceof StringType) ||
                    (b instanceof LongType) ||
                    (b instanceof DoubleType) ||
                    (b instanceof DateType) ||
                    (b instanceof TimestampType) ||
                    (b instanceof BooleanType) ||
                    (b instanceof BinaryType);
        } else if (a instanceof  BinaryType) {
            return (b instanceof BinaryType);
        } else if (a instanceof  BooleanType) {
            return (b instanceof BooleanType);
        } else if (a instanceof  MapType) {
            return (b instanceof MapType);
        } else if (a instanceof  ArrayType) {
            return (b instanceof ArrayType);
        } else if (a instanceof  StructType) {
            return (b instanceof StructType);
        } else {
            String msg = String.format(
                    "`%s` type is not supported for compatible type check",
                    a);
            throw new IllegalStateException(msg);
        }

    }
}
