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
package io.delta.kernel.internal.util;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static java.util.Arrays.asList;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;
import static io.delta.kernel.expressions.AlwaysFalse.ALWAYS_FALSE;
import static io.delta.kernel.expressions.AlwaysTrue.ALWAYS_TRUE;

import io.delta.kernel.internal.InternalScanFileUtils;

public class PartitionUtils {
    private PartitionUtils() {}

    /**
     * Utility method to remove the given columns (as {@code columnsToRemove}) from the
     * given {@code physicalSchema}.
     *
     * @param physicalSchema
     * @param logicalSchema   To create a logical name to physical name map. Partition column names
     *                        are in logical space and we need to identify the equivalent
     *                        physical column name.
     * @param columnsToRemove
     * @return
     */
    public static StructType physicalSchemaWithoutPartitionColumns(
        StructType logicalSchema, StructType physicalSchema, Set<String> columnsToRemove) {
        if (columnsToRemove == null || columnsToRemove.size() == 0) {
            return physicalSchema;
        }

        // Partition columns are top-level only
        Map<String, String> physicalToLogical = new HashMap<String, String>() {
            {
                IntStream.range(0, logicalSchema.length())
                    .mapToObj(i -> new Tuple2<>(logicalSchema.at(i), physicalSchema.at(i)))
                    .forEach(tuple2 -> put(tuple2._2.getName(), tuple2._1.getName()));
            }
        };

        return new StructType(
            physicalSchema.fields().stream()
                .filter(field ->
                    !columnsToRemove.contains(physicalToLogical.get(field.getName())))
                .collect(Collectors.toList()));
    }

    public static ColumnarBatch withPartitionColumns(
        ExpressionHandler expressionHandler,
        ColumnarBatch dataBatch,
        Map<String, String> partitionValues,
        StructType schemaWithPartitionCols) {
        if (partitionValues == null || partitionValues.size() == 0) {
            // no partition column vectors to attach to.
            return dataBatch;
        }

        for (int colIdx = 0; colIdx < schemaWithPartitionCols.length(); colIdx++) {
            StructField structField = schemaWithPartitionCols.at(colIdx);

            if (partitionValues.containsKey(structField.getName())) {
                // Create a partition vector

                ExpressionEvaluator evaluator = expressionHandler.getEvaluator(
                    dataBatch.getSchema(),
                    literalForPartitionValue(
                        structField.getDataType(),
                        partitionValues.get(structField.getName())),
                    structField.getDataType()
                );

                ColumnVector partitionVector = evaluator.eval(dataBatch);
                dataBatch = dataBatch.withNewColumn(colIdx, structField, partitionVector);
            }
        }

        return dataBatch;
    }

    /**
     * Split the given predicate into predicate on partition columns and predicate on data columns.
     *
     * @param predicate
     * @param partitionColNames
     * @return Tuple of partition column predicate and data column predicate.
     */
    public static Tuple2<Predicate, Predicate> splitMetadataAndDataPredicates(
        Predicate predicate,
        Set<String> partitionColNames) {
        String predicateName = predicate.getName();
        List<Expression> children = predicate.getChildren();
        if ("AND".equalsIgnoreCase(predicateName)) {
            Predicate left = (Predicate) children.get(0);
            Predicate right = (Predicate) children.get(1);
            Tuple2<Predicate, Predicate> leftResult =
                splitMetadataAndDataPredicates(left, partitionColNames);
            Tuple2<Predicate, Predicate> rightResult =
                splitMetadataAndDataPredicates(right, partitionColNames);

            return new Tuple2<>(
                combineWithAndOp(leftResult._1, rightResult._1),
                combineWithAndOp(leftResult._2, rightResult._2));
        }
        if (hasNonPartitionColumns(children, partitionColNames)) {
            return new Tuple2(ALWAYS_TRUE, predicate);
        } else {
            return new Tuple2<>(predicate, ALWAYS_TRUE);
        }
    }

    /**
     * Rewrite the given predicate on partition columns on `partitionValues_parsed` in checkpoint
     * schema. The rewritten predicate can be pushed to the Parquet reader when reading the
     * checkpoint files.
     *
     * @param predicate Predicate on partition columns.
     * @param partitionColMetadata Map of partition column name (in lower case) to its type.
     * @return Rewritten {@link Predicate} on `partitionValues_parsed` in `add`.
     */
    public static Predicate rewritePartitionPredicateOnCheckpointFileSchema(
            Predicate predicate,
            Map<String, StructField> partitionColMetadata) {
        return new Predicate(
                predicate.getName(),
                predicate.getChildren().stream()
                        .map(child ->
                                rewriteColRefOnPartitionValuesParsed(child, partitionColMetadata))
                        .collect(Collectors.toList()));
    }

    private static Expression rewriteColRefOnPartitionValuesParsed(
            Expression expression,
            Map<String, StructField> partitionColMetadata) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            String partColName = column.getNames()[0];
            StructField partColField =
                    partitionColMetadata.get(partColName.toLowerCase(Locale.ROOT));
            if (partColField == null) {
                throw new IllegalArgumentException(partColName + " is not present in metadata");
            }

            String partColPhysicalName = ColumnMapping.getPhysicalName(partColField);

            return InternalScanFileUtils.getPartitionValuesParsedRefInAddFile(partColPhysicalName);
        } else if (expression instanceof Predicate) {
            return rewritePartitionPredicateOnCheckpointFileSchema(
                    (Predicate) expression,
                    partitionColMetadata);
        }

        return expression;
    }

    /**
     * Utility method to rewrite the partition predicate referring to the table schema as predicate
     * referring to the {@code partitionValues} in scan files read from Delta log. The scan file
     * batch is returned by the {@link io.delta.kernel.Scan#getScanFiles(TableClient)}.
     * <p>
     * E.g. given predicate on partition columns:
     *   {@code p1 = 'new york' && p2 >= 26} where p1 is of type string and p2 is of int
     * Rewritten expression looks like:
     *   {@code element_at(Column('add', 'partitionValues'), 'p1') = 'new york'
     *      &&
     *   partition_value(element_at(Column('add', 'partitionValues'), 'p2'), 'integer') >= 26}
     *
     * The column `add.partitionValues` is a {@literal map(string -> string)} type. Each partition
     * values is in string serialization format according to the Delta protocol. Expression
     * `partition_value` deserializes the string value into the given partition column type value.
     * String type partition values don't need any deserialization.
     *
     * @param predicate            Predicate containing filters only on partition columns.
     * @param partitionColMetadata Map of partition column name (in lower case) to its type.
     * @return
     */
    public static Predicate rewritePartitionPredicateOnScanFileSchema(
        Predicate predicate, Map<String, StructField> partitionColMetadata) {
        return new Predicate(
            predicate.getName(),
            predicate.getChildren().stream()
                .map(child -> rewritePartitionColumnRef(child, partitionColMetadata))
                .collect(Collectors.toList()));
    }

    private static Expression rewritePartitionColumnRef(
        Expression expression, Map<String, StructField> partitionColMetadata) {
        Column scanFilePartitionValuesRef = InternalScanFileUtils.ADD_FILE_PARTITION_COL_REF;
        if (expression instanceof Column) {
            Column column = (Column) expression;
            String partColName = column.getNames()[0];
            StructField partColField =
                partitionColMetadata.get(partColName.toLowerCase(Locale.ROOT));
            if (partColField == null) {
                throw new IllegalArgumentException(partColName + " is not present in metadata");
            }
            DataType partColType = partColField.getDataType();
            String partColPhysicalName = ColumnMapping.getPhysicalName(partColField);

            Expression elementAt =
                new ScalarExpression(
                    "element_at",
                    asList(scanFilePartitionValuesRef, Literal.ofString(partColPhysicalName)));

            if (partColType instanceof StringType) {
                return elementAt;
            }

            // Add expression to decode the partition value based on the partition column type.
            return new PartitionValueExpression(elementAt, partColType);
        } else if (expression instanceof Predicate) {
            return rewritePartitionPredicateOnScanFileSchema(
                (Predicate) expression, partitionColMetadata);
        }

        return expression;
    }

    private static boolean hasNonPartitionColumns(
        List<Expression> children,
        Set<String> partitionColNames) {
        for (Expression child : children) {
            if (child instanceof Column) {
                String[] names = ((Column) child).getNames();
                // Partition columns are never of nested types.
                if (names.length != 1 ||
                        !partitionColNames.contains(names[0].toLowerCase(Locale.ROOT))) {
                    return true;
                }
            } else {
                if(hasNonPartitionColumns(child.getChildren(), partitionColNames)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Predicate combineWithAndOp(Predicate left, Predicate right) {
        String leftName = left.getName().toUpperCase();
        String rightName = right.getName().toUpperCase();
        if (leftName.equals("ALWAYS_FALSE") || rightName.equals("ALWAYS_FALSE")) {
            return ALWAYS_FALSE;
        }
        if (leftName.equals("ALWAYS_TRUE")) {
            return right;
        }
        if (rightName.equals("ALWAYS_TRUE")) {
            return left;
        }
        return new And(left, right);
    }

    private static Literal literalForPartitionValue(DataType dataType, String partitionValue) {
        if (partitionValue == null) {
            return Literal.ofNull(dataType);
        }

        if (dataType instanceof BooleanType) {
            return Literal.ofBoolean(Boolean.parseBoolean(partitionValue));
        }
        if (dataType instanceof ByteType) {
            return Literal.ofByte(Byte.parseByte(partitionValue));
        }
        if (dataType instanceof ShortType) {
            return Literal.ofShort(Short.parseShort(partitionValue));
        }
        if (dataType instanceof IntegerType) {
            return Literal.ofInt(Integer.parseInt(partitionValue));
        }
        if (dataType instanceof LongType) {
            return Literal.ofLong(Long.parseLong(partitionValue));
        }
        if (dataType instanceof FloatType) {
            return Literal.ofFloat(Float.parseFloat(partitionValue));
        }
        if (dataType instanceof DoubleType) {
            return Literal.ofDouble(Double.parseDouble(partitionValue));
        }
        if (dataType instanceof StringType) {
            return Literal.ofString(partitionValue);
        }
        if (dataType instanceof BinaryType) {
            return Literal.ofBinary(partitionValue.getBytes());
        }
        if (dataType instanceof DateType) {
            return Literal.ofDate(InternalUtils.daysSinceEpoch(Date.valueOf(partitionValue)));
        }
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return Literal.ofDecimal(
                new BigDecimal(partitionValue), decimalType.getPrecision(), decimalType.getScale());
        }
        if (dataType instanceof TimestampType) {
            return Literal.ofTimestamp(
                InternalUtils.microsSinceEpoch(Timestamp.valueOf(partitionValue)));
        }
        if (dataType instanceof TimestampNTZType) {
            // Both the timestamp and timestamp_ntz have no timezone info, so they are interpreted
            // in local time zone.
            return Literal.ofTimestampNtz(
                    InternalUtils.microsSinceEpoch(Timestamp.valueOf(partitionValue)));
        }

        throw new UnsupportedOperationException("Unsupported partition column: " + dataType);
    }
}
