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

import java.sql.Date;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ExpressionEvaluator;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.lang.ListUtils;

public class PartitionUtils
{
    private PartitionUtils() {}

    public static Map<String, Integer> getPartitionOrdinals(
        StructType snapshotSchema,
        StructType partitionSchema)
    {
        final Map<String, Integer> output = new HashMap<>();
        partitionSchema
            .fieldNames()
            .forEach(fieldName -> output.put(fieldName, snapshotSchema.indexOf(fieldName)));

        return output;
    }

    /**
     * Partition the given condition into two optional conjunctive predicates M, D such that
     * condition = M AND D, where we define:
     * - M: conjunction of predicates that can be evaluated using metadata only.
     * - D: conjunction of other predicates.
     */
    public static Tuple2<Optional<Expression>, Optional<Expression>> splitMetadataAndDataPredicates(
        Expression condition,
        List<String> partitionColumns)
    {
        final Tuple2<List<Expression>, List<Expression>> metadataAndDataPredicates = ListUtils
            .partition(
                splitConjunctivePredicates(condition),
                c -> isPredicateMetadataOnly(c, partitionColumns)
            );

        final Optional<Expression> metadataConjunction;
        if (metadataAndDataPredicates._1.isEmpty()) {
            metadataConjunction = Optional.empty();
        }
        else {
            metadataConjunction = Optional.of(And.apply(metadataAndDataPredicates._1));
        }

        final Optional<Expression> dataConjunction;
        if (metadataAndDataPredicates._2.isEmpty()) {
            dataConjunction = Optional.empty();
        }
        else {
            dataConjunction = Optional.of(And.apply(metadataAndDataPredicates._2));
        }
        return new Tuple2<>(metadataConjunction, dataConjunction);
    }

    private static List<Expression> splitConjunctivePredicates(Expression condition)
    {
        if (condition instanceof And) {
            final And andExpr = (And) condition;
            return Stream.concat(
                splitConjunctivePredicates(andExpr.getLeft()).stream(),
                splitConjunctivePredicates(andExpr.getRight()).stream()
            ).collect(Collectors.toList());
        }
        return Collections.singletonList(condition);
    }

    private static boolean isPredicateMetadataOnly(
        Expression condition,
        List<String> partitionColumns)
    {
        Set<String> lowercasePartCols = partitionColumns
            .stream().map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());

        return condition
            .references()
            .stream()
            .map(s -> s.toLowerCase(Locale.ROOT))
            .allMatch(lowercasePartCols::contains);
    }

    /**
     * Utility method to remove the given columns (as {@code columnsToRemove}) from the
     * given {@code schema}.
     *
     * @param schema
     * @param columnsToRemove
     * @return
     */
    public static StructType withColumnsRemoved(StructType schema, Set<String> columnsToRemove)
    {
        if (columnsToRemove == null || columnsToRemove.size() == 0) {
            return schema;
        }

        return new StructType(
            schema.fields().stream()
                .filter(field -> !columnsToRemove.contains(field.getName()))
                .collect(Collectors.toList()));
    }

    public static ColumnarBatch withPartitionColumns(
        ExpressionHandler expressionHandler,
        ColumnarBatch dataBatch,
        StructType dataBatchSchema,
        Map<String, String> partitionValues,
        StructType schemaWithPartitionCols)
    {
        if (partitionValues == null || partitionValues.size() == 0) {
            // no partition column vectors to attach to.
            return dataBatch;
        }

        for (int colIdx = 0; colIdx < schemaWithPartitionCols.length(); colIdx++) {
            StructField structField = schemaWithPartitionCols.at(colIdx);

            if (partitionValues.containsKey(structField.getName())) {
                // Create a partition vector

                ExpressionEvaluator evaluator = expressionHandler.getEvaluator(
                    dataBatchSchema,
                    literalForPartitionValue(
                        structField.getDataType(),
                        partitionValues.get(structField.getName())
                    )
                );

                ColumnVector partitionVector = evaluator.eval(dataBatch);
                dataBatch.insertVector(colIdx, structField, partitionVector);
            }
        }

        return dataBatch;
    }

    private static Literal literalForPartitionValue(DataType dataType, String partitionValue)
    {
        if (partitionValue == null) {
            return Literal.ofNull(dataType);
        }

        if (dataType instanceof BooleanType) {
            return Literal.of(Boolean.parseBoolean(partitionValue));
        }
        if (dataType instanceof ByteType) {
            return Literal.of(Byte.parseByte(partitionValue));
        }
        if (dataType instanceof ShortType) {
            return Literal.of(Short.parseShort(partitionValue));
        }
        if (dataType instanceof IntegerType) {
            return Literal.of(Integer.parseInt(partitionValue));
        }
        if (dataType instanceof LongType) {
            return Literal.of(Long.parseLong(partitionValue));
        }
        if (dataType instanceof FloatType) {
            return Literal.of(Float.parseFloat(partitionValue));
        }
        if (dataType instanceof DoubleType) {
            return Literal.of(Double.parseDouble(partitionValue));
        }
        if (dataType instanceof StringType) {
            return Literal.of(partitionValue);
        }
        if (dataType instanceof BinaryType) {
            return Literal.of(partitionValue.getBytes());
        }
        if (dataType instanceof DateType) {
            return Literal.of(Date.valueOf(partitionValue));
        }

        throw new UnsupportedOperationException("Unsupported partition column: " + dataType);
    }
}
