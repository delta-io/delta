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
package io.delta.kernel.defaults.internal.expressions;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

import io.delta.kernel.internal.DeltaErrors;
import static io.delta.kernel.internal.util.ExpressionUtils.getLeft;
import static io.delta.kernel.internal.util.ExpressionUtils.getRight;
import static io.delta.kernel.internal.util.ExpressionUtils.getUnaryChild;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.vector.DefaultBooleanVector;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.booleanWrapperVector;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.childAt;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.compare;
import static io.delta.kernel.defaults.internal.expressions.DefaultExpressionUtils.evalNullability;
import static io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo;

/**
 * Implementation of {@link ExpressionEvaluator} for default {@link ExpressionHandler}.
 * It takes care of validating, adding necessary implicit casts and evaluating the
 * {@link Expression} on given {@link ColumnarBatch}.
 */
public class DefaultExpressionEvaluator implements ExpressionEvaluator {
    private final Expression expression;

    /**
     * Create a {@link DefaultExpressionEvaluator} instance bound to the given expression and
     * <i>inputSchem</i>.
     *
     * @param inputSchema Input data schema
     * @param expression  Expression to evaluate.
     * @param outputType  Expected result data type.
     */
    public DefaultExpressionEvaluator(
        StructType inputSchema,
        Expression expression,
        DataType outputType) {
        ExpressionTransformResult transformResult =
            new ExpressionTransformer(inputSchema).visit(expression);
        if (!transformResult.outputType.equivalent(outputType)) {
            String reason = format(
                    "Can not create an expression handler returns result of type %s", outputType);
            throw DeltaErrors.unsupportedExpression(expression, Optional.of(reason));
        }
        this.expression = transformResult.expression;
    }

    @Override
    public ColumnVector eval(ColumnarBatch input) {
        return new ExpressionEvalVisitor(input).visit(expression);
    }

    @Override
    public void close() { /* nothing to close */ }

    /**
     * Encapsulates the result of {@link ExpressionTransformer}
     */
    private static class ExpressionTransformResult {
        public final Expression expression; // transformed expression
        public final DataType outputType; // output type of the expression

        ExpressionTransformResult(Expression expression, DataType outputType) {
            this.expression = expression;
            this.outputType = outputType;
        }
    }

    /**
     * Implementation of {@link ExpressionVisitor} to validate the given expression as follows.
     * <ul>
     *     <li>given input column is part of the input data schema</li>
     *     <li>expression inputs are of supported types. Insert cast according to the rules in
     *     {@link ImplicitCastExpression} to make the types compatible for evaluation by
     *     {@link ExpressionEvalVisitor}
     *     </li>
     * </ul>
     * <p>
     * Return type of each expression visit is a tuple of new rewritten expression and its result
     * data type.
     */
    private static class ExpressionTransformer
        extends ExpressionVisitor<ExpressionTransformResult> {
        private StructType inputDataSchema;

        ExpressionTransformer(StructType inputDataSchema) {
            this.inputDataSchema = requireNonNull(inputDataSchema, "inputDataSchema is null");
        }

        @Override
        ExpressionTransformResult visitAnd(And and) {
            Predicate left = validateIsPredicate(and, visit(and.getLeft()));
            Predicate right = validateIsPredicate(and, visit(and.getRight()));
            return new ExpressionTransformResult(new And(left, right), BooleanType.BOOLEAN);
        }

        @Override
        ExpressionTransformResult visitOr(Or or) {
            Predicate left = validateIsPredicate(or, visit(or.getLeft()));
            Predicate right = validateIsPredicate(or, visit(or.getRight()));
            return new ExpressionTransformResult(new Or(left, right), BooleanType.BOOLEAN);
        }

        @Override
        ExpressionTransformResult visitAlwaysTrue(AlwaysTrue alwaysTrue) {
            // nothing to validate or rewrite.
            return new ExpressionTransformResult(alwaysTrue, BooleanType.BOOLEAN);
        }

        @Override
        ExpressionTransformResult visitAlwaysFalse(AlwaysFalse alwaysFalse) {
            // nothing to validate or rewrite.
            return new ExpressionTransformResult(alwaysFalse, BooleanType.BOOLEAN);
        }

        @Override
        ExpressionTransformResult visitComparator(Predicate predicate) {
            switch (predicate.getName()) {
                case "=":
                case ">":
                case ">=":
                case "<":
                case "<=":
                    return new ExpressionTransformResult(
                        transformBinaryComparator(predicate),
                        BooleanType.BOOLEAN);
                default:
                    throw DeltaErrors.unsupportedExpression(
                            predicate, Optional.of("unsupported expression encountered"));
            }
        }

        @Override
        ExpressionTransformResult visitLiteral(Literal literal) {
            // nothing to validate or rewrite
            return new ExpressionTransformResult(literal, literal.getDataType());
        }

        @Override
        ExpressionTransformResult visitColumn(Column column) {
            String[] names = column.getNames();
            DataType currentType = inputDataSchema;
            for (int level = 0; level < names.length; level++) {
                assertColumnExists(currentType instanceof StructType, inputDataSchema, column);
                StructType structSchema = ((StructType) currentType);
                int ordinal = structSchema.indexOf(names[level]);
                assertColumnExists(ordinal != -1, inputDataSchema, column);
                currentType = structSchema.at(ordinal).getDataType();
            }
            assertColumnExists(currentType != null, inputDataSchema, column);
            return new ExpressionTransformResult(column, currentType);
        }

        @Override
        ExpressionTransformResult visitCast(Cast cast) {
            Cast processed = new Cast(visit(cast.getInput()).expression, cast.getOutputType());
            return new ExpressionTransformResult(processed, cast.getOutputType());
        }

        @Override
        ExpressionTransformResult visitPartitionValue(PartitionValueExpression partitionValue) {
            ExpressionTransformResult serializedPartValueInput = visit(partitionValue.getInput());
            checkArgument(
                serializedPartValueInput.outputType instanceof StringType,
                "%s: expected string input, but got %s",
                partitionValue, serializedPartValueInput.outputType);
            DataType partitionColType = partitionValue.getDataType();
            if (partitionColType instanceof StructType ||
                partitionColType instanceof ArrayType ||
                partitionColType instanceof MapType) {
                throw DeltaErrors.unsupportedExpression(
                        partitionValue,
                        Optional.of("unsupported partition data type: " + partitionColType));
            }
            return new ExpressionTransformResult(
                new PartitionValueExpression(serializedPartValueInput.expression, partitionColType),
                partitionColType);
        }

        @Override
        ExpressionTransformResult visitElementAt(ScalarExpression elementAt) {
            ExpressionTransformResult transformedMapInput = visit(childAt(elementAt, 0));
            ExpressionTransformResult transformedLookupKey = visit(childAt(elementAt, 1));

            ScalarExpression transformedExpression = ElementAtEvaluator.validateAndTransform(
                elementAt,
                transformedMapInput.expression,
                transformedMapInput.outputType,
                transformedLookupKey.expression,
                transformedLookupKey.outputType);

            return new ExpressionTransformResult(
                transformedExpression,
                ((MapType) transformedMapInput.outputType).getValueType());
        }

        @Override
        ExpressionTransformResult visitNot(Predicate predicate) {
            Predicate child = validateIsPredicate(predicate, visit(predicate.getChildren().get(0)));
            return new ExpressionTransformResult(
                new Predicate(predicate.getName(), child),
                BooleanType.BOOLEAN);
        }

        @Override
        ExpressionTransformResult visitIsNotNull(Predicate predicate) {
            Expression child = visit(predicate.getChildren().get(0)).expression;
            return new ExpressionTransformResult(
                new Predicate(predicate.getName(), child),
                BooleanType.BOOLEAN
            );
        }

        @Override
        ExpressionTransformResult visitIsNull(Predicate predicate) {
            Expression child = visit(getUnaryChild(predicate)).expression;
            return new ExpressionTransformResult(
                new Predicate(predicate.getName(), child),
                BooleanType.BOOLEAN
            );
        }

        @Override
        ExpressionTransformResult visitCoalesce(ScalarExpression coalesce) {
            List<ExpressionTransformResult> children = coalesce.getChildren().stream()
                .map(this::visit)
                .collect(Collectors.toList());
            if (children.size() == 0) {
                throw DeltaErrors.unsupportedExpression(
                    coalesce, Optional.of("Coalesce requires at least one expression"));
            }
            // TODO support least-common-type resolution
            long numDistinctTypes = children.stream().map(e -> e.outputType)
                .distinct()
                .count();
            if (numDistinctTypes > 1) {
                throw DeltaErrors.unsupportedExpression(
                        coalesce,
                        Optional.of("Coalesce is only supported for arguments of the same type"));
            }
            // TODO support other data types besides boolean (just needs tests)
            if (!(children.get(0).outputType instanceof BooleanType)) {
                throw new UnsupportedOperationException(
                    "Coalesce is only supported for boolean type expressions");
            }
            return new ExpressionTransformResult(
                new ScalarExpression(
                    "COALESCE",
                    children.stream()
                        .map(e -> e.expression)
                        .collect(Collectors.toList())),
                children.get(0).outputType
            );
        }

        private Predicate validateIsPredicate(
            Expression baseExpression,
            ExpressionTransformResult result) {
            checkArgument(
                result.outputType instanceof BooleanType &&
                    result.expression instanceof Predicate,
                "%s: expected a predicate expression but got %s with output type %s.",
                baseExpression,
                result.expression,
                result.outputType);
            return (Predicate) result.expression;
        }

        private Expression transformBinaryComparator(Predicate predicate) {
            ExpressionTransformResult leftResult = visit(getLeft(predicate));
            ExpressionTransformResult rightResult = visit(getRight(predicate));
            Expression left = leftResult.expression;
            Expression right = rightResult.expression;
            if (!leftResult.outputType.equivalent(rightResult.outputType)) {
                if (canCastTo(leftResult.outputType, rightResult.outputType)) {
                    left = new Cast(left, rightResult.outputType);
                } else if (canCastTo(rightResult.outputType, leftResult.outputType)) {
                    right = new Cast(right, leftResult.outputType);
                } else {
                    String msg = format("operands are of different types which are not " +
                            "comparable: left type=%s, right type=%s",
                            leftResult.outputType, rightResult.outputType);
                    throw DeltaErrors.unsupportedExpression(predicate, Optional.of(msg));
                }
            }
            return new Predicate(predicate.getName(), left, right);
        }
    }

    /**
     * Implementation of {@link ExpressionVisitor} to evaluate expression on a
     * {@link ColumnarBatch}.
     */
    private static class ExpressionEvalVisitor extends ExpressionVisitor<ColumnVector> {
        private final ColumnarBatch input;

        ExpressionEvalVisitor(ColumnarBatch input) {
            this.input = input;
        }

        /*
        | Operand 1 | Operand 2 | `AND`      | `OR`       |
        |-----------|-----------|------------|------------|
        | True      | True      | True       | True       |
        | True      | False     | False      | True       |
        | True      | NULL      | NULL       | True       |
        | False     | True      | False      | True       |
        | False     | False     | False      | False      |
        | False     | NULL      | False      | NULL       |
        | NULL      | True      | NULL       | True       |
        | NULL      | False     | False      | NULL       |
        | NULL      | NULL      | NULL       | NULL       |
         */
        @Override
        ColumnVector visitAnd(And and) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(and);
            ColumnVector left = argResults.leftResult;
            ColumnVector right = argResults.rightResult;
            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = new boolean[numRows];
            for (int rowId = 0; rowId < numRows; rowId++) {
                boolean leftIsTrue = !left.isNullAt(rowId) && left.getBoolean(rowId);
                boolean rightIsTrue = !right.isNullAt(rowId) && right.getBoolean(rowId);
                boolean leftIsFalse = !left.isNullAt(rowId) && !left.getBoolean(rowId);
                boolean rightIsFalse = !right.isNullAt(rowId) && !right.getBoolean(rowId);

                if (leftIsFalse || rightIsFalse) {
                    nullability[rowId] = false;
                    result[rowId] = false;
                } else if (leftIsTrue && rightIsTrue) {
                    nullability[rowId] = false;
                    result[rowId] = true;
                } else {
                    nullability[rowId] = true;
                    // result[rowId] is undefined when nullability[rowId] = true
                }
            }
            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitOr(Or or) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(or);
            ColumnVector left = argResults.leftResult;
            ColumnVector right = argResults.rightResult;
            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = new boolean[numRows];
            for (int rowId = 0; rowId < numRows; rowId++) {
                boolean leftIsTrue = !left.isNullAt(rowId) && left.getBoolean(rowId);
                boolean rightIsTrue = !right.isNullAt(rowId) && right.getBoolean(rowId);
                boolean leftIsFalse = !left.isNullAt(rowId) && !left.getBoolean(rowId);
                boolean rightIsFalse = !right.isNullAt(rowId) && !right.getBoolean(rowId);

                if (leftIsTrue || rightIsTrue) {
                    nullability[rowId] = false;
                    result[rowId] = true;
                } else if (leftIsFalse && rightIsFalse) {
                    nullability[rowId] = false;
                    result[rowId] = false;
                } else {
                    nullability[rowId] = true;
                    // result[rowId] is undefined when nullability[rowId] = true
                }
            }
            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitAlwaysTrue(AlwaysTrue alwaysTrue) {
            return new DefaultConstantVector(BooleanType.BOOLEAN, input.getSize(), true);
        }

        @Override
        ColumnVector visitAlwaysFalse(AlwaysFalse alwaysFalse) {
            return new DefaultConstantVector(BooleanType.BOOLEAN, input.getSize(), false);
        }

        @Override
        ColumnVector visitComparator(Predicate predicate) {
            PredicateChildrenEvalResult argResults = evalBinaryExpressionChildren(predicate);

            int numRows = argResults.rowCount;
            boolean[] result = new boolean[numRows];
            boolean[] nullability = evalNullability(argResults.leftResult, argResults.rightResult);
            int[] compareResult = compare(argResults.leftResult, argResults.rightResult);
            switch (predicate.getName()) {
                case "=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] == 0;
                    }
                    break;
                case ">":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] > 0;
                    }
                    break;
                case ">=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] >= 0;
                    }
                    break;
                case "<":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] < 0;
                    }
                    break;
                case "<=":
                    for (int rowId = 0; rowId < numRows; rowId++) {
                        result[rowId] = compareResult[rowId] <= 0;
                    }
                    break;
                default:
                    throw DeltaErrors.unsupportedExpression(
                            predicate,
                            Optional.of("unsupported expression encountered"));
            }

            return new DefaultBooleanVector(numRows, Optional.of(nullability), result);
        }

        @Override
        ColumnVector visitLiteral(Literal literal) {
            DataType dataType = literal.getDataType();
            if (dataType instanceof BooleanType ||
                dataType instanceof ByteType ||
                dataType instanceof ShortType ||
                dataType instanceof IntegerType ||
                dataType instanceof LongType ||
                dataType instanceof FloatType ||
                dataType instanceof DoubleType ||
                dataType instanceof StringType ||
                dataType instanceof BinaryType ||
                dataType instanceof DecimalType ||
                dataType instanceof DateType ||
                dataType instanceof TimestampType ||
                dataType instanceof TimestampNTZType) {
                return new DefaultConstantVector(dataType, input.getSize(), literal.getValue());
            }

            throw new UnsupportedOperationException(
                "unsupported expression encountered: " + literal);
        }

        @Override
        ColumnVector visitColumn(Column column) {
            String[] names = column.getNames();
            DataType currentType = input.getSchema();
            ColumnVector columnVector = null;
            for (int level = 0; level < names.length; level++) {
                assertColumnExists(currentType instanceof StructType, input.getSchema(), column);
                StructType structSchema = ((StructType) currentType);
                int ordinal = structSchema.indexOf(names[level]);
                assertColumnExists(ordinal != -1, input.getSchema(), column);
                currentType = structSchema.at(ordinal).getDataType();

                if (level == 0) {
                    columnVector = input.getColumnVector(ordinal);
                } else {
                    columnVector = columnVector.getChild(ordinal);
                }
            }
            assertColumnExists(columnVector != null, input.getSchema(), column);
            return columnVector;
        }

        @Override
        ColumnVector visitCast(Cast cast) {
            ColumnVector inputResult = visit(cast.getInput());
            return new ImplicitCastExpression(cast.getInput(), cast.getOutputType())
                .eval(inputResult);
        }

        @Override
        ColumnVector visitPartitionValue(PartitionValueExpression partitionValue) {
            ColumnVector input = visit(partitionValue.getInput());
            return PartitionValueEvaluator.eval(input, partitionValue.getDataType());
        }

        @Override
        ColumnVector visitElementAt(ScalarExpression elementAt) {
            ColumnVector map = visit(childAt(elementAt, 0));
            ColumnVector lookupKey = visit(childAt(elementAt, 1));
            return ElementAtEvaluator.eval(map, lookupKey);
        }

        @Override
        ColumnVector visitNot(Predicate predicate) {
            ColumnVector childResult = visit(childAt(predicate, 0));
            return booleanWrapperVector(
                childResult,
                rowId -> !childResult.getBoolean(rowId),
                rowId -> childResult.isNullAt(rowId)
            );
        }

        @Override
        ColumnVector visitIsNotNull(Predicate predicate) {
            ColumnVector childResult = visit(childAt(predicate, 0));
            return booleanWrapperVector(
                childResult,
                rowId -> !childResult.isNullAt(rowId),
                rowId -> false
            );
        }

        @Override
        ColumnVector visitIsNull(Predicate predicate) {
            ColumnVector childResult = visit(getUnaryChild(predicate));
            return booleanWrapperVector(
                childResult,
                rowId -> childResult.isNullAt(rowId),
                rowId -> false
            );
        }

        @Override
        ColumnVector visitCoalesce(ScalarExpression coalesce) {
            List<ColumnVector> childResults = coalesce.getChildren()
                .stream()
                .map(this::visit)
                .collect(Collectors.toList());
            return DefaultExpressionUtils.combinationVector(
                childResults,
                rowId -> {
                    for (int idx = 0; idx < childResults.size(); idx++) {
                        if (!childResults.get(idx).isNullAt(rowId)) {
                            return idx;
                        }
                    }
                    return 0; // If all are null then any idx suffices
                }
            );
        }

        /**
         * Utility method to evaluate inputs to the binary input expression. Also validates the
         * evaluated expression result {@link ColumnVector}s are of the same size.
         *
         * @param predicate
         * @return Triplet of (result vector size, left operand result, left operand result)
         */
        private PredicateChildrenEvalResult evalBinaryExpressionChildren(Predicate predicate) {
            ColumnVector left = visit(getLeft(predicate));
            ColumnVector right = visit(getRight(predicate));
            checkArgument(
                left.getSize() == right.getSize(),
                "Left and right operand returned different results: left=%d, right=d",
                left.getSize(),
                right.getSize());
            return new PredicateChildrenEvalResult(left.getSize(), left, right);
        }
    }

    /**
     * Encapsulates children expression result of binary input predicate
     */
    private static class PredicateChildrenEvalResult {
        public final int rowCount;
        public final ColumnVector leftResult;
        public final ColumnVector rightResult;

        PredicateChildrenEvalResult(
            int rowCount, ColumnVector leftResult, ColumnVector rightResult) {
            this.rowCount = rowCount;
            this.leftResult = leftResult;
            this.rightResult = rightResult;
        }
    }

    private static void assertColumnExists(boolean condition, StructType schema, Column column) {
        if (!condition) {
            throw new IllegalArgumentException(
                format("%s doesn't exist in input data schema: %s", column, schema));
        }
    }
}
