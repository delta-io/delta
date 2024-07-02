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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.internal.util.Utils;

import static io.delta.kernel.defaults.internal.DefaultEngineErrors.invalidEscapeSequence;
import static io.delta.kernel.defaults.internal.DefaultEngineErrors.unsupportedExpressionException;

/**
 * Utility methods to evaluate {@code like} expression.
 */
public class LikeExpressionEvaluator {
    private LikeExpressionEvaluator() {
    }

    static Predicate validateAndTransform(
            Predicate like,
            List<Expression> childrenExpressions,
            List<DataType> childrenOutputTypes) {
        int size = childrenExpressions.size();
        if (size < 2 || size > 3) {
            throw unsupportedExpressionException(like,
                    "Invalid number of inputs to LIKE expression. " +
                            "Example usage: LIKE(column, 'test%'), LIKE(column, 'test\\[%', '\\')");
        }

        Expression left = childrenExpressions.get(0);
        DataType leftOutputType = childrenOutputTypes.get(0);
        Expression right = childrenExpressions.get(1);
        DataType rightOutputType = childrenOutputTypes.get(1);
        Expression escapeCharExpr = size == 3 ? childrenExpressions.get(2) : null;
        DataType escapeCharOutputType = size == 3 ? childrenOutputTypes.get(2) : null;

        if (!(StringType.STRING.equivalent(leftOutputType)
                && StringType.STRING.equivalent(rightOutputType))) {
            throw unsupportedExpressionException(like,
                    "LIKE is only supported for string type expressions");
        }

        if (escapeCharExpr != null &&
                (!(escapeCharExpr instanceof Literal &&
                StringType.STRING.equivalent(escapeCharOutputType)))) {
            throw unsupportedExpressionException(like,
                    "LIKE expects escape token expression to be a literal of String type");
        }

        Literal literal = (Literal) escapeCharExpr;
        if (literal != null &&
                literal.getValue().toString().length() != 1) {
            throw unsupportedExpressionException(like,
                    "LIKE expects escape token to be a single character");
        }

        List<Expression> children = new ArrayList<>(Arrays.asList(left, right));
        if(Objects.nonNull(escapeCharExpr)) {
            children.add(escapeCharExpr);
        }
        return new Predicate(like.getName(), children);
    }

    static ColumnVector eval(
            List<Expression> childrenExpressions,
            List<ColumnVector> childrenVectors) {
        final char DEFAULT_ESCAPE_CHAR = '\\';
        final boolean isPatternLiteralType = childrenExpressions.get(1) instanceof Literal;

        return new ColumnVector() {
            final ColumnVector escapeCharVector =
                    childrenVectors.size() == 3 ?
                    childrenVectors.get(2) :
                    null;
            final ColumnVector left = childrenVectors.get(0);
            final ColumnVector right = childrenVectors.get(1);

            Character escapeChar = null;
            String regexCache = null;

            public void initEscapeCharIfRequired() {
                if (escapeChar == null) {
                    escapeChar =
                            escapeCharVector != null && !escapeCharVector.getString(0).isEmpty() ?
                                escapeCharVector.getString(0).charAt(0) :
                                DEFAULT_ESCAPE_CHAR;
                    }
            }

            @Override
            public DataType getDataType() {
                return BooleanType.BOOLEAN;
            }

            @Override
            public int getSize() {
                return left.getSize();
            }

            @Override
            public void close() {
                Utils.closeCloseables(left, right);
            }

            @Override
            public boolean getBoolean(int rowId) {
                initEscapeCharIfRequired();
                return isLike(left.getString(rowId), right.getString(rowId), escapeChar);
            }

            @Override
            public boolean isNullAt(int rowId) {
                return left.isNullAt(rowId) || right.isNullAt(rowId);
            }

            public boolean isLike(String input, String pattern, char escape) {
                if (!Objects.isNull(input) && !Objects.isNull(pattern)) {
                    String regex = getRegexFromCacheOrEval(pattern, escape);
                    return input.matches(regex);
                }
                return false;
            }

            public String getRegexFromCacheOrEval(String pattern, char escape) {
                String regex = (regexCache != null) ?
                        regexCache :
                        escapeLikeRegex(pattern, escape);
                if(isPatternLiteralType) { // set cache only for literals to avoid re-computation
                    regexCache = regex;
                }
                return regex;
            }
        };
    }

    /**
     * utility method to convert a predicate pattern to a java regex
     * @param pattern the pattern used in the expression
     * @param escape escape character to use
     * @return java regex
     */
    private static String escapeLikeRegex(String pattern, char escape) {
        final int len = pattern.length();
        final StringBuilder javaPattern = new StringBuilder(len + len);
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);

            if (c == escape) {
                if (i == (pattern.length() - 1)) {
                    throw invalidEscapeSequence(pattern, i);
                }
                char nextChar = pattern.charAt(i + 1);
                if ((nextChar == '_')
                        || (nextChar == '%')
                        || (nextChar == escape)) {
                    javaPattern.append(Pattern.quote(Character.toString(nextChar)));
                    i++;
                } else {
                    throw invalidEscapeSequence(pattern, i);
                }
            } else if (c == '_') {
                javaPattern.append('.');
            } else if (c == '%') {
                javaPattern.append(".*");
            } else {
                javaPattern.append(Pattern.quote(Character.toString(c)));
            }

        }
        return "(?s)" + javaPattern;
    }
}
