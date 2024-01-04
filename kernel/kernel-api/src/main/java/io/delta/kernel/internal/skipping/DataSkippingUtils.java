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
package io.delta.kernel.internal.skipping;

import java.util.*;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.types.*;
import static io.delta.kernel.internal.InternalScanFileUtils.SCAN_FILE_SCHEMA;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.lang.String.format;

public class DataSkippingUtils {

    private static final int ADD_FILE_ORDINAL = SCAN_FILE_SCHEMA.indexOf("add");
    private static final int ADD_FILE_STATS_ORDINAL = AddFile.SCHEMA_WITH_STATS.indexOf("stats");

    /**
     * Given the table schema return the expected statistics schema.
     */
    public static StructType getStatsSchema(StructType dataSchema) {
        return StatsSchemaHelper.getStatsSchema(dataSchema);
    }

    /**
     * Given a {@code FilteredColumnarBatch} of scan files and the statistics schema to parse,
     * return the parsed JSON stats from the scan files.
     */
    public static ColumnarBatch parseJsonStats(
        TableClient tableClient, FilteredColumnarBatch scanFileBatch, StructType statsSchema) {
        ColumnVector statsVector = scanFileBatch.getData()
            .getColumnVector(ADD_FILE_ORDINAL)
            .getChild(ADD_FILE_STATS_ORDINAL);
        return tableClient.getJsonHandler()
            .parseJson(statsVector, statsSchema, scanFileBatch.getSelectionVector());
    }

    /**
     * Returns a file skipping predicate expression, derived from the user query, which uses column
     * statistics to prune away files that provably contain no rows the query cares about.
     *
     * Specifically, the filter extraction code must obey the following rules:
     *
     * 1. Given a query predicate `e`, `constructDataFilters(e)` must return TRUE for a file unless
     *    we can prove `e` will not return TRUE for any row the file might contain. For example,
     *    given `a = 3` and min/max stat values [0, 100], this skipping predicate is safe:
     *
     *      AND(minValues.a <= 3, maxValues.a >= 3)
     *
     *    Because that condition must be true for any file that might possibly contain `a = 3`; the
     *    skipping predicate could return FALSE only if the max is too low, or the min too high; it
     *    could return NULL only if a is NULL in every row of the file. In both latter cases, it is
     *    safe to skip the file because `a = 3` can never evaluate to TRUE.
     *
     * 2. It is unsafe to apply skipping to operators that can evaluate to NULL or produce an error
     *    for non-NULL inputs. For example, consider this query predicate involving integer
     *    addition:
     *
     *      a + 1 = 3
     *
     *    It might be tempting to apply the standard equality skipping predicate:
     *
     *      AND(minValues.a + 1 <= 3, 3 <= maxValues.a + 1)
     *
     *    However, the skipping predicate would be unsound, because the addition operator could
     *    trigger integer overflow (e.g. minValues.a = 0 and maxValues.a = INT_MAX), even though the
     *    file could very well contain rows satisfying a + 1 = 3.
     *
     * 3. Predicates involving NOT are ineligible for skipping, because
     *    `Not(constructDataFilters(e))` is seldom equivalent to `constructDataFilters(Not(e))`.
     *    For example, consider the query predicate:
     *
     *      NOT(a = 1)
     *
     *    A simple inversion of the data skipping predicate would be:
     *
     *      NOT(AND(minValues.a <= 1, maxValues.a >= 1))
     *      ==> OR(NOT(minValues.a <= 1), NOT(maxValues.a >= 1))
     *      ==> OR(minValues.a > 1, maxValues.a < 1)
     *
     *    By contrast, if we first combine the NOT with = to obtain
     *
     *      a != 1
     *
     *    We get a different skipping predicate:
     *
     *      NOT(AND(minValues.a = 1, maxValues.a = 1))
     *      ==> OR(NOT(minValues.a = 1), NOT(maxValues.a = 1))
     *      ==>  OR(minValues.a != 1, maxValues.a != 1)
     *
     *    A truth table confirms that the first (naively inverted) skipping predicate is incorrect:
     *
     *      minValues.a
     *      | maxValues.a
     *      | | OR(minValues.a > 1, maxValues.a < 1)
     *      | | | OR(minValues.a != 1, maxValues.a != 1)
     *      0 0 T T
     *      0 1 F T    !! first predicate wrongly skipped a = 0
     *      1 1 F F
     *
     *    Fortunately, we may be able to eliminate NOT from some (branches of some) predicates:
     *
     *    a. It is safe to push the NOT into the children of AND and OR using de Morgan's Law, e.g.
     *
     *         NOT(AND(a, b)) ==> OR(NOT(a), NOT(B)).
     *
     *    b. It is safe to fold NOT into other operators, when a negated form of the operator
     *       exists:
     *
     *         NOT(NOT(x)) ==> x
     *         NOT(a == b) ==> a != b
     *         NOT(a > b) ==> a <= b
     *
     * TODO update these docs about missing stats
     * NOTE: The skipping predicate must handle the case where min and max stats for a column are
     * both NULL -- which indicates that all values in the file are NULL. Fortunately, most of the
     * operators we support data skipping for are NULL intolerant, and thus trivially satisfy this
     * requirement because they never return TRUE for NULL inputs. The only NULL tolerant operator
     * we support -- IS [NOT] NULL -- is specifically NULL aware.
     *
     * NOTE: The skipping predicate does *NOT* need to worry about missing stats columns (which also
     * manifest as NULL). That case is handled separately by `verifyStatsForFilter` (which disables
     * skipping for any file that lacks the needed stats columns).
     */
    public static Optional<Predicate> constructDataFilters(
        Predicate dataFilter, StructType dataSchema) {
        StatsSchemaHelper schemaHelper = new StatsSchemaHelper(dataSchema);
        return constructDataFilters(dataFilter, schemaHelper);
    }

    public static Optional<Predicate> constructDataFilters(
            Predicate dataFilter, StatsSchemaHelper schemaHelper) {

        switch (dataFilter.getName().toUpperCase(Locale.ROOT)) {

            // Push skipping predicate generation through the AND:
            //
            // constructDataFilters(AND(a, b))
            // ==> AND(constructDataFilters(a), constructDataFilters(b))
            //
            // To see why this transformation is safe, consider that `constructDataFilters(a)` must
            // evaluate to TRUE *UNLESS* we can prove that `a` would not evaluate to TRUE for any row the
            // file might contain. Thus, if the rewritten form of the skipping predicate does not evaluate
            // to TRUE, at least one of the skipping predicates must not have evaluated to TRUE, which in
            // turn means we were able to prove that `a` and/or `b` will not evaluate to TRUE for any row
            // of the file. If that is the case, then `AND(a, b)` also cannot evaluate to TRUE for any row
            // of the file, which proves we have a valid data skipping predicate.
            //
            // NOTE: AND is special -- we can safely skip the file if one leg does not evaluate to TRUE,
            // even if we cannot construct a skipping filter for the other leg.
            case "AND":
                Optional<Predicate> e1Filter = constructDataFilters(
                    asPredicate(getLeft(dataFilter)), schemaHelper);
                Optional<Predicate> e2Filter = constructDataFilters(
                    asPredicate(getRight(dataFilter)), schemaHelper);
                if (e1Filter.isPresent() && e2Filter.isPresent()) {
                    return Optional.of(new And(e1Filter.get(), e2Filter.get()));
                } else if (e1Filter.isPresent()) {
                    return e1Filter;
                } else {
                    return e2Filter; // possibly none
                }

            case "=": case "<": case "<=": case ">": case ">=":
                Expression left = getLeft(dataFilter);
                Expression right = getRight(dataFilter);

                if (left instanceof Column && right instanceof Literal) {
                    Column leftCol = (Column) left;
                    Literal rightLit = (Literal) right;
                    if (schemaHelper.isSkippingEligibleMinMaxColumn(leftCol) &&
                        schemaHelper.isSkippingEligibleLiteral(rightLit)) {
                        return Optional.of(constructComparatorDataFilters(
                            dataFilter.getName(), leftCol, rightLit, schemaHelper));
                    }
                } else if (right instanceof Column && left instanceof Literal) {
                    return constructDataFilters(reverseComparatorFilter(dataFilter), schemaHelper);
                }

            // TODO more expressions
            default:
                return Optional.empty();
        }
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Useful helper functions for dealing with expressions
    //////////////////////////////////////////////////////////////////////////////////

    /** Construct the skipping predicate for a given comparator */
    private static Predicate constructComparatorDataFilters(
            String comparator, Column leftCol, Literal rightLit, StatsSchemaHelper schemaHelper) {

        switch (comparator.toUpperCase(Locale.ROOT)) {

            // Match any file whose min/max range contains the requested point.
            case "=":
                return new And(
                    new Predicate("<=", schemaHelper.getMinColumn(leftCol), rightLit),
                    new Predicate("<=", rightLit, schemaHelper.getMaxColumn(leftCol))
                );

            // Match any file whose min is less than the requested upper bound.
            case "<":
                return new Predicate("<", schemaHelper.getMinColumn(leftCol), rightLit);

            // Match any file whose min is less than or equal to the requested upper bound
            case "<=":
                return new Predicate("<=", schemaHelper.getMinColumn(leftCol), rightLit);

            // Match any file whose max is larger than the requested lower bound.
            case ">":
                return new Predicate(">", schemaHelper.getMaxColumn(leftCol), rightLit);

            // Match any file whose max is larger than or equal to the requested lower bound.
            case ">=":
                return new Predicate(">=", schemaHelper.getMaxColumn(leftCol), rightLit);

            default:
                throw new IllegalArgumentException(
                    String.format("Unsupported comparator expression %s", comparator));
        }
    }

    private static Map<String, String> reverseComparators = new HashMap<String, String>(){{
        put("=", "=");
        put("<", ">");
        put("<=", ">=");
        put(">", "<");
        put(">=", "<=");
    }};

    private static Predicate reverseComparatorFilter(Predicate predicate) {
        return new Predicate(
            reverseComparators.get(predicate.getName().toUpperCase(Locale.ROOT)),
            getRight(predicate),
            getLeft(predicate)
        );
    }

    /**
     * Return an expression cast as a predicate, throw an error if it is not a predicate
     */
    private static Predicate asPredicate(Expression expression) {
        checkArgument(expression instanceof Predicate,
            String.format("Expected predicate but got %s", expression));
        return (Predicate) expression;
    }

    /**
     * Utility method to return the left child of the binary input expression
     */
    static Expression getLeft(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 2,
            format("%s: expected two inputs, but got %s", expression, children.size()));
        return children.get(0);
    }

    /**
     * Utility method to return the right child of the binary input expression
     */
    static Expression getRight(Expression expression) {
        List<Expression> children = expression.getChildren();
        checkArgument(
            children.size() == 2,
            format("%s: expected two inputs, but got %s", expression, children.size()));
        return children.get(1);
    }

}
