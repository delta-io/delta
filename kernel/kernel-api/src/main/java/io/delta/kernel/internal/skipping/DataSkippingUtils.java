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
import io.delta.kernel.types.*;
import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_ORDINAL;
import static io.delta.kernel.internal.InternalScanFileUtils.ADD_FILE_STATS_ORDINAL;
import static io.delta.kernel.internal.util.ExpressionUtils.*;

public class DataSkippingUtils {

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
     * Prunes the given schema to only include the referenced leaf columns. If a leaf column is a
     * nested column it must be referenced using the full column path, e.g. "C_0.C_1.C_leaf"
     * @param schema the schema to prune
     * @param referencedLeafCols set of leaf columns in {@code schema}
     */
    public static StructType pruneStatsSchema(StructType schema, Set<Column> referencedLeafCols) {
        return pruneSchema(referencedLeafCols, schema, new String[0]);
    }

    /**
     * Constructs a data skipping filter to prune files using column statistics given
     * a query data filter if possible. The returned filter will evaluate to FALSE for any files
     * that can be safely skipped. If the filter evaluates to NULL or TRUE, the file should not
     * be skipped.
     *
     * @param dataFilters query filters on the data columns
     * @param dataSchema the data schema of the table
     * @return data skipping filter to prune files if it exists as a {@link DataSkippingPredicate}
     */
    public static Optional<DataSkippingPredicate> constructDataSkippingFilter(
            Predicate dataFilters, StructType dataSchema) {
        StatsSchemaHelper schemaHelper = new StatsSchemaHelper(dataSchema);
        return constructDataSkippingFilter(dataFilters, schemaHelper);
    }

    //////////////////////////////////////////////////////////////////////////////////
    // Helper functions
    //////////////////////////////////////////////////////////////////////////////////

    /**
     * Returns a file skipping predicate expression, derived from the user query, which uses column
     * statistics to prune away files that provably contain no rows the query cares about.
     * <p>
     * Specifically, the filter extraction code must obey the following rules:
     * <p>
     * 1. Given a query predicate `e`, `constructDataSkippingFilter(e)` must return TRUE for a
     * file unless
     * we can prove `e` will not return TRUE for any row the file might contain. For example,
     * given `a = 3` and min/max stat values [0, 100], this skipping predicate is safe:
     * <p>
     * AND(minValues.a <= 3, maxValues.a >= 3)
     * <p>
     * Because that condition must be true for any file that might possibly contain `a = 3`; the
     * skipping predicate could return FALSE only if the max is too low, or the min too high; it
     * could return NULL only if a is NULL in every row of the file. In both latter cases, it is
     * safe to skip the file because `a = 3` can never evaluate to TRUE.
     * <p>
     * 2. It is unsafe to apply skipping to operators that can evaluate to NULL or produce an error
     * for non-NULL inputs. For example, consider this query predicate involving integer
     * addition:
     * <p>
     * a + 1 = 3
     * <p>
     * It might be tempting to apply the standard equality skipping predicate:
     * <p>
     * AND(minValues.a + 1 <= 3, 3 <= maxValues.a + 1)
     * <p>
     * However, the skipping predicate would be unsound, because the addition operator could
     * trigger integer overflow (e.g. minValues.a = 0 and maxValues.a = INT_MAX), even though the
     * file could very well contain rows satisfying a + 1 = 3.
     * <p>
     * 3. Predicates involving NOT are ineligible for skipping, because
     * `Not(constructDataSkippingFilter(e))` is seldom equivalent to `constructDataSkippingFilter
     * (Not(e))`.
     * For example, consider the query predicate:
     * <p>
     * NOT(a = 1)
     * <p>
     * A simple inversion of the data skipping predicate would be:
     * <p>
     * NOT(AND(minValues.a <= 1, maxValues.a >= 1))
     * ==> OR(NOT(minValues.a <= 1), NOT(maxValues.a >= 1))
     * ==> OR(minValues.a > 1, maxValues.a < 1)
     * <p>
     * By contrast, if we first combine the NOT with = to obtain
     * <p>
     * a != 1
     * <p>
     * We get a different skipping predicate:
     * <p>
     * NOT(AND(minValues.a = 1, maxValues.a = 1))
     * ==> OR(NOT(minValues.a = 1), NOT(maxValues.a = 1))
     * ==>  OR(minValues.a != 1, maxValues.a != 1)
     * <p>
     * A truth table confirms that the first (naively inverted) skipping predicate is incorrect:
     * <p>
     * minValues.a
     * | maxValues.a
     * | | OR(minValues.a > 1, maxValues.a < 1)
     * | | | OR(minValues.a != 1, maxValues.a != 1)
     * 0 0 T T
     * 0 1 F T    !! first predicate wrongly skipped a = 0
     * 1 1 F F
     * <p>
     * Fortunately, we may be able to eliminate NOT from some (branches of some) predicates:
     * <p>
     * a. It is safe to push the NOT into the children of AND and OR using de Morgan's Law, e.g.
     * <p>
     * NOT(AND(a, b)) ==> OR(NOT(a), NOT(B)).
     * <p>
     * b. It is safe to fold NOT into other operators, when a negated form of the operator
     * exists:
     * <p>
     * NOT(NOT(x)) ==> x
     * NOT(a == b) ==> a != b
     * NOT(a > b) ==> a <= b
     * <p>
     * NOTE: The skipping predicate must handle the case where min and max stats for a column are
     * both NULL -- which indicates that all values in the file are NULL. Fortunately, most of the
     * operators we support data skipping for are NULL intolerant, and thus trivially satisfy this
     * requirement because they never return TRUE for NULL inputs. The only NULL tolerant operator
     * we support -- IS [NOT] NULL -- is specifically NULL aware. The predicate evaluates to NULL
     * if any required statistics are missing.
     */
    private static Optional<DataSkippingPredicate> constructDataSkippingFilter(
            Predicate dataFilters, StatsSchemaHelper schemaHelper) {

        switch (dataFilters.getName().toUpperCase(Locale.ROOT)) {

            // Push skipping predicate generation through the AND:
            //
            // constructDataSkippingFilter(AND(a, b))
            // ==> AND(constructDataSkippingFilter(a), constructDataSkippingFilter(b))
            //
            // To see why this transformation is safe, consider that
            // `constructDataSkippingFilter(a)` must evaluate to TRUE *UNLESS* we can prove that
            // `a` would not evaluate to TRUE for any
            // row the file might contain. Thus, if the rewritten form of the skipping predicate
            // does not evaluate to TRUE, at least one of the skipping predicates must not have
            // evaluated to TRUE, which in turn means we were able to prove that `a` and/or `b`
            // will not evaluate to TRUE for any row of the file. If that is the case, then
            // `AND(a, b)` also cannot evaluate to TRUE for any row of the file, which proves we
            // have a valid data skipping predicate.
            //
            // NOTE: AND is special -- we can safely skip the file if one leg does not evaluate to
            // TRUE, even if we cannot construct a skipping filter for the other leg.
            case "AND":
                Optional<DataSkippingPredicate> e1AndFilter = constructDataSkippingFilter(
                    asPredicate(getLeft(dataFilters)), schemaHelper);
                Optional<DataSkippingPredicate> e2AndFilter = constructDataSkippingFilter(
                    asPredicate(getRight(dataFilters)), schemaHelper);
                if (e1AndFilter.isPresent() && e2AndFilter.isPresent()) {
                    return Optional.of(and(e1AndFilter.get(), e2AndFilter.get()));
                } else if (e1AndFilter.isPresent()) {
                    return e1AndFilter;
                } else {
                    return e2AndFilter; // possibly none
                }

            // Push skipping predicate generation through OR (similar to AND case).
            //
            // constructDataFilters(OR(a, b))
            // ==> OR(constructDataFilters(a), constructDataFilters(b))
            //
            // Similar to AND case, if the rewritten predicate does not evaluate to TRUE, then it
            // means that neither `constructDataFilters(a)` nor `constructDataFilters(b)` evaluated
            // to TRUE, which in turn means that neither `a` nor `b` could evaluate to TRUE for any
            // row the file might contain, which proves we have a valid data skipping predicate.
            //
            // Unlike AND, a single leg of an OR expression provides no filtering power -- we can
            // only reject a file if both legs evaluate to false.
            case "OR":
                Optional<DataSkippingPredicate> e1OrFilter = constructDataSkippingFilter(
                    asPredicate(getLeft(dataFilters)), schemaHelper);
                Optional<DataSkippingPredicate> e2OrFilter = constructDataSkippingFilter(
                    asPredicate(getRight(dataFilters)), schemaHelper);
                if (e1OrFilter.isPresent() && e2OrFilter.isPresent()) {
                    return Optional.of(or(e1OrFilter.get(), e2OrFilter.get()));
                } else {
                    return Optional.empty();
                }

            case "=": case "<": case "<=": case ">": case ">=":
                Expression left = getLeft(dataFilters);
                Expression right = getRight(dataFilters);

                if (left instanceof Column && right instanceof Literal) {
                    Column leftCol = (Column) left;
                    Literal rightLit = (Literal) right;
                    if (schemaHelper.isSkippingEligibleMinMaxColumn(leftCol) &&
                        schemaHelper.isSkippingEligibleLiteral(rightLit)) {
                        return Optional.of(constructComparatorDataSkippingFilters(
                            dataFilters.getName(), leftCol, rightLit, schemaHelper));
                    }
                } else if (right instanceof Column && left instanceof Literal) {
                    return constructDataSkippingFilter(
                        reverseComparatorFilter(dataFilters), schemaHelper);
                }
                break;

            case "NOT":
                return constructNotDataSkippingFilters(
                    asPredicate(getUnaryChild(dataFilters)), schemaHelper);

            // TODO more expressions
        }
        return Optional.empty();
    }

    /** Construct the skipping predicate for a given comparator */
    private static DataSkippingPredicate constructComparatorDataSkippingFilters(
            String comparator, Column leftCol, Literal rightLit, StatsSchemaHelper schemaHelper) {

        switch (comparator.toUpperCase(Locale.ROOT)) {

            // Match any file whose min/max range contains the requested point.
            case "=":
                // For example a = 1 --> minValue.a <= 1 AND maxValue.a >= 1
                return and(
                    constructBinaryDataSkippingPredicate(
                        "<=", schemaHelper.getMinColumn(leftCol), rightLit),
                    constructBinaryDataSkippingPredicate(
                        ">=", schemaHelper.getMaxColumn(leftCol), rightLit)
                );

            // Match any file whose min is less than the requested upper bound.
            case "<":
                return constructBinaryDataSkippingPredicate(
                    "<", schemaHelper.getMinColumn(leftCol), rightLit);

            // Match any file whose min is less than or equal to the requested upper bound
            case "<=":
                return constructBinaryDataSkippingPredicate(
                    "<=", schemaHelper.getMinColumn(leftCol), rightLit);

            // Match any file whose max is larger than the requested lower bound.
            case ">":
                return constructBinaryDataSkippingPredicate(
                    ">", schemaHelper.getMaxColumn(leftCol), rightLit);

            // Match any file whose max is larger than or equal to the requested lower bound.
            case ">=":
                return constructBinaryDataSkippingPredicate(
                    ">=", schemaHelper.getMaxColumn(leftCol), rightLit);

            default:
                throw new IllegalArgumentException(
                    String.format("Unsupported comparator expression %s", comparator));
        }
    }

    /**
     * Constructs a {@link DataSkippingPredicate} for a binary predicate expression with a left
     * expression of type {@link Column} and a right expression of type {@link Literal}.
     */
    private static DataSkippingPredicate constructBinaryDataSkippingPredicate(
            String exprName,
            Column col,
            Literal lit) {
        return new DataSkippingPredicate(
            new Predicate(exprName,  col, lit),
            new HashSet<Column>(){
                {
                    add(col);
                }
            }
        );
    }

    /**
     * Given two {@link DataSkippingPredicate}s constructs the {@link DataSkippingPredicate}
     * representing the AND expression of {@code left} and {@code right}.
     */
    private static DataSkippingPredicate and(
            DataSkippingPredicate left, DataSkippingPredicate right) {
        return new DataSkippingPredicate(
            new And(
                left.getPredicate(),
                right.getPredicate()
            ),
            new HashSet<Column>() {
                {
                    addAll(left.getReferencedCols());
                    addAll(right.getReferencedCols());
                }
            }
        );
    }

    /**
     * Given two {@link DataSkippingPredicate}s constructs the {@link DataSkippingPredicate}
     * representing the OR expression of {@code left} and {@code right}.
     */
    private static DataSkippingPredicate or(
        DataSkippingPredicate left, DataSkippingPredicate right) {
        return new DataSkippingPredicate(
            new Or(
                left.getPredicate(),
                right.getPredicate()
            ),
            new HashSet<Column>() {
                {
                    addAll(left.getReferencedCols());
                    addAll(right.getReferencedCols());
                }
            }
        );
    }

    private static final Map<String, String> REVERSE_COMPARATORS = new HashMap<String, String>(){
        {
            put("=", "=");
            put("<", ">");
            put("<=", ">=");
            put(">", "<");
            put(">=", "<=");
        }
    };

    private static Predicate reverseComparatorFilter(Predicate predicate) {
        return new Predicate(
            REVERSE_COMPARATORS.get(predicate.getName().toUpperCase(Locale.ROOT)),
            getRight(predicate),
            getLeft(predicate)
        );
    }

    /** Construct the skipping predicate for a NOT expression child if possible */
    private static Optional<DataSkippingPredicate> constructNotDataSkippingFilters(
            Predicate childPredicate, StatsSchemaHelper schemaHelper) {
        switch (childPredicate.getName().toUpperCase(Locale.ROOT)) {
            // Use deMorgan's law to push the NOT past the AND. This is safe even with SQL
            // tri-valued logic (see below), and is desirable because we cannot generally push
            // predicate filters
            // through NOT, but we *CAN* push predicate filters through AND and OR:
            //
            // constructDataFilters(NOT(AND(a, b)))
            // ==> constructDataFilters(OR(NOT(a), NOT(b)))
            // ==> OR(constructDataFilters(NOT(a)), constructDataFilters(NOT(b)))
            //
            // Assuming we can push the resulting NOT operations all the way down to some leaf
            // operation it can fold into, the rewrite allows us to create a data skipping filter
            // from the expression.
            //
            // a b AND(a, b)
            // | | | NOT(AND(a, b))
            // | | | | OR(NOT(a), NOT(b))
            // T T T F F
            // T F F T T
            // T N N N N
            // F F F T T
            // F N F T T
            // N N N N N
            case "AND":
                return constructDataSkippingFilter(
                    new Or(
                        new Predicate("NOT", asPredicate(getLeft(childPredicate))),
                        new Predicate("NOT", asPredicate(getRight(childPredicate)))
                    ),
                    schemaHelper
                );

            // Similar to AND, we can (and want to) push the NOT past the OR using deMorgan's law.
            case "OR":
                return constructDataSkippingFilter(
                    new And(
                        new Predicate("NOT", asPredicate(getLeft(childPredicate))),
                        new Predicate("NOT", asPredicate(getRight(childPredicate)))
                    ),
                    schemaHelper
                );

            case "=":
                Expression left = getLeft(childPredicate);
                Expression right = getRight(childPredicate);
                if (left instanceof Column && right instanceof Literal) {
                    Column leftCol = (Column) left;
                    Literal rightLit = (Literal) right;
                    if (schemaHelper.isSkippingEligibleMinMaxColumn(leftCol) &&
                        schemaHelper.isSkippingEligibleLiteral(rightLit)) {
                        // Match any file whose min/max range contains anything other than the
                        // rejected point.
                        // For example a != 1 --> minValue.a < 1 OR maxValue.a > 1
                        return Optional.of(
                            or(
                                constructBinaryDataSkippingPredicate(
                                    "<", schemaHelper.getMinColumn(leftCol), rightLit),
                                constructBinaryDataSkippingPredicate(
                                    ">", schemaHelper.getMaxColumn(leftCol), rightLit)
                            )
                        );
                    }
                } else if (right instanceof Column && left instanceof Literal) {
                    return constructDataSkippingFilter(
                        new Predicate("NOT", new Predicate("=", right, left)),
                        schemaHelper);
                }
                break;
            case "<":
                return constructDataSkippingFilter(
                    new Predicate(">=", childPredicate.getChildren()),
                    schemaHelper);
            case "<=":
                return constructDataSkippingFilter(
                    new Predicate(">", childPredicate.getChildren()),
                    schemaHelper);
            case ">":
                return constructDataSkippingFilter(
                    new Predicate("<=", childPredicate.getChildren()),
                    schemaHelper);
            case ">=":
                return constructDataSkippingFilter(
                    new Predicate("<", childPredicate.getChildren()),
                    schemaHelper);
            case "NOT":
                // Remove redundant pairs of NOT
                return constructDataSkippingFilter(
                    asPredicate(getUnaryChild(childPredicate)),
                    schemaHelper);
        }
        return Optional.empty();
    }

    /**
     * Prunes the given schema to include only the referenced leaf columns to keep. These leaf
     * columns (possible nested) are relative to the root schema, not to the current level of
     * recursion. {@code leafColumnsToKeep} is unchanged at any level of recursion.
     * <p>
     * For example consider the following schema:
     * <pre>
     * |--level1_struct: struct
     * |   |--level2_struct: struct
     * |       |--level3_struct: struct
     * |           |--level_4_col: int
     * |       |--level_3_col: int
     * </pre>
     * At the second level of recursion on field {@code level2_struct} we would have parameters
     * <ol>
     *     <li>leafColumnsToKeep=Set(Column(level1_struct.level2_struct.level_3_col))</li>
     *     <li>schema:
     *          <pre>
     *          |--level3_struct: struct
     *          |   |--level_4_col: int
     *          |--level_3_col: int
     *          </pre>
     *     </li>
     *     <li>parentPath=["level1_struct", "level2_struct"]</li>
     * </ol>
     *
     * @param leafColumnsToKeep set of leaf columns relative to the schema root
     * @param schema schema to prune
     * @param parentPath parent path of the fields in {@code schema} relative to the schema root
     */
    private static StructType pruneSchema(
        Set<Column> leafColumnsToKeep, StructType schema, String[] parentPath) {
        List<StructField> prunedFields = new ArrayList<>();
        for (StructField field : schema.fields()) {
            String[] colPath = appendArray(parentPath, field.getName());
            if (field.getDataType() instanceof StructType) {
                StructType prunedNestedSchema = pruneSchema(
                    leafColumnsToKeep,
                    (StructType) field.getDataType(),
                    colPath
                );
                if (prunedNestedSchema.length() > 0) {
                    // Only add a struct field it has un-pruned nested columns
                    prunedFields.add(
                        new StructField(
                            field.getName(),
                            prunedNestedSchema,
                            field.isNullable(),
                            field.getMetadata())
                    );
                }
            } else {
                if (leafColumnsToKeep.contains(new Column(colPath))) {
                    prunedFields.add(field);
                }
            }
        }
        return new StructType(prunedFields);
    }

    /**
     * Given an array {@code arr} and a string element {@code appendElem} return a new array with
     * {@code appendElem} inserted at the end
     */
    private static String[] appendArray(String[] arr, String appendElem){
        String[] newNames = new String[arr.length + 1];
        System.arraycopy(arr, 0, newNames, 0, arr.length);
        newNames[arr.length] = appendElem;
        return newNames;
    }
}
