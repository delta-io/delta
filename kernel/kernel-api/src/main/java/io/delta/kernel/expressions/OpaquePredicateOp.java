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
package io.delta.kernel.expressions;

import io.delta.kernel.data.Row;
import io.delta.kernel.exceptions.KernelException;
import java.util.List;
import java.util.Optional;

/**
 * An opaque predicate operation that is defined and implemented by the engine (not Kernel).
 *
 * <p>This interface mirrors Rust Delta Kernel's {@code OpaquePredicateOp} trait and allows engines
 * to extend Delta Kernel's expression system with custom predicates. When Kernel encounters an
 * {@link OpaquePredicate} during evaluation, it calls back to the engine via this interface.
 *
 * <p><b>Design Philosophy (from Rust Delta Kernel):</b>
 *
 * <p>This interface supports three evaluation strategies:
 *
 * <ol>
 *   <li><b>Scalar Evaluation</b> ({@link #evalPredScalar}): For partition pruning and evaluating
 *       opaque data skipping predicates produced by indirect evaluation
 *   <li><b>Direct Data Skipping</b> ({@link #evalAsDataSkippingPredicate}): For Parquet row group
 *       skipping using footer stats
 *   <li><b>Indirect Data Skipping</b> ({@link #asDataSkippingPredicate}): For stats-based file
 *       pruning, converting the predicate to reference stats columns
 * </ol>
 *
 * <p><b>Usage Pattern:</b>
 *
 * <pre>{@code
 * // Engine creates an opaque predicate
 * OpaquePredicateOp sparkFilters = new SparkFilterOpaquePredicate(filters);
 * Predicate predicate = Predicate.opaque(sparkFilters, childExpressions);
 *
 * // Pass to Kernel
 * scanBuilder.withFilter(predicate);
 *
 * // Kernel calls back during evaluation with appropriate evaluator
 * // e.g., for partition pruning:
 * sparkFilters.evalPredScalar(evalExpr, evalPred, exprs, inverted)
 * }</pre>
 *
 * @since 4.1.0
 * @see <a href="https://github.com/delta-io/delta-kernel-rs">Rust Delta Kernel</a>
 */
public interface OpaquePredicateOp {
  /**
   * Returns a succinct identifier for this operation, used for debugging and logging.
   *
   * @return the name of this operation
   */
  String name();

  /**
   * Attempts scalar evaluation of this (possibly inverted) opaque predicate on behalf of a {@code
   * DirectPredicateEvaluator}, e.g. for partition pruning or to evaluate an opaque data skipping
   * predicate produced previously by an {@code IndirectDataSkippingPredicateEvaluator}.
   *
   * <p>Implementations can evaluate the child expressions however they see fit, possibly by calling
   * back to the provided evaluators.
   *
   * <p><b>Error Semantics:</b> A {@link KernelException} indicates that this operation does not
   * support scalar evaluation, or was invoked incorrectly (e.g., wrong number/types of arguments,
   * None input, etc.); the operation is disqualified from participating in partition pruning and/or
   * data skipping.
   *
   * <p><b>NULL Semantics:</b> {@code Optional.empty()} means the operation produced a legitimately
   * NULL output.
   *
   * @param evalExpr a Kernel-supplied scalar expression evaluator that can convert column
   *     references to scalar values
   * @param evalPred a Kernel-supplied predicate evaluator for evaluating sub-predicates
   * @param exprs child expressions to evaluate
   * @param inverted whether this predicate should be inverted (NOT applied)
   * @return {@code Optional.of(true/false)} if evaluation succeeded, or {@code Optional.empty()} if
   *     the result is NULL
   * @throws KernelException if this operation doesn't support scalar evaluation
   */
  Optional<Boolean> evalPredScalar(
      ScalarExpressionEvaluator evalExpr,
      DirectPredicateEvaluator evalPred,
      List<Expression> exprs,
      boolean inverted)
      throws KernelException;

  /**
   * Evaluates this (possibly inverted) opaque predicate for data skipping on behalf of a {@code
   * DirectDataSkippingPredicateEvaluator}, e.g., for Parquet row group skipping.
   *
   * <p>Implementations can evaluate the child expressions however they see fit, possibly by calling
   * back to the provided evaluator.
   *
   * <p><b>Semantics:</b> {@code Optional.empty()} indicates that this operation does not support
   * evaluation as a data skipping predicate, or was invoked incorrectly (e.g., wrong number/types
   * of arguments, None input, etc.); the operation is disqualified from participating in row group
   * skipping.
   *
   * @param evaluator a Kernel-supplied data skipping predicate evaluator
   * @param exprs child expressions to evaluate
   * @param inverted whether this predicate should be inverted (NOT applied)
   * @return {@code Optional.of(true/false)} if evaluation succeeded, or {@code Optional.empty()} if
   *     the operation doesn't support direct data skipping
   */
  Optional<Boolean> evalAsDataSkippingPredicate(
      DirectDataSkippingPredicateEvaluator evaluator, List<Expression> exprs, boolean inverted);

  /**
   * Converts this (possibly inverted) opaque predicate to a data skipping predicate on behalf of an
   * {@code IndirectDataSkippingPredicateEvaluator}, e.g., for stats-based file pruning.
   *
   * <p>Implementations can transform the predicate and its child expressions however they see fit,
   * possibly by calling back to the owning evaluator. The resulting predicate should reference
   * stats columns (e.g., {@code stats.minValues.columnName}) instead of data columns.
   *
   * <p><b>Semantics:</b> {@code Optional.empty()} indicates that this operation does not support
   * conversion to a data skipping predicate, or was invoked incorrectly (e.g., wrong number/types
   * of arguments, None input, etc.); the operation is disqualified from participating in file
   * pruning.
   *
   * @param evaluator a Kernel-supplied indirect data skipping predicate evaluator
   * @param exprs child expressions to transform
   * @param inverted whether this predicate should be inverted (NOT applied)
   * @return {@code Optional} containing the transformed predicate if conversion succeeded, or
   *     {@code Optional.empty()} if the operation doesn't support indirect data skipping
   */
  Optional<Predicate> asDataSkippingPredicate(
      IndirectDataSkippingPredicateEvaluator evaluator, List<Expression> exprs, boolean inverted);

  /**
   * <b>EXTENSION METHOD (not in Rust Kernel):</b> Evaluates this predicate on an AddFile row during
   * scan file enumeration.
   *
   * <p>This method is a Java-specific extension that provides a simpler interface for engines to
   * implement file-level filtering during distributed log replay. It's called by {@code
   * DistributedScan.getScanFiles()} for each AddFile.
   *
   * <p>The engine can use any logic to evaluate the predicate, including:
   *
   * <ul>
   *   <li>Partition value filtering (checking {@code add.partitionValues})
   *   <li>Stats-based data skipping (checking {@code add.stats.minValues/maxValues/nullCount})
   *   <li>Custom file-level predicates
   * </ul>
   *
   * <p><b>AddFile Schema:</b> The input row conforms to Delta's AddFile schema with fields like:
   *
   * <ul>
   *   <li>{@code path}: String - file path
   *   <li>{@code partitionValues}: Map&lt;String, String&gt; - partition column values
   *   <li>{@code size}: Long - file size in bytes
   *   <li>{@code modificationTime}: Long - file modification time
   *   <li>{@code stats}: Struct - parsed statistics with minValues, maxValues, nullCount
   * </ul>
   *
   * @param addFileRow a row representing an AddFile action from the Delta log
   * @return {@code Optional.of(true)} to include the file, {@code Optional.of(false)} to exclude
   *     it, or {@code Optional.empty()} if evaluation is not possible (file will be included by
   *     default)
   */
  default Optional<Boolean> evaluateOnAddFile(Row addFileRow) {
    // Default: include all files (no filtering)
    return Optional.of(true);
  }
}
