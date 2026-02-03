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

import java.util.List;
import java.util.Objects;

/**
 * An opaque predicate that wraps an engine-specific predicate operation.
 *
 * <p>This class is analogous to Rust Delta Kernel's {@code OpaquePredicate} struct. It allows
 * engines to define custom predicates that Kernel cannot interpret directly but can carry through
 * its APIs and delegate back to the engine for evaluation.
 *
 * <p><b>Design (from Rust Kernel):</b>
 *
 * <ul>
 *   <li>The {@code op} field holds the engine-specific operation
 *   <li>The {@code exprs} field (inherited from {@link Predicate}) holds child expressions
 *   <li>Cannot be serialized/deserialized (engine-specific)
 * </ul>
 *
 * <p><b>Rust Equivalent:</b>
 *
 * <pre>{@code
 * pub struct OpaquePredicate {
 *     pub op: OpaquePredicateOpRef,
 *     pub exprs: Vec<Expression>,
 * }
 * }</pre>
 *
 * @since 4.1.0
 * @see OpaquePredicateOp
 */
public class OpaquePredicate extends Predicate {
  private final OpaquePredicateOp op;

  /**
   * Creates a new OpaquePredicate with the given operation and child expressions.
   *
   * @param op the engine-specific predicate operation
   * @param exprs child expressions that this predicate operates on
   */
  public OpaquePredicate(OpaquePredicateOp op, List<Expression> exprs) {
    super(op.name(), exprs);
    this.op = Objects.requireNonNull(op, "OpaquePredicateOp cannot be null");
  }

  /**
   * Returns the engine-specific predicate operation.
   *
   * @return the opaque predicate operation
   */
  public OpaquePredicateOp getOp() {
    return op;
  }

  @Override
  public String toString() {
    return String.format("OPAQUE_PREDICATE(%s, %s)", op.name(), getChildren());
  }

  @Override
  public int hashCode() {
    // Use op's name and children for hashing
    return Objects.hash(op.name(), getChildren());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof OpaquePredicate)) return false;
    OpaquePredicate that = (OpaquePredicate) o;
    // Compare by name and children
    return op.name().equals(that.op.name()) && getChildren().equals(that.getChildren());
  }
}
