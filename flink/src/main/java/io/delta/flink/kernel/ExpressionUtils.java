/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.kernel;

import io.delta.kernel.expressions.*;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Helpers for building Kernel {@link Predicate} expressions that aren't natively expressible in
 * Kernel's expression API. Currently this is just the row-{@code IN} builder used to construct
 * partition / primary-key membership filters in the upsert merge path; expand as needed when other
 * compound expressions earn their own helper.
 */
public class ExpressionUtils {
  /**
   * Generate a predicate equivalent to <code>(colNames) IN (values)</code>. Empty {@code values} →
   * {@link AlwaysFalse#ALWAYS_FALSE}. Single-column → native {@link In} for engine pushdown.
   * Multi-column → left-deep OR-of-ANDs ({@code (c1=v11 AND ...) OR (c1=v21 AND ...) OR ...}),
   * since Kernel has no row-IN expression.
   *
   * @param colNames left-side tuple of column names; must be non-empty
   * @param values right-side list of value tuples; each row must have the same arity as {@code
   *     colNames}
   * @throws IllegalArgumentException if {@code colNames} is empty or any tuple arity mismatches
   */
  public static Predicate in(List<String> colNames, List<List<Literal>> values) {
    if (colNames.isEmpty()) {
      throw new IllegalArgumentException("in() requires at least one column");
    }
    if (values.isEmpty()) {
      return AlwaysFalse.ALWAYS_FALSE;
    }
    for (List<Literal> row : values) {
      if (row.size() != colNames.size()) {
        throw new IllegalArgumentException(
            "Value tuple arity " + row.size() + " does not match column count " + colNames.size());
      }
    }

    // Single-column fast path: use the native IN predicate (the engine can push it down better
    // than an OR chain).
    if (colNames.size() == 1) {
      Column col = new Column(colNames.get(0));
      List<Expression> literals =
          values.stream().map(row -> (Expression) row.get(0)).collect(Collectors.toList());
      return new In(col, literals);
    }

    // Multi-column: build (c1=v11 AND c2=v12 AND ...) OR (c1=v21 AND c2=v22 AND ...) OR ...
    // We accumulate left-deep AND/OR trees rather than building everything as a flat predicate,
    // because Kernel's And/Or only support binary children.
    Predicate result = null;
    for (List<Literal> row : values) {
      Predicate rowEq = null;
      for (int i = 0; i < colNames.size(); i++) {
        Predicate eq = new Predicate("=", new Column(colNames.get(i)), row.get(i));
        rowEq = (rowEq == null) ? eq : new And(rowEq, eq);
      }
      result = (result == null) ? rowEq : new Or(result, rowEq);
    }
    return result;
  }
}
