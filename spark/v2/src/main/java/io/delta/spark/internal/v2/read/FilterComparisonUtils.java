/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.Predicate;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.Or;

/**
 * Structural comparison utilities for V2 scan filter arrays.
 *
 * <p>Produces canonical string representations of filter trees that are insensitive to the nesting
 * order of Or and And nodes. Two filter trees with the same structure compare as equal regardless
 * of how their binary tree is shaped (e.g., left-deep vs right-deep).
 *
 * <p><b>Design note:</b> All array elements are canonicalized into a single global set rather than
 * compared per-element. This means {@code [Or(a,b), c]} and {@code [a, Or(b,c)]} would compare as
 * equal despite different array structure. If per-element structural fidelity is ever needed, switch
 * to per-element canonicalization with multiset comparison.
 */
final class FilterComparisonUtils {

  private FilterComparisonUtils() {}

  /**
   * Produce a canonical string for a filter tree. Or and And nodes are flattened and their children
   * sorted, so {@code Or(a, Or(b, c))} and {@code Or(Or(a, b), c)} both produce the same canonical
   * form.
   */
  static String canonicalize(Filter f) {
    if (f instanceof Or) {
      return flattenBinary(f, Or.class, "OR", o -> ((Or) o).left(), o -> ((Or) o).right());
    } else if (f instanceof And) {
      return flattenBinary(f, And.class, "AND", a -> ((And) a).left(), a -> ((And) a).right());
    } else {
      return f.toString();
    }
  }

  private static <T extends Filter> String flattenBinary(
      Filter f, Class<T> type, String name,
      Function<Filter, Filter> left,
      Function<Filter, Filter> right) {
    TreeSet<String> children = new TreeSet<>();
    collectChildren(f, type, left, right, children);
    return name + "(" + String.join(",", children) + ")";
  }

  private static <T extends Filter> void collectChildren(
      Filter f, Class<T> type,
      Function<Filter, Filter> left,
      Function<Filter, Filter> right,
      TreeSet<String> result) {
    if (type.isInstance(f)) {
      collectChildren(left.apply(f), type, left, right, result);
      collectChildren(right.apply(f), type, left, right, result);
    } else {
      result.add(canonicalize(f));
    }
  }

  /**
   * Compute the canonical string set for a filter array. Each filter is canonicalized
   * (Or/And trees flattened and sorted) and collected into an unmodifiable set.
   * Intended to be stored as a field for O(1) equals/hashCode via Set.equals/Set.hashCode.
   */
  static Set<String> canonicalFilterSet(Filter[] filters) {
    if (filters == null || filters.length == 0) return Collections.emptySet();
    Set<String> result = new HashSet<>();
    for (Filter f : filters) result.add(canonicalize(f));
    return Collections.unmodifiableSet(result);
  }

  // --- Kernel Predicate equivalents ---

  /**
   * Produce a canonical string for a kernel predicate tree. OR and AND nodes are flattened and
   * their children sorted.
   */
  static String canonicalize(Predicate p) {
    if (isBinaryLogical(p, "OR")) {
      TreeSet<String> children = new TreeSet<>();
      collectPredicateChildren(p, "OR", children);
      return "OR(" + String.join(",", children) + ")";
    } else if (isBinaryLogical(p, "AND")) {
      TreeSet<String> children = new TreeSet<>();
      collectPredicateChildren(p, "AND", children);
      return "AND(" + String.join(",", children) + ")";
    } else {
      return p.toString();
    }
  }

  private static boolean isBinaryLogical(Predicate p, String name) {
    if (!name.equals(p.getName())) return false;
    List<Expression> children = p.getChildren();
    return children.size() == 2
        && children.get(0) instanceof Predicate
        && children.get(1) instanceof Predicate;
  }

  private static void collectPredicateChildren(
      Predicate p, String name, TreeSet<String> result) {
    if (isBinaryLogical(p, name)) {
      collectPredicateChildren((Predicate) p.getChildren().get(0), name, result);
      collectPredicateChildren((Predicate) p.getChildren().get(1), name, result);
    } else {
      result.add(canonicalize(p));
    }
  }

  /**
   * Compute the canonical string set for a kernel predicate array.
   * Intended to be stored as a field for O(1) equals/hashCode via Set.equals/Set.hashCode.
   */
  static Set<String> canonicalPredicateSet(Predicate[] predicates) {
    if (predicates == null || predicates.length == 0) return Collections.emptySet();
    Set<String> result = new HashSet<>();
    for (Predicate p : predicates) result.add(canonicalize(p));
    return Collections.unmodifiableSet(result);
  }
}
