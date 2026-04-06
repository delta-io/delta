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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import Function;
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
   * Semantic comparison of filter arrays, insensitive to Or/And nesting order within each element.
   */
  static boolean semanticFilterEquals(Filter[] a, Filter[] b) {
    if (a == b) return true;
    if (a == null || b == null) return false;
    if (a.length != b.length) return false;
    Set<String> setA = new HashSet<>();
    Set<String> setB = new HashSet<>();
    for (Filter f : a) setA.add(canonicalize(f));
    for (Filter f : b) setB.add(canonicalize(f));
    return setA.equals(setB);
  }

  /** Semantic hash for filter arrays, insensitive to Or/And nesting order. */
  static int semanticFilterHash(Filter[] arr) {
    if (arr == null) return 0;
    Set<String> canonSet = new HashSet<>();
    for (Filter f : arr) canonSet.add(canonicalize(f));
    return canonSet.hashCode();
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

  /** Semantic comparison of kernel predicate arrays, insensitive to Or/And nesting order. */
  static boolean semanticPredicateEquals(Predicate[] a, Predicate[] b) {
    if (a == b) return true;
    if (a == null || b == null) return false;
    if (a.length != b.length) return false;
    Set<String> setA = new HashSet<>();
    Set<String> setB = new HashSet<>();
    for (Predicate p : a) setA.add(canonicalize(p));
    for (Predicate p : b) setB.add(canonicalize(p));
    return setA.equals(setB);
  }

  /** Semantic hash for kernel predicate arrays, insensitive to Or/And nesting order. */
  static int semanticPredicateHash(Predicate[] arr) {
    if (arr == null) return 0;
    Set<String> canonSet = new HashSet<>();
    for (Predicate p : arr) canonSet.add(canonicalize(p));
    return canonSet.hashCode();
  }
}
