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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Expression;
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
}
