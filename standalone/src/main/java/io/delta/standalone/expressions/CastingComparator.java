package io.delta.standalone.expressions;

import java.util.Comparator;

public class CastingComparator<T extends Comparable<T>> {
    private final Comparator<T> comparator;

    public CastingComparator() {
        comparator = Comparator.naturalOrder();
    }

    int compare(Object a, Object b) {
        return comparator.compare((T) a, (T) b);
    }
}
