/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.expressions;

import org.apache.spark.SparkException;

public class HilbertStates {

    /**
     * Constructs a hilbert state for the given arity, [[n]].
     * This state list can be used to map n-points to their corresponding d-key value.
     *
     * @param n The number of bits in this space (we assert 2 <= n <= 9 for simplicity)
     * @return The CompactStateList for mapping from n-point to hilbert distance key.
     */
    private static HilbertCompactStateList constructHilbertState(int n) {
        HilbertIndex.GeneratorTable generator = HilbertIndex.getStateGenerator(n);
        return generator.generateStateList().getNPointToDKeyStateMap();
    }

    private HilbertStates() { }

    private static class HilbertIndex2 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(2);
    }

    private static class HilbertIndex3 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(3);
    }

    private static class HilbertIndex4 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(4);
    }

    private static class HilbertIndex5 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(5);
    }

    private static class HilbertIndex6 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(6);
    }

    private static class HilbertIndex7 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(7);
    }

    private static class HilbertIndex8 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(8);
    }

    private static class HilbertIndex9 {
        static final HilbertCompactStateList STATE_LIST = constructHilbertState(9);
    }

    public static HilbertCompactStateList getStateList(int n) throws SparkException {
        switch (n) {
            case 2:
                return HilbertIndex2.STATE_LIST;
            case 3:
                return HilbertIndex3.STATE_LIST;
            case 4:
                return HilbertIndex4.STATE_LIST;
            case 5:
                return HilbertIndex5.STATE_LIST;
            case 6:
                return HilbertIndex6.STATE_LIST;
            case 7:
                return HilbertIndex7.STATE_LIST;
            case 8:
                return HilbertIndex8.STATE_LIST;
            case 9:
                return HilbertIndex9.STATE_LIST;
            default:
                throw new SparkException(String.format("Cannot perform hilbert clustering on " +
                    "fewer than 2 or more than 9 dimensions; got %d dimensions", n));
        }
    }
}
