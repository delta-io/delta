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
package io.delta.kernel.defaults.internal.expressions;

import java.util.Arrays;
import java.util.Optional;
import static java.lang.String.format;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.expressions.Cast;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;

import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.util.Utils;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import static io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo;

/**
 * Utility methods to evaluate {@code element_at} expression.
 */
class ElementAtEvaluator {
    private ElementAtEvaluator() {}

    /**
     * Validate and transform the {@code element_at} expression with given validated and
     * transformed inputs.
     */
    static ScalarExpression validateAndTransform(
        ScalarExpression elementAt,
        Expression mapInput,
        DataType mapInputType,
        Expression lookupKey,
        DataType lookupKeyType) {

        MapType asMapType = validateSupportedMapType(elementAt, mapInputType);
        DataType keyTypeFromMapInput = asMapType.getKeyType();

        if (!keyTypeFromMapInput.equivalent(lookupKeyType)) {
            if (canCastTo(lookupKeyType, keyTypeFromMapInput)) {
                lookupKey = new Cast(lookupKey, keyTypeFromMapInput);
            } else {
                String reason = format(
                        "lookup key type (%s) is different from the map key type (%s)",
                        lookupKeyType, asMapType.getKeyType());
                throw DeltaErrors.unsupportedExpression(elementAt, Optional.of(reason));
            }
        }
        return new ScalarExpression(elementAt.getName(), Arrays.asList(mapInput, lookupKey));
    }

    /**
     * Utility method to evaluate the {@code element_at} on given map and key vectors.
     * @param map {@link ColumnVector} of {@code map(string, string)} type.
     * @param lookupKey {@link ColumnVector} of {@code string} type.
     * @return result {@link ColumnVector} containing the lookup values.
     */
    static ColumnVector eval(ColumnVector map, ColumnVector lookupKey) {
        return new ColumnVector() {
            // Store the last lookup value to avoid multiple looks up for same row id.
            // The general pattern is call `isNullAt(rowId)` followed by `getString`.
            // So the cache of one value is enough.
            private int lastLookupRowId = -1;
            private String lastLookupValue = null;

            @Override
            public DataType getDataType() {
                return ((MapType) map.getDataType()).getValueType();
            }

            @Override
            public int getSize() {
                return map.getSize();
            }

            @Override
            public void close() {
                Utils.closeCloseables(map, lookupKey);
            }

            @Override
            public boolean isNullAt(int rowId) {
                if (rowId == lastLookupRowId) {
                    return lastLookupValue == null;
                }
                return map.isNullAt(rowId) || lookupValue(rowId) == null;
            }

            @Override
            public String getString(int rowId) {
                lookupValue(rowId);
                return lastLookupValue == null ? null : lastLookupValue;
            }

            private Object lookupValue(int rowId) {
                if (rowId == lastLookupRowId) {
                    return lastLookupValue;
                }
                lastLookupRowId = rowId;
                String keyValue = lookupKey.getString(rowId);
                lastLookupValue = findValueForKey(map.getMap(rowId), keyValue);
                return lastLookupValue;
            }

            /**
             * Given a {@link MapValue} and string {@code key} find the corresponding value.
             * Returns null if the key is not in the map.
             * @param mapValue String->String map to search
             * @param key the key to look up the value for; may be null
             */
            private String findValueForKey(MapValue mapValue, String key) {
                ColumnVector keyVector = mapValue.getKeys();
                for (int i = 0; i < mapValue.getSize(); i++) {
                    if ((keyVector.isNullAt(i) && key == null) ||
                            (!keyVector.isNullAt(i) && keyVector.getString(i).equals(key))) {
                        return mapValue.getValues().isNullAt(i) ? null :
                                mapValue.getValues().getString(i);
                    }
                }
                // If the key is not in the map return null
                return null;
            }
        };
    }

    private static MapType validateSupportedMapType(Expression elementAt, DataType mapInputType) {
        checkArgument(
            mapInputType instanceof MapType,
            "expected a map type input as first argument: " + elementAt);
        MapType asMapType = (MapType) mapInputType;
        // For now we only need to support lookup in columns of type `map(string -> string)`.
        // Additional type support may be added later
        if (asMapType.getKeyType().equivalent(StringType.STRING) &&
            asMapType.getValueType().equivalent(StringType.STRING)) {
            return asMapType;
        }
        throw new UnsupportedOperationException(
            format("%s: Supported only on type map(string, string) input data", elementAt));
    }
}
