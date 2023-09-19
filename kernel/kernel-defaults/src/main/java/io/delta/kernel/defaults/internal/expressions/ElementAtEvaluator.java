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
import static java.lang.String.format;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.expressions.ScalarExpression;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.utils.Utils;

import static io.delta.kernel.defaults.internal.DefaultKernelUtils.checkArgument;
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
                lookupKey = new ImplicitCastExpression(lookupKey, keyTypeFromMapInput);
            } else {
                throw new UnsupportedOperationException(format(
                    "%s: lookup key type (%s) is different from the map key type (%s)",
                    elementAt, lookupKeyType, asMapType.getKeyType()));
            }
        }
        return new ScalarExpression(elementAt.getName(), Arrays.asList(mapInput, lookupKey));
    }

    /**
     * Utility method to evaluate the {@code element_at} on given map and key vectors.
     * @param map {@link ColumnVector} of {@code map(string, string)} type.
     * @param lookupKey {@link ColumnVector} of {@code string} type.
     * @return
     */
    static ColumnVector eval(ColumnVector map, ColumnVector lookupKey) {
        return new ColumnVector() {
            // Store the last lookup value to avoid multiple looks up for same row id.
            private int lastLookupRowId = -1;
            private Object lastLookupValue = null;

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
                return lastLookupValue == null ? null : (String) lastLookupValue;
            }

            private Object lookupValue(int rowId) {
                if (rowId == lastLookupRowId) {
                    return lastLookupValue;
                }
                // TODO: this needs to be updated after the new way of accessing the complex
                //  types is merged.
                lastLookupRowId = rowId;
                String keyValue = lookupKey.getString(rowId);
                lastLookupValue = map.getMap(rowId).get(keyValue);
                return lastLookupValue;
            }
        };
    }

    private static MapType validateSupportedMapType(Expression elementAt, DataType mapInputType) {
        checkArgument(
            mapInputType instanceof MapType,
            "expected a map type input as first argument: " + elementAt);
        MapType asMapType = (MapType) mapInputType;
        // TODO: we may extend type support in future, but for the need is just a look
        // in map(string, string).
        if (asMapType.getKeyType().equivalent(StringType.INSTANCE) &&
            asMapType.getValueType().equivalent(StringType.INSTANCE)) {
            return asMapType;
        }
        throw new UnsupportedOperationException(
            format("%s: Supported only on type map(string, string) input data", elementAt));
    }
}
