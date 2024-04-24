/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import java.util.Arrays;

import io.delta.kernel.client.ExpressionHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.*;
import io.delta.kernel.types.*;

public class VariantUtils {
    public static ColumnarBatch withVariantColumns(
            ExpressionHandler expressionHandler,
            ColumnarBatch dataBatch) {
        for (int i = 0; i < dataBatch.getSchema().length(); i++) {
            StructField field = dataBatch.getSchema().at(i);
            if (!(field.getDataType() instanceof StructType) &&
                !(field.getDataType() instanceof ArrayType) &&
                !(field.getDataType() instanceof MapType) &&
                (field.getDataType() != VariantType.VARIANT ||
                dataBatch.getColumnVector(i).getDataType() == VariantType.VARIANT)) {
                continue;
            }

            ExpressionEvaluator evaluator = expressionHandler.getEvaluator(
                // Field here is variant type if its actually a variant.
                // TODO: probably better to pass in the schema as an argument
                // so the schema is enforced at the expression level. Need to pass in a literal
                // schema
                new StructType().add(field),
                new ScalarExpression(
                    "variant_coalesce",
                    Arrays.asList(new Column(field.getName()))
                ),
                VariantType.VARIANT
            );

            // TODO: don't need to pass in the entire batch.
            ColumnVector variantCol = evaluator.eval(dataBatch);
            // TODO: make a more efficient way to do this.
            dataBatch =
                dataBatch.withDeletedColumnAt(i).withNewColumn(i, field, variantCol);
        }
        return dataBatch;
    }

    private static ColumnVector[] getColumnBatchVectors(ColumnarBatch batch) {
        ColumnVector[] res = new ColumnVector[batch.getSchema().length()];
        for (int i = 0; i < batch.getSchema().length(); i++) {
            res[i] = batch.getColumnVector(i);
        }
        return res;
    }
}
