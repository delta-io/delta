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

package io.delta.kernel.internal.deletionvectors;

import java.io.IOException;
import java.util.Optional;

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.types.StructField;
import io.delta.kernel.utils.Tuple2;

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.data.SelectionColumnVector;

/**
 * Utility methods regarding deletion vectors.
 */
public class DeletionVectorUtils
{
    public static Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> loadNewDvAndBitmap(
        TableClient tableClient,
        String tablePath,
        DeletionVectorDescriptor dv)
    {
        DeletionVectorStoredBitmap storedBitmap =
            new DeletionVectorStoredBitmap(dv, Optional.of(tablePath));
        try {
            RoaringBitmapArray bitmap = storedBitmap
                .load(tableClient.getFileSystemClient());
            return new Tuple2<>(dv, bitmap);
        }
        catch (IOException e) {
            throw new RuntimeException("Couldn't load dv", e);
        }
    }
}
