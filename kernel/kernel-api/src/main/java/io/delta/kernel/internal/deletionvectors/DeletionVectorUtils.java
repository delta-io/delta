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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeature;
import io.delta.kernel.internal.util.Tuple2;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/** Utility methods regarding deletion vectors. */
public class DeletionVectorUtils {
  public static Tuple2<DeletionVectorDescriptor, RoaringBitmapArray> loadNewDvAndBitmap(
      Engine engine, String tablePath, DeletionVectorDescriptor dv) {
    DeletionVectorStoredBitmap storedBitmap =
        new DeletionVectorStoredBitmap(dv, Optional.of(tablePath));
    try {
      RoaringBitmapArray bitmap = storedBitmap.load(engine.getFileSystemClient());
      return new Tuple2<>(dv, bitmap);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't load dv", e);
    }
  }

  /**
   * Check if deletion vectors are readable for the given protocol and metadata.
   *
   * @param protocol the protocol to check
   * @param metadata the metadata to check
   * @return true if deletion vectors are readable
   */
  public static boolean isReadable(Protocol protocol, Metadata metadata) {
    Set<TableFeature> features = protocol.getImplicitlyAndExplicitlySupportedReaderWriterFeatures();
    boolean featureSupported =
        features.stream().anyMatch(f -> f.featureName().equals("deletionVectors"));
    // Deletion vectors are only supported on parquet tables
    boolean isParquetFormat = "parquet".equalsIgnoreCase(metadata.getFormat().getProvider());
    return featureSupported && isParquetFormat;
  }
}
