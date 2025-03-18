/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.clustering;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.types.StructType;
import java.util.List;

public class ClusteringUtils {
  public static DomainMetadata getClusteringDomainMetadata(
      List<Column> logicalClusteringColumns, StructType schema) {
    final ClusteringMetadataDomain clusteringMetadataDomain =
        new ClusteringMetadataDomain(logicalClusteringColumns, schema);
    return clusteringMetadataDomain.toDomainMetadata();
  }
}
