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
package io.delta.spark.internal.v2.read.deletionvector;

import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import java.util.Optional;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.delta.RowIndexFilterType;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

/**
 * Utility class for deletion vector support in the V2 connector.
 *
 * <p>Provides methods for:
 *
 * <ul>
 *   <li>Checking if a table supports deletion vectors
 *   <li>Converting Kernel DV descriptors to Spark format
 *   <li>Building DV metadata for PartitionedFile
 *   <li>Schema augmentation with DV columns
 * </ul>
 */
public final class DeletionVectorUtils {

  private DeletionVectorUtils() {}

  /** Check if table supports reading deletion vectors. */
  public static boolean isReadable(Protocol protocol, Metadata metadata) {
    return protocol.supportsFeature(TableFeatures.DELETION_VECTORS_RW_FEATURE)
        && "parquet".equalsIgnoreCase(metadata.getFormat().getProvider());
  }

  /** Convert Kernel's DeletionVectorDescriptor to Spark's DeletionVectorDescriptor. */
  public static org.apache.spark.sql.delta.actions.DeletionVectorDescriptor convertToSpark(
      DeletionVectorDescriptor kernelDv) {
    return new org.apache.spark.sql.delta.actions.DeletionVectorDescriptor(
        kernelDv.getStorageType(),
        kernelDv.getPathOrInlineDv(),
        Option.apply(kernelDv.getOffset().orElse(null)),
        kernelDv.getSizeInBytes(),
        kernelDv.getCardinality(),
        Option.empty());
  }

  /**
   * Build metadata map containing DV descriptor if present.
   *
   * <p>The metadata is stored in PartitionedFile.otherConstantMetadataColumnValues and used by
   * DeltaParquetFileFormat to generate the is_row_deleted column.
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> buildMetadata(Optional<DeletionVectorDescriptor> dvOpt) {
    if (!dvOpt.isPresent()) {
      return (Map<String, Object>) (Map<?, ?>) Map$.MODULE$.empty();
    }

    String dvBase64 = convertToSpark(dvOpt.get()).serializeToBase64();

    return (Map<String, Object>)
        (Map<?, ?>)
            Map$.MODULE$
                .empty()
                .$plus(
                    new Tuple2<>(
                        DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED(), dvBase64))
                .$plus(
                    new Tuple2<>(
                        DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE(),
                        RowIndexFilterType.IF_CONTAINED));
  }

  /** Add the __delta_internal_is_row_deleted column to the schema. */
  public static StructType augmentSchema(StructType schema) {
    return schema.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
  }
}
