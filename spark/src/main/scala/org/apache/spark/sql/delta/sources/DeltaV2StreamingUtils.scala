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

package org.apache.spark.sql.delta.sources

/**
 * Utilities/constants for Delta's V2 streaming path integration.
 *
 * This is defined in sparkV1 so it can be referenced from both:
 * - spark (DeltaDataSource / V1 streaming APIs)
 * - spark-unified (ApplyV2Streaming rule)
 */
object DeltaV2StreamingUtils {
  /** Marker option key injected by ApplyV2Streaming into streaming options. */
  final val V2_STREAMING_SCHEMA_SOURCE_KEY: String = "__v2StreamingSchemaSource"

  /** Marker option value indicating the schema originated from kernel SparkTable. */
  final val V2_STREAMING_SCHEMA_SOURCE_SPARK_TABLE: String = "SparkTable"
}
