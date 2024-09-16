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
package io.delta.kernel.internal.snapshot

import io.delta.kernel.internal.TableConfig.{ENABLE_EXPIRED_LOG_CLEANUP, ICEBERG_COMPAT_V2_ENABLED, LOG_RETENTION}
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

class MetadataCleanupSuite extends AnyFunSuite with MockFileSystemClientUtils {
  // Input
  //  1. List of delta log file contents (that includes delta and checkpoint files)
  //  2. Current time through mocked Clock
  //  3. Table metadata retention enabled and a period of time for
  //  which metadata should be retained

  // Output
  // 1. List of delta log files that should be deleted


  def metadata(logCleanupEnabled: Boolean = true, retentionMillis: Int = 24): Metadata = {
    val configurationMap = Map(
      ENABLE_EXPIRED_LOG_CLEANUP.getKey -> logCleanupEnabled.toString,
      LOG_RETENTION.getKey -> retentionMillis.toString
    )
    new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      DataTypeJsonSerDe.serializeDataType(new StructType()),
      new StructType(),
      VectorUtils.stringArrayValue(Collections.emptyList()), // partitionColumns
      Optional.empty(), // createdTime
      stringStringMapValue(configurationMap.asJava) // configurationMap
    )
  }
}
