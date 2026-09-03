/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2.kernel

import org.apache.hadoop.conf.Configuration
import io.delta.kernel.defaults.engine.{DefaultEngine => KernelDefaultEngine}
import io.delta.kernel.engine.{Engine => KernelEngine}

/** Factory for creating the default Kernel [[KernelEngine]] used by the DSv2 connector. */
object KernelEngineFactory {

  /** Builds the backend-appropriate default engine. */
  def createDefaultEngine(hadoopConf: Configuration): KernelEngine = {
    KernelDefaultEngine.create(hadoopConf)
  }
}
