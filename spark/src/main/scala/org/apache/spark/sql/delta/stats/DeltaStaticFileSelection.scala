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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta.metering.DeltaLogging


/**
 * Keeps the V1 and V2 static file-selection boundary identical by owning the scoped call in one
 * place. This object is public because its V1 and V2 callers export into separate package trees.
 */
object DeltaStaticFileSelection extends DeltaLogging {

  /**
   * Records only `body`, which must be the actual `DeltaScanGenerator.filesForScan`-equivalent
   * call.
   */
  def record(body: => DeltaScan): DeltaScan = {
    val scan = recordFrameProfile("Delta", "staticFileSelection") {
      val result = body
      result
    }
    scan
  }
}
