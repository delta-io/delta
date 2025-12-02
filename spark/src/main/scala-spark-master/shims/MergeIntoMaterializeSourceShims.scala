/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.commands.merge

import org.apache.spark.SparkException

object MergeIntoMaterializeSourceShims {

  /** In Spark 4.0+ we could check on error class, which is more stable. */
  def mergeMaterializedSourceRddBlockLostError(e: SparkException, rddId: Int): Boolean = {
    e.getErrorClass == "CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND" &&
      e.getMessageParameters.get("rddBlockId").contains(s"rdd_${rddId}")
  }
}
