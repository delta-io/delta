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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.commands.merge.MergeIntoMaterializeSource
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession

trait InsertReplaceOnMaterializeSource extends MergeIntoMaterializeSource {
  override protected def operation: String = "INSERT REPLACE ON/USING"

  override protected def enableColumnPruningBeforeMaterialize: Boolean = false

  override protected def materializeSourceErrorOpType: String =
    InsertReplaceOnOrUsingMaterializeSourceError.OP_TYPE

  override protected def getMaterializeSourceMode(spark: SparkSession): String =
    spark.conf.get(DeltaSQLConf.INSERT_REPLACE_ON_OR_USING_MATERIALIZE_SOURCE)

}

object InsertReplaceOnOrUsingMaterializeSourceError {
  val OP_TYPE = "delta.dml.insertReplaceOnOrUsing.materializeSourceError"
}
