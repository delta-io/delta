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

package io.delta.sharing.spark

import io.delta.sharing.client.util.ConfUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

// A spark rule that applies limit pushdown to DeltaSharingFileIndex, when the config is enabled.
// To allow only fetching needed files from delta sharing server.
object DeltaFormatSharingLimitPushDown extends Rule[LogicalPlan] {

  def setup(spark: SparkSession): Unit = synchronized {
    if (!spark.experimental.extraOptimizations.contains(DeltaFormatSharingLimitPushDown)) {
      spark.experimental.extraOptimizations ++= Seq(DeltaFormatSharingLimitPushDown)
    }
  }

  def apply(p: LogicalPlan): LogicalPlan = {
    p transform {
      case localLimit @ LocalLimit(
            literalExpr @ IntegerLiteral(limit),
            l @ LogicalRelation(
              r @ HadoopFsRelation(remoteIndex: DeltaSharingFileIndex, _, _, _, _, _),
              _,
              _,
              _
            )
          ) if (ConfUtils.limitPushdownEnabled(p.conf) && remoteIndex.limitHint.isEmpty) =>
          val spark = SparkSession.active
          val newRel = r.copy(location = remoteIndex.copy(limitHint = Some(limit)))(spark)
          LocalLimit(literalExpr, l.copy(relation = newRel))
    }
  }
}
