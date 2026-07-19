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

package io.delta.internal

import io.delta.spark.internal.v2.catalog.DeltaV2Table

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ChangelogInfoUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{ChangelogInfo, Identifier}
import org.apache.spark.sql.delta.{DeltaErrors, TableChanges}
import org.apache.spark.sql.delta.catalog.{ChangelogSupport, DeltaV2TableMarker}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation, DataSourceV2RelationShim}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Resolves CDF reads against V2 [[DeltaV2Table]] tables (`table_changes(...)` and
 * `.option("readChangeFeed", "true").table(name)`) into the kernel-based DSv2 changelog plan.
 *
 * V1 [[org.apache.spark.sql.delta.catalog.DeltaTableV2]] CDC reads stay on the legacy path
 * handled by [[org.apache.spark.sql.delta.DeltaAnalysis]]'s `TableChanges` rule -- the V1 rule
 * skips itself when it sees a [[org.apache.spark.sql.delta.catalog.DeltaV2TableMarker]]
 * child (the marker `DeltaV2Table` mixes in) so this rule can take over without contention.
 */
class ResolveTableChangesV2(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {

    if (!session.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHANGELOG_V2_ENABLED)) return plan

    plan.resolveOperators {
      // `table_changes('t', 0, 5)` where the catalog returned DeltaV2Table. Rewrite the inner
      // `DataSourceV2Relation(DeltaV2Table)` into a `ChangelogTable` plan; the result replaces
      // the entire `TableChanges` node.
      case tc: TableChanges if tc.child.resolved &&
        plan.exists(DeltaV2TableMarker.isDeltaV2TableRelation) =>
        tc.child.transformDown {
          case DataSourceV2RelationShim(_: DeltaV2Table,
          _, Some(catalog: ChangelogSupport), Some(ident), options) =>
            buildChangelogRelation(catalog, ident, options)
        }

      // `.option("readChangeFeed", "true").table(name)` where the catalog returned DeltaV2Table
      // (no `TableChanges` wrapper -- the relation flows straight into the query). Check for a
      // `timeTravelSpec` to see if this is a time travel query.
      case DataSourceV2Relation(_: DeltaV2Table,
      _, Some(catalog: ChangelogSupport), Some(ident), options, timeTravelSpec)
        if CDCReader.isCDCRead(options) =>
        rejectTimeTravelWithCDF(timeTravelSpec.isDefined)
        buildChangelogRelation(catalog, ident, options)
    }
  }

  /**
   * Builds a Spark DSv2 CDC relation for a Delta DSv2 table using the provided reader options.
   */
  private def buildChangelogRelation(
      catalog: ChangelogSupport,
      ident: Identifier,
      options: CaseInsensitiveStringMap): LogicalPlan = {
    rejectUnsupportedOptions(options)
    // We can use [[ChangelogInfoUtils.fromOptions]] directly here because Spark's CDC options
    // match Delta's CDF options, e.p. startingVersion, endingVersion.
    val baseInfo = ChangelogInfoUtils.fromOptions(
      options,
      session.sessionState.conf.sessionLocalTimeZone)
    val info = new ChangelogInfo(
      baseInfo.range(),
      ChangelogInfo.DeduplicationMode.DROP_CARRYOVERS,
      /* computeUpdates = */ true)
    val changelog = catalog.loadChangelog(ident, info)
    DataSourceV2Relation.create(
      ChangelogTable(changelog, info), Some(catalog), Some(ident), options)
  }

  /**
   * Rejects options that aren't supposed to be supported with Delta CDF, even though Spark's CDC
   * implementation can support these modes.
   */
  private def rejectUnsupportedOptions(options: CaseInsensitiveStringMap): Unit = {
    val unsupportedOptions = Seq(
      "computeUpdates",
      "deduplicationMode",
      "startingBoundInclusive",
      "endingBoundInclusive")
    unsupportedOptions.foreach { key =>
      if (options.containsKey(key)) throw DeltaErrors.changelogUnsupportedOption(key)
    }
  }

  /**
   * Block combining a CDF read with a time travel pin.
   */
  private def rejectTimeTravelWithCDF(hasTimeTravelSpec: Boolean): Unit = {
    if (hasTimeTravelSpec) {
      throw DeltaErrors.timeTravelNotSupportedException
    }
  }
}
