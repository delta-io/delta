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

package org.apache.spark.sql.delta
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.{AlterTableSetPropertiesDeltaCommand, AlterTableUnsetPropertiesDeltaCommand}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.{Utils => DeltaUtils}

/**
 * A base class for implementing a preparation command for removing table features.
 * Must implement a run method. Note, the run method must be implemented in a way that when
 * it finishes, the table does not use the feature that is being removed, and nobody is
 * allowed to start using it again implicitly. One way to achieve this is by
 * disabling the feature on the table before proceeding to the actual removal.
 * See [[RemovableFeature.preDowngradeCommand]].
 */
sealed abstract class PreDowngradeTableFeatureCommand {
  /**
   * Returns true when it performs a cleaning action. When no action was required
   * it returns false.
   */
  def removeFeatureTracesIfNeeded(): Boolean
}

case class TestWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  // To remove the feature we only need to remove the table property.
  override def removeFeatureTracesIfNeeded(): Boolean = {
    // Make sure feature data/metadata exist before proceeding.
    if (TestRemovableWriterFeature.validateRemoval(table.initialSnapshot)) return false

    if (DeltaUtils.isTesting) {
      recordDeltaEvent(table.deltaLog, "delta.test.TestWriterFeaturePreDowngradeCommand")
    }

    val properties = Seq(TestRemovableWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestReaderWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  // To remove the feature we only need to remove the table property.
  override def removeFeatureTracesIfNeeded(): Boolean = {
    // Make sure feature data/metadata exist before proceeding.
    if (TestRemovableReaderWriterFeature.validateRemoval(table.initialSnapshot)) return false

    if (DeltaUtils.isTesting) {
      recordDeltaEvent(table.deltaLog, "delta.test.TestReaderWriterFeaturePreDowngradeCommand")
    }

    val properties = Seq(TestRemovableReaderWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestLegacyWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand {
  /** Return true if we removed the property, false if no action was needed. */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    if (TestRemovableLegacyWriterFeature.validateRemoval(table.initialSnapshot)) return false

    val properties = Seq(TestRemovableLegacyWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class TestLegacyReaderWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand {
  /** Return true if we removed the property, false if no action was needed. */
  override def removeFeatureTracesIfNeeded(): Boolean = {
    if (TestRemovableLegacyReaderWriterFeature.validateRemoval(table.initialSnapshot)) return false

    val properties = Seq(TestRemovableLegacyReaderWriterFeature.TABLE_PROP_KEY)
    AlterTableUnsetPropertiesDeltaCommand(table, properties, ifExists = true).run(table.spark)
    true
  }
}

case class V2CheckpointPreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand
  with DeltaLogging {
  /**
   * We set the checkpoint policy to classic to prevent any transactions from creating
   * v2 checkpoints.
   *
   * @return True if it changed checkpoint policy metadata property to classic.
   *         False otherwise.
   */
  override def removeFeatureTracesIfNeeded(): Boolean = {

    if (V2CheckpointTableFeature.validateRemoval(table.initialSnapshot)) return false

    val startTimeNs = System.nanoTime()
    val properties = Map(DeltaConfigs.CHECKPOINT_POLICY.key -> CheckpointPolicy.Classic.name)
    AlterTableSetPropertiesDeltaCommand(table, properties).run(table.spark)

    recordDeltaEvent(
      table.deltaLog,
      opType = "delta.v2CheckpointFeatureRemovalMetrics",
      data =
        Map(("downgradeTimeMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)))
    )

    true
  }
}
