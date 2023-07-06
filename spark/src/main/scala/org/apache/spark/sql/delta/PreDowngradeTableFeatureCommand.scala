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

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableUnsetPropertiesDeltaCommand

/**
 * A base class for implementing a preparation command for removing table features.
 * Must implement a run method. Note, the run method must be implemented in a way that when
 * it finishes, the table does not use the feature that is being removed, and nobody is
 * allowed to start using it again implicitly. One way to achieve this is by
 * disabling the feature on the table before proceeding to the actual removal.
 * See [[RemovableFeature.preDowngradeCommand]].
 */
sealed abstract class PreDowngradeTableFeatureCommand {
  def table: DeltaTableV2
  def run(): Unit
}

case class TestWriterFeaturePreDowngradeCommand(table: DeltaTableV2)
  extends PreDowngradeTableFeatureCommand {
  // To remove the feature we only need to remove the table property.
  override def run(): Unit = {
    AlterTableUnsetPropertiesDeltaCommand(
      table, Seq(TestRemovableWriterFeature.TABLE_PROP_KEY), true).run(table.spark)
  }
}
