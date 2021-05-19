/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.tables

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.catalog.DeltaTableV2

import org.apache.spark.sql.DataFrame

object DeltaTableTestUtils {

  /** A utility method to access the private constructor of [[DeltaTable]] in tests. */
  def createTable(df: DataFrame, deltaLog: DeltaLog): DeltaTable = {
    new DeltaTable(df, DeltaTableV2(df.sparkSession, deltaLog.dataPath))
  }
}
