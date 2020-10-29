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

package io.delta.standalone.internal.util

import java.io.File

import io.delta.standalone.DeltaLog
import io.delta.standalone.internal.DeltaLogImpl
import org.apache.hadoop.conf.Configuration

object GoldenTableUtils {

  /** Load the golden table as a class resource so that it works in IntelliJ and SBT tests */
  val goldenTable = new File(getClass.getResource("/golden").toURI)

  /**
   * Create a [[DeltaLog]] for the given golden table and execute the test function.
   *
   * @param name The name of the golden table to load.
   * @param testFunc The test to execute which takes the [[DeltaLog]] as input arg.
   */
  def withLogForGoldenTable(name: String)(testFunc: DeltaLog => Unit): Unit = {
    val tablePath = new File(goldenTable, name).getCanonicalPath
    val log = DeltaLog.forTable(new Configuration(), tablePath)
    testFunc(log)
  }

  /**
   * Create a [[DeltaLogImpl]] for the given golden table and execute the test function.
   *
   * This should only be used when `private[internal]` methods and variables (which [[DeltaLog]]
   * doesn't expose but [[DeltaLogImpl]] does) are needed by the test function.
   *
   * @param name The name of the golden table to load.
   * @param testFunc The test to execute which takes the [[DeltaLogImpl]] as input arg.
   */
  def withLogImplForGoldenTable(name: String)(testFunc: DeltaLogImpl => Unit): Unit = {
    val tablePath = new File(goldenTable, name).getCanonicalPath
    val log = DeltaLogImpl.forTable(new Configuration(), tablePath)
    testFunc(log)
  }

  /**
   * Create the full table path for the given golden table and execute the test function.
   *
   * @param name The name of the golden table to load.
   * @param testFunc The test to execute which takes the full table path as input arg.
   */
  def withGoldenTable(name: String)(testFunc: String => Unit): Unit = {
    val tablePath = new File(goldenTable, name).getCanonicalPath
    testFunc(tablePath)
  }
}
