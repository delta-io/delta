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

package org.apache.spark.sql.delta.util.threads

import java.util.Properties

import org.apache.spark.{SparkFunSuite, TaskContext, TaskContextImpl}
import org.apache.spark.sql.test.SharedSparkSession

class DeltaThreadPoolSuite extends SparkFunSuite with SharedSparkSession {

  val threadPool: DeltaThreadPool = DeltaThreadPool("test", 1)

  def makeTaskContext(id: Int): TaskContext = {
    new TaskContextImpl(id, 0, 0, 0, attemptNumber = 45613, 0, null, new Properties(), null)
  }

  def testForwarding(testName: String, id: Int)(f: => Unit): Unit = {
    test(testName) {
      val prevTaskContext = TaskContext.get()
      TaskContext.setTaskContext(makeTaskContext(id))
      sparkContext.setLocalProperty("test", id.toString)

      try {
        f
      } finally {
        TaskContext.setTaskContext(prevTaskContext)
      }
    }
  }

  def assertTaskAndProperties(id: Int): Unit = {
    assert(TaskContext.get() !== null)
    assert(TaskContext.get().stageId() === id)
    assert(sparkContext.getLocalProperty("test") === id.toString)
  }

  testForwarding("parallelMap captures TaskContext", id = 0) {
    threadPool.parallelMap(spark, 0 until 1) { _ =>
      assertTaskAndProperties(id = 0)
    }
  }

  testForwarding("submit captures TaskContext and local properties", id = 1) {
    threadPool.submit(spark) {
      assertTaskAndProperties(id = 1)
    }
  }

  testForwarding("submitNonFateSharing captures TaskContext and local properties", id = 2) {
    threadPool.submitNonFateSharing { _ =>
      assertTaskAndProperties(id = 2)
    }
  }
}
