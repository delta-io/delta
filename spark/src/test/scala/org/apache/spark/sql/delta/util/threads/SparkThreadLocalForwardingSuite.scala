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
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

import org.apache.spark._
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.ThreadUtils.namedThreadFactory

class SparkThreadLocalForwardingSuite extends SparkFunSuite {

  private def createThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val keepAliveTimeSeconds = 60
    val threadPool = new SparkThreadLocalForwardingThreadPoolExecutor(
      nThreads,
      nThreads,
      keepAliveTimeSeconds,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  test("SparkThreadLocalForwardingThreadPoolExecutor properly propagates" +
      " TaskContext and Spark Local Properties") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local"))
    val executor = createThreadPool(1, "test-threads")
    implicit val executionContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(executor)

    val prevTaskContext = TaskContext.get()
    try {
      // assert that each instance of submitting a task to the execution context captures the
      // current task context
      val futures = (1 to 10) map { i =>
        setTaskAndProperties(i, sc)

        Future {
          checkTaskAndProperties(i, sc)
        }(executionContext)
      }

      assert(ThreadUtils.awaitResult(Future.sequence(futures), 10.seconds).forall(identity))
    } finally {
      ThreadUtils.shutdown(executor)
      TaskContext.setTaskContext(prevTaskContext)
      sc.stop()
    }
  }

  def makeTaskContext(id: Int): TaskContext = {
    new TaskContextImpl(id, 0, 0, 0, attemptNumber = 45613, 0, null, new Properties(), null)
  }

  def setTaskAndProperties(i: Int, sc: SparkContext = SparkContext.getActive.get): Unit = {
    val tc = makeTaskContext(i)
    TaskContext.setTaskContext(tc)
    sc.setLocalProperty("test", i.toString)
  }

  def checkTaskAndProperties(i: Int, sc: SparkContext = SparkContext.getActive.get): Boolean = {
    TaskContext.get() != null &&
      TaskContext.get().stageId() == i &&
      sc.getLocalProperty("test") == i.toString
  }

  test("That CapturedSparkThreadLocals properly restores the existing state") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local"))
    val prevTaskContext = TaskContext.get()
    try {
      setTaskAndProperties(10)
      val capturedSparkThreadLocals = CapturedSparkThreadLocals()
      setTaskAndProperties(11)
      assert(!checkTaskAndProperties(10, sc))
      assert(checkTaskAndProperties(11, sc))
      capturedSparkThreadLocals.runWithCaptured {
        assert(checkTaskAndProperties(10, sc))
      }
      assert(checkTaskAndProperties(11, sc))
    } finally {
      TaskContext.setTaskContext(prevTaskContext)
      sc.stop()
    }
  }

  test("That CapturedSparkThreadLocals properly restores the existing spark properties." +
    " Changes to local properties inside a task do not affect the original properties") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local"))
    try {
      sc.setLocalProperty("TestProp", "1")
      val capturedSparkThreadLocals = CapturedSparkThreadLocals()
      assert(sc.getLocalProperty("TestProp") == "1")
      capturedSparkThreadLocals.runWithCaptured {
        sc.setLocalProperty("TestProp", "2")
        assert(sc.getLocalProperty("TestProp") == "2")
      }
      assert(sc.getLocalProperty("TestProp") == "1")
    } finally {
      sc.stop()
    }
  }


  test("captured spark thread locals are immutable") {
    val sc = SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster("local"))
    try {
      sc.setLocalProperty("test1", "good")
      sc.setLocalProperty("test2", "good")
      val threadLocals = CapturedSparkThreadLocals()
      sc.setLocalProperty("test2", "bad")
      assert(sc.getLocalProperty("test1") == "good")
      assert(sc.getLocalProperty("test2") == "bad")
      threadLocals.runWithCaptured {
        assert(sc.getLocalProperty("test1") == "good")
        assert(sc.getLocalProperty("test2") == "good")
        sc.setLocalProperty("test1", "bad")
        sc.setLocalProperty("test2", "maybe")
        assert(sc.getLocalProperty("test1") == "bad")
        assert(sc.getLocalProperty("test2") == "maybe")
      }
      assert(sc.getLocalProperty("test1") == "good")
      assert(sc.getLocalProperty("test2") == "bad")
      threadLocals.runWithCaptured {
        assert(sc.getLocalProperty("test1") == "good")
        assert(sc.getLocalProperty("test2") == "good")
      }
    } finally {
      sc.stop()
    }
  }
}
