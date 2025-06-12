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
import java.util.concurrent._

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.logging.DeltaLogKeys

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.{LoggingShims, MDC}
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Implementation of ThreadPoolExecutor that captures the Spark ThreadLocals present at submit time
 * and inserts them into the thread before executing the provided runnable.
 */
class SparkThreadLocalForwardingThreadPoolExecutor(
    corePoolSize: Int,
    maximumPoolSize: Int,
    keepAliveTime: Long,
    unit: TimeUnit,
    workQueue: BlockingQueue[Runnable],
    threadFactory: ThreadFactory,
    rejectedExecutionHandler: RejectedExecutionHandler = new ThreadPoolExecutor.AbortPolicy)
  extends ThreadPoolExecutor(
    corePoolSize, maximumPoolSize, keepAliveTime,
    unit, workQueue, threadFactory, rejectedExecutionHandler) {

  override def execute(command: Runnable): Unit =
    super.execute(new SparkThreadLocalCapturingRunnable(command))
}


trait SparkThreadLocalCapturingHelper extends LoggingShims {
  // At the time of creating this instance we capture the task context and command context.
  val capturedTaskContext = TaskContext.get()
  val sparkContext = SparkContext.getActive
  // Capture an immutable threadsafe snapshot of the current local properties
  val capturedProperties = sparkContext
    .map(sc => CapturedSparkThreadLocals.toValuesArray(
      SparkUtils.cloneProperties(sc.getLocalProperties)))

  def runWithCaptured[T](body: => T): T = {
    // Save the previous contexts, overwrite them with the captured contexts, and then restore the
    // previous when execution completes.
    // This has the unfortunate side effect of writing nulls to these thread locals if they were
    // empty beforehand.
    val previousTaskContext = TaskContext.get()
    val previousProperties = sparkContext.map(_.getLocalProperties)

    TaskContext.setTaskContext(capturedTaskContext)
    for {
      p <- capturedProperties
      sc <- sparkContext
    } {
      sc.setLocalProperties(CapturedSparkThreadLocals.toProperties(p))
    }

    try {
      body
    } catch {
      case t: Throwable =>
        logError(log"Exception in thread " +
          log"${MDC(DeltaLogKeys.THREAD_NAME, Thread.currentThread().getName)}", t)
        throw t
    } finally {
      TaskContext.setTaskContext(previousTaskContext)
      for {
        p <- previousProperties
        sc <- sparkContext
      } {
        sc.setLocalProperties(p)
      }
    }
  }
}

class CapturedSparkThreadLocals extends SparkThreadLocalCapturingHelper

object CapturedSparkThreadLocals {
  def apply(): CapturedSparkThreadLocals = {
    new CapturedSparkThreadLocals()
  }

  def toProperties(props: Array[(String, String)]): Properties = {
    val resultProps = new Properties()
    for ((key, value) <- props) {
      resultProps.put(key, value)
    }
    resultProps
  }

  def toValuesArray(props: Properties): Array[(String, String)] = {
    props.asScala.toArray
  }

}

class SparkThreadLocalCapturingRunnable(runnable: Runnable)
    extends Runnable with SparkThreadLocalCapturingHelper {
  override def run(): Unit = {
    runWithCaptured(runnable.run())
  }
}
