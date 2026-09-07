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

package io.delta.spark.internal.v2

import java.util.concurrent.FutureTask

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

class KernelContextSuite extends SparkFunSuite with SharedSparkSession {

  private val hadoopConfKey = "kernel.context.test"
  private val invariantHadoopConfKey = "kernel.context.invariant.test"

  test("an empty context uses the active session Hadoop configuration") {
    withSQLConf(hadoopConfKey -> "session-value") {
      assert(KernelContext.empty.materializeHadoopConf().get(hadoopConfKey) == "session-value")
    }
  }

  test("session-invariant filesystem options override the active session configuration") {
    withSQLConf(hadoopConfKey -> "session-value") {
      val context = KernelContext(Map(hadoopConfKey -> "context-value"))

      assert(context.materializeHadoopConf().get(hadoopConfKey) == "context-value")
    }
  }

  test("materializeHadoopConf resolves the active Spark session on every call") {
    val originalSession = SparkSession.active
    val otherSession = spark.newSession()
    originalSession.conf.set(hadoopConfKey, "original-session")
    otherSession.conf.set(hadoopConfKey, "other-session")
    val context = KernelContext.empty

    try {
      SparkSession.setActiveSession(originalSession)
      assert(context.materializeHadoopConf().get(hadoopConfKey) == "original-session")

      SparkSession.setActiveSession(otherSession)
      assert(context.materializeHadoopConf().get(hadoopConfKey) == "other-session")
    } finally {
      SparkSession.setActiveSession(originalSession)
      originalSession.conf.unset(hadoopConfKey)
      otherSession.conf.unset(hadoopConfKey)
    }
  }

  test("child threads inherit the active session without changing the shared KernelContext") {
    val originalSession = SparkSession.active
    val sessionA = spark.newSession()
    val sessionB = spark.newSession()
    val invariantOptions = Map(invariantHadoopConfKey -> "context-value")
    val context = KernelContext(invariantOptions)

    try {
      sessionA.conf.set(hadoopConfKey, "session-a")
      SparkSession.setActiveSession(sessionA)
      val parentAConf = context.materializeHadoopConf()
      val childATask = new FutureTask[(SparkSession, String, String)](() => {
        val hadoopConf = context.materializeHadoopConf()
        (SparkSession.active, hadoopConf.get(hadoopConfKey), hadoopConf.get(invariantHadoopConfKey))
      })
      new Thread(childATask).start()
      val (childASession, childASessionValue, childAInvariantValue) = childATask.get()

      assert(childASession eq sessionA)
      assert(childASessionValue == parentAConf.get(hadoopConfKey))
      assert(childAInvariantValue == parentAConf.get(invariantHadoopConfKey))

      sessionB.conf.set(hadoopConfKey, "session-b")
      SparkSession.setActiveSession(sessionB)
      val parentBConf = context.materializeHadoopConf()
      val childBTask = new FutureTask[(SparkSession, String, String)](() => {
        val hadoopConf = context.materializeHadoopConf()
        (SparkSession.active, hadoopConf.get(hadoopConfKey), hadoopConf.get(invariantHadoopConfKey))
      })
      new Thread(childBTask).start()
      val (childBSession, childBSessionValue, childBInvariantValue) = childBTask.get()

      assert(childBSession eq sessionB)
      assert(childBSessionValue == parentBConf.get(hadoopConfKey))
      assert(childBSessionValue != childASessionValue)
      assert(childBInvariantValue == parentBConf.get(invariantHadoopConfKey))
      assert(childBInvariantValue == childAInvariantValue)
      assert(context.sessionInvariantFsOptions == invariantOptions)
    } finally {
      SparkSession.setActiveSession(originalSession)
      sessionA.conf.unset(hadoopConfKey)
      sessionB.conf.unset(hadoopConfKey)
    }
  }

  test("constructing a context with null session-invariant filesystem options throws") {
    Seq[() => KernelContext](
      () => new KernelContext(null),
      () => KernelContext(null)).foreach { createContext =>
      val error = intercept[IllegalArgumentException] {
        createContext()
      }
      assert(error.getMessage == "requirement failed: sessionInvariantFsOptions must not be null")
    }
  }
}
