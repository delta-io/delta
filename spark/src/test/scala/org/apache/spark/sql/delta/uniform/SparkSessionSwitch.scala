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

package org.apache.spark

import org.apache.spark.sql.SparkSession

/**
 * Helper for easily switch between multiple sessions in test
 */
trait SparkSessionSwitch {

  private val knownSessions =
    collection.mutable.HashMap[SparkSession, (Option[SparkContext], SparkEnv)]()

  /**
   * Create a SparkSession and save its context. Calling this will not change
   * the current active SparkSession. Use [[withSession]] when you want to use
   * the newly created session.
   *
   * @param factory used to create the session
   * @return the newly created session
   */
  def newSession(factory: => SparkSession): SparkSession = {
    registerActiveSession()
    val old = SparkSession.getActiveSession
    clear()
    val created = factory
    registerActiveSession()
    old.foreach(restore)
    created
  }

  /**
   * Execute code with the given session.
   * @param session session to use
   * @param thunk code to execute within the specified session
   */
  def withSession[T](session: SparkSession)(thunk: SparkSession => T): T = {
    val oldSession = SparkSession.getActiveSession
    restore(session)
    val result = thunk(session)
    oldSession.foreach(restore)
    result
  }

  /**
   * Record the SparkContext/SparkEnv for current active session
   */
  private def registerActiveSession(): Unit = {
    SparkSession.getActiveSession
      .foreach(knownSessions.put(_, (SparkContext.getActive, SparkEnv.get)))
  }

  /**
   * Restore the snapshot made for the given session
   * @param session the session to be restore
   */
  private def restore(session: SparkSession): Unit = {
    val (restoreContext, restoreEnv) = knownSessions.getOrElse(
      session, throw new IllegalArgumentException("Unknown Session to restore"))
    SparkSession.setActiveSession(session)
    SparkSession.setDefaultSession(session)

    val oldContext = SparkContext.getActive
    SparkContext.clearActiveContext()
    restoreContext.foreach(SparkContext.setActiveContext)
    // Synchronize the context
    (oldContext, restoreContext) match {
      case (Some(off), Some(on)) => syncContext(off, on)
      case _ =>
    }

    SparkEnv.set(restoreEnv)
  }

  /**
   * Clear the session related context. Necessary before creating new sessions
   */
  private def clear(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkContext.clearActiveContext()
    SparkEnv.set(null)
  }

  /**
   * Synchronize local properties when switch SparkContext by merging
   * and overwriting from off to on
   * @param off the context to be deactivated
   * @param on the context to be activated
   */
  private def syncContext(off: SparkContext, on: SparkContext): Unit = {
    // NOTE: cannot use putAll due to a problem of Scala2 + JDK9+
    // See https://github.com/scala/bug/issues/10418 for detail
    val onProperties = on.localProperties.get()
    off.localProperties.get().forEach((k, v) => onProperties.put(k, v))
  }
}
