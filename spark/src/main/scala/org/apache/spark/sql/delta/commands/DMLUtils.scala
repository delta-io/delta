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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaCommitTag
import org.apache.spark.sql.delta.actions.{Action, FileAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf.DELTA_LAST_COMMIT_VERSION_IN_SESSION

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.setActiveSession

object DMLUtils {

  /** Holder for some of the parameters for a `OptimisticTransaction.commit` */
  case class TaggedCommitData[A <: Action](
      actions: Seq[A],
      tags: Map[DeltaCommitTag, String] = Map.empty) {

    def withTag[T](key: DeltaCommitTag.TypedCommitTag[T], value: T): TaggedCommitData[A] = {
      val mergedValue = key.mergeWithNewTypedValue(tags.get(key), value)
      this.copy(tags = this.tags + (key -> mergedValue))
    }

    def stringTags: Map[String, String] = tags.map { case (k, v) => k.key -> v }
  }

  object TaggedCommitData {
    def empty[A <: Action]: TaggedCommitData[A] = TaggedCommitData(Seq.empty[A])
  }
}

/**
 * Helper trait to run a block of code with a cloned SparkSession and overwrite SQL configs.
 */
trait RunWithClonedSparkSession {

  protected val SQLConfigsToOverwrite: Map[ConfigEntry[Boolean], Boolean]

  def runWithClonedSparkSession[T](spark: SparkSession)(f: SparkSession => T): T = {
    val clonedSession = spark.cloneSession()
    SQLConfigsToOverwrite.foreach { case (key, overwriteValue) =>
      clonedSession.sessionState.conf.setConf(key, overwriteValue)
    }
    setActiveSession(clonedSession)
    try {
      f(clonedSession)
    } finally {
      copyBackLastCommitVersionFromClonedSession(
        originalSession = spark,
        clonedSession = clonedSession
      )
      setActiveSession(spark)
    }
  }

  /**
   * Copy last commit version from cloned session in case it was updated. This makes sure the
   * caller will see the updated value using the original session. Any concurrent modification in
   * the original session would get overwritten. But a similar issue would happen anyways if the
   * original session was used directly instead of the clone.
   */
  private def copyBackLastCommitVersionFromClonedSession(
      originalSession: SparkSession,
      clonedSession: SparkSession): Unit = {
    val copyConfigs = Set(DELTA_LAST_COMMIT_VERSION_IN_SESSION.key)
    copyConfigs.foreach { config =>
      clonedSession.conf
        .getOption(config)
        .foreach(value => originalSession.conf.set(config, value))
    }
  }
}
