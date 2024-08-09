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

package org.apache.spark.sql.delta.coordinatedcommits

import scala.collection.mutable

import io.delta.dynamodbcommitcoordinator.DynamoDBCommitCoordinatorClientBuilder
import io.delta.storage.commit.CommitCoordinatorClient

import org.apache.spark.sql.SparkSession

object CommitCoordinatorClient {
  def semanticEquals(
      commitCoordinatorClientOpt1: Option[CommitCoordinatorClient],
      commitCoordinatorClientOpt2: Option[CommitCoordinatorClient]): Boolean = {
    (commitCoordinatorClientOpt1, commitCoordinatorClientOpt2) match {
      case (Some(commitCoordinatorClient1), Some(commitCoordinatorClient2)) =>
        commitCoordinatorClient1.semanticEquals(commitCoordinatorClient2)
      case (None, None) =>
        true
      case _ =>
        false
    }
  }
}

/** A builder interface for [[CommitCoordinatorClient]] */
trait CommitCoordinatorBuilder {

  /** Name of the commit-coordinator */
  def getName: String

  /** Returns a commit-coordinator client based on the given conf */
  def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient
}

/** Factory to get the correct [[CommitCoordinatorClient]] for a table */
object CommitCoordinatorProvider {
  // mapping from different commit-coordinator names to the corresponding
  // [[CommitCoordinatorBuilder]]s.
  private val nameToBuilderMapping = mutable.Map.empty[String, CommitCoordinatorBuilder]

  /** Registers a new [[CommitCoordinatorBuilder]] with the [[CommitCoordinatorProvider]] */
  def registerBuilder(commitCoordinatorBuilder: CommitCoordinatorBuilder): Unit = synchronized {
    nameToBuilderMapping.get(commitCoordinatorBuilder.getName) match {
      case Some(commitCoordinatorBuilder: CommitCoordinatorBuilder) =>
        throw new IllegalArgumentException(
          s"commit-coordinator: ${commitCoordinatorBuilder.getName} already" +
          s" registered with builder ${commitCoordinatorBuilder.getClass.getName}")
      case None =>
        nameToBuilderMapping.put(commitCoordinatorBuilder.getName, commitCoordinatorBuilder)
    }
  }

  /** Returns a [[CommitCoordinatorClient]] for the given `name`, `conf`, and `spark` */
  def getCommitCoordinatorClient(
      name: String,
      conf: Map[String, String],
      spark: SparkSession): CommitCoordinatorClient = synchronized {
    nameToBuilderMapping.get(name).map(_.build(spark, conf)).getOrElse {
      throw new IllegalArgumentException(s"Unknown commit-coordinator: $name")
    }
  }

  // Visible only for UTs
  private[delta] def clearNonDefaultBuilders(): Unit = synchronized {
    val initialCommitCoordinatorNames = initialCommitCoordinatorBuilders.map(_.getName).toSet
    nameToBuilderMapping.retain((k, _) => initialCommitCoordinatorNames.contains(k))
  }

  private val initialCommitCoordinatorBuilders = Seq[CommitCoordinatorBuilder](
    new DynamoDBCommitCoordinatorClientBuilder()
  )
  initialCommitCoordinatorBuilders.foreach(registerBuilder)
}
