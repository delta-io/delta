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

package org.apache.spark.sql.delta.skipping.clustering.temp

import org.apache.spark.sql.connector.expressions.{Expression, NamedReference, Transform}

/**
 * Minimal version of Spark's ClusterByTransform. We'll remove this when we integrate with OSS
 * Spark's CLUSTER BY implementation.
 *
 * This class represents a transform for `ClusterBySpec`. This is used to bundle
 * ClusterBySpec in CreateTable's partitioning transforms to pass it down to analyzer/delta.
 */
final case class ClusterByTransform(
    columnNames: Seq[NamedReference]) extends Transform {

  override val name: String = "temp_cluster_by"

  override def arguments: Array[Expression] = columnNames.toArray

  override def toString: String = s"$name(${arguments.map(_.describe).mkString(", ")})"
}

/**
 * Convenience extractor for ClusterByTransform.
 */
object ClusterByTransform {
  def unapply(transform: Transform): Option[Seq[NamedReference]] =
    transform match {
      case NamedTransform("temp_cluster_by", arguments) =>
        Some(arguments.map(_.asInstanceOf[NamedReference]))
      case _ =>
        None
    }
}

/**
 * Copied from OSS Spark. We'll remove this when we integrate with OSS Spark's CLUSTER BY.
 * Convenience extractor for any Transform.
 */
private object NamedTransform {
  def unapply(transform: Transform): Some[(String, Seq[Expression])] = {
    Some((transform.name, transform.arguments))
  }
}
