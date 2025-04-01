/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.clustering

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.utils.DataFileStatus

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

class ClusteringUtilsSuite extends AnyFunSuite with Matchers {

  test("validateDataFileStatus: Throws when statistics are missing entirely") {
    val dataFile = new DataFileStatus("path", 100L, 123456L, Optional.empty())
    val clusteringCols = List(new Column("a"))

    val ex = intercept[KernelException] {
      ClusteringUtils.validateDataFileStatus(clusteringCols.asJava, dataFile)
    }

    assert(ex.getMessage.contains(
      "Cannot write to a clustering-enabled table without per-file statistics."))
  }

  test("validateDataFileStatus: Throws when min/max/nullCount is missing for a clustering column") {
    val stats = new DataFileStatistics(
      1L,
      Map.empty[Column, Literal].asJava,
      Map.empty[Column, Literal].asJava,
      Map.empty[Column, java.lang.Long].asJava)
    val dataFile = new DataFileStatus("path", 100L, 123456L, Optional.of(stats))
    val clusteringCols = List(new Column("a"))

    val ex = intercept[KernelException] {
      ClusteringUtils.validateDataFileStatus(clusteringCols.asJava, dataFile)
    }

    assert(ex.getMessage.contains(
      "Cannot write to a clustering-enabled table without per-column statistics."))
  }

  test("validateDataFileStatus: Throws when min/max is missing and nullCount != numRecords " +
    "for a clustering column") {
    val col = new Column("a")
    val stats = new DataFileStatistics(
      10L,
      Map.empty[Column, Literal].asJava,
      Map.empty[Column, Literal].asJava,
      Map(col -> java.lang.Long.valueOf(5L)).asJava)
    val dataFile = new DataFileStatus("path", 100L, 123456L, Optional.of(stats))

    val ex = intercept[KernelException] {
      ClusteringUtils.validateDataFileStatus(List(col).asJava, dataFile)
    }

    assert(ex.getMessage.contains(
      "Cannot write to a clustering-enabled table without per-column statistics."))
  }

  test("validateDataFileStatus: Passes when min/max is missing but nullCount == numRecords " +
    "for a clustering column") {
    val col = new Column("a")
    val stats = new DataFileStatistics(
      10L,
      Map.empty[Column, Literal].asJava,
      Map.empty[Column, Literal].asJava,
      Map(col -> java.lang.Long.valueOf(10L)).asJava)
    val dataFile = new DataFileStatus("path", 100L, 123456L, Optional.of(stats))

    noException should be thrownBy {
      ClusteringUtils.validateDataFileStatus(List(col).asJava, dataFile)
    }
  }

  test("validateDataFileStatus: Passes when all stats exist for clustering column") {
    val col = new Column("a")
    val stats = new DataFileStatistics(
      100L,
      Map(col -> Literal.ofInt(1)).asJava,
      Map(col -> Literal.ofInt(10)).asJava,
      Map(col -> java.lang.Long.valueOf(0L)).asJava)
    val dataFile = new DataFileStatus("path", 100L, 123456L, Optional.of(stats))
    noException should be thrownBy {
      ClusteringUtils.validateDataFileStatus(List(col).asJava, dataFile)
    }
  }
}
