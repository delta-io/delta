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

package org.apache.spark.sql.delta.util

import java.io.{File, IOException}
import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files

import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.deletionvectors.{PortableRoaringBitmapArraySerializationFormat, RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils

import org.apache.spark.sql.{Column, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

class BitmapAggregatorE2ESuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLTestUtils {

  import BitmapAggregatorE2ESuite._
  import testImplicits._
  import org.apache.spark.sql.functions._

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test(s"DataFrame bitmap groupBy aggregate no duplicates - $serializationFormat") {
      dataFrameBitmapGroupByAggregateWithoutDuplicates(format = serializationFormat)
    }
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test("DataFrame bitmap groupBy aggregate no duplicates - invalid Int ids" +
        s" - $serializationFormat") {
      dataFrameBitmapGroupByAggregateWithoutDuplicates(
        offset = INVALID_INT_OFFSET,
        format = serializationFormat)
    }
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test("DataFrame bitmap groupBy aggregate no duplicates - invalid unsigned Int ids" +
        s" - $serializationFormat") {
      dataFrameBitmapGroupByAggregateWithoutDuplicates(
        offset = UNSIGNED_INT_OFFSET,
        format = serializationFormat)
    }
  }

  private def dataFrameBitmapGroupByAggregateWithoutDuplicates(
      offset: Long = 0L,
      format: RoaringBitmapArrayFormat.Value): Unit = {
    val baseDF = spark
      .range(DATASET_SIZE)
      .map { id =>
        val newId = id + offset
        // put 2 adjacent and one with gap
        (newId % 6) match {
          case 0 | 1 | 4 => ("file1" -> newId)
          case 2 | 3 | 5 => ("file2" -> newId)
        }
      }
      .toDF("file", "id")
      .cache()

    val bitmapAgg = bitmapAggColumn(baseDF("id"), format)
    val aggregationOutput = baseDF
      .groupBy("file")
      .agg(bitmapAgg)
      .as[(String, (Long, Long, Array[Byte]))]
      .collect()
      .toMap
      .mapValues(v => RoaringBitmapArray.readFrom(v._3))

    val dfFile1 = baseDF
      .select("id")
      .where("file = 'file1'")
      .as[Long]
      .collect()
    val dfFile2 = baseDF
      .select("id")
      .where("file = 'file2'")
      .as[Long]
      .collect()

    assertEqualContents(aggregationOutput("file1"), dfFile1)
    assertEqualContents(aggregationOutput("file2"), dfFile2)
    baseDF.unpersist()
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test("DataFrame bitmap groupBy aggregate with duplicates" +
        s" - $serializationFormat") {
      dataFrameBitmapGroupAggregateWithDuplicates(format = serializationFormat)
    }
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test("DataFrame bitmap groupBy aggregate with duplicates - invalid Int ids" +
        s" - $serializationFormat") {
      dataFrameBitmapGroupAggregateWithDuplicates(
        offset = INVALID_INT_OFFSET,
        format = serializationFormat)
    }
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test("DataFrame bitmap groupBy aggregate with duplicates - invalid unsigned Int ids" +
        s" - $serializationFormat") {
      dataFrameBitmapGroupAggregateWithDuplicates(
        offset = UNSIGNED_INT_OFFSET,
        format = serializationFormat)
    }
  }

  def dataFrameBitmapGroupAggregateWithDuplicates(
      offset: Long = 0L,
      format: RoaringBitmapArrayFormat.Value) {
    val baseDF = spark
      .range(DATASET_SIZE)
      .flatMap { id =>
        val newId = id + offset
        // put two adjacent and duplicate the one after a gap
        (newId % 6) match {
          case 0 | 1 => Seq("file1" -> newId)
          case 2 | 3 => Seq("file2" -> newId)
          case 4 => Seq("file1" -> newId, "file1" -> newId) // duplicate in file1
          case 5 => Seq("file2" -> newId, "file2" -> newId) // duplicate in file2
        }
      }
      .toDF("file", "id")
      .cache()

    val bitmapAgg = bitmapAggColumn(baseDF("id"), format)
    // scalastyle:off countstring
    val aggregationOutput = baseDF
      .groupBy("file")
      .agg(bitmapAgg, count("id"))
      .as[(String, (Long, Long, Array[Byte]), Long)]
      .collect()
      .map(t => (t._1 -> (RoaringBitmapArray.readFrom(t._2._3), t._3)))
      .toMap
    // scalastyle:on countstring

    val dfFile1 = baseDF
      .select("id")
      .where("file = 'file1'")
      .distinct()
      .as[Long]
      .collect()
    val dfFile2 = baseDF
      .select("id")
      .where("file = 'file2'")
      .distinct()
      .as[Long]
      .collect()

    val file1Value = aggregationOutput("file1")
    assert(file1Value._2 > file1Value._1.cardinality)
    val file2Value = aggregationOutput("file2")
    assert(file2Value._2 > file2Value._1.cardinality)

    assertEqualContents(file1Value._1, dfFile1)
    assertEqualContents(file2Value._1, dfFile2)
  }

  // modulo ordering
  private def assertEqualContents(aggregator: RoaringBitmapArray, dataset: Array[Long]): Unit = {
    // make sure they are in the same order
    val aggregatorArray = aggregator.values.sorted
    assert(aggregatorArray === dataset.sorted)
  }
}

object BitmapAggregatorE2ESuite {
  // Pick something large enough hat 2 files have at least 64k entries each
  final val DATASET_SIZE: Long = 1000000L

  // Cross the `isValidInt` threshold
  final val INVALID_INT_OFFSET: Long = Int.MaxValue.toLong - DATASET_SIZE / 2

  // Cross the 32bit threshold
  final val UNSIGNED_INT_OFFSET: Long = (1L << 32) - DATASET_SIZE / 2

  private[delta] def bitmapAggColumn(
      column: Column,
      format: RoaringBitmapArrayFormat.Value): Column = {
    val func = new BitmapAggregator(column.expr, format);
    new Column(func.toAggregateExpression(isDistinct = false))
  }
}
