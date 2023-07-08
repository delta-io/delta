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

package org.apache.spark.sql.delta.expressions.aggregation

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.types.LongType

class BitmapAggregatorSuite extends SparkFunSuite {

  import BitmapAggregatorSuite._

  private val childExpression = BoundReference(0, LongType, nullable = true)

  /** Creates a bitmap aggregate expression, using the child expression defined above. */
  private def newBitmapAgg(format: RoaringBitmapArrayFormat.Value): BitmapAggregator =
    new BitmapAggregator(childExpression, format)

  for (serializationFormat <- RoaringBitmapArrayFormat.values)
  test(s"Bitmap serialization - $serializationFormat") {
    val bitmapSet = fillSetWithAggregator(newBitmapAgg(serializationFormat), Array(1L, 2L, 3L, 4L))
    val serialized = bitmapSet.serializeAsByteArray(serializationFormat)
    val deserialized = RoaringBitmapArray.readFrom(serialized)
    assert(bitmapSet === deserialized)
    assert(bitmapSet.## === deserialized.##)
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values)
  test(s"Aggregator serialization - $serializationFormat") {
    val aggregator = newBitmapAgg(serializationFormat)
    val bitmapSet = fillSetWithAggregator(aggregator, Array(1L, 2L, 3L, 4L))
    val deserialized = aggregator.deserialize(aggregator.serialize(bitmapSet))
    assert(bitmapSet === deserialized)
    assert(bitmapSet.## === deserialized.##)
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values)
  test(s"Bitmap Aggregator merge no duplicates - $serializationFormat") {
    val (dataset1, dataset2) = createDatasetsNoDuplicates

    val finalResult =
      fillSetWithAggregatorAndMerge(
        newBitmapAgg(serializationFormat),
        dataset1,
        dataset2)

    verifyContainsAll(finalResult, dataset1)
    verifyContainsAll(finalResult, dataset2)
  }

  for (serializationFormat <- RoaringBitmapArrayFormat.values)
  test(s"Bitmap Aggregator with duplicates - $serializationFormat") {
    val (dataset1, dataset2) = createDatasetsWithDuplicates

    val finalResult =
      fillSetWithAggregatorAndMerge(
        newBitmapAgg(serializationFormat),
        dataset1,
        dataset2)

    verifyContainsAll(finalResult, dataset1)
    verifyContainsAll(finalResult, dataset2)
  }

  private lazy val createDatasetsNoDuplicates: (List[Long], List[Long]) = {
    val primeSet = primes(DATASET_SIZE).toSet
    val notPrime = (0 until DATASET_SIZE).filterNot(primeSet.contains).toList
    (primeSet.map(_.toLong).toList, notPrime.map(_.toLong))
  }

  private def createDatasetsWithDuplicates: (List[Long], List[Long]) = {
    var (primes, notPrimes) = createDatasetsNoDuplicates
    // duplicate all powers of 3 (powers of 2 might align with container boundaries)
    notPrimes ::= 3L
    var value = 3L
    while (value < DATASET_SIZE.toLong) {
      value *= 3L
      primes ::= value
    }
    (primes, notPrimes)
  }

  // List the first primes smaller than `end`
  private def primes(end: Int): List[Int] = {
    // scalastyle:off
    // Basically https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes#Algorithm_and_variants
    // but concretely the implementation is adapted from:
    // https://medium.com/coding-with-clarity/functional-vs-iterative-prime-numbers-in-scala-7e22447146f0
    // scalastyle:on
    val primeIndices = mutable.ArrayBuffer.fill((end + 1) / 2)(true)

    val intSqrt = Math.sqrt(end).toInt
    for {
      i <- 3 to end by 2 if i <= intSqrt
      nonPrime <- i * i to end by 2 * i
    } primeIndices.update(nonPrime / 2, false)


    (for (i <- primeIndices.indices if primeIndices(i)) yield 2 * i + 1).tail.toList
  }

  private def fillSetWithAggregatorAndMerge(
    aggregator: BitmapAggregator,
    dataset1: Seq[Long],
    dataset2: Seq[Long]): RoaringBitmapArray = {
    val buffer1 = fillSetWithAggregator(aggregator, dataset1)
    val buffer2 = fillSetWithAggregator(aggregator, dataset2)
    val merged = aggregator.merge(buffer1, buffer2)
    val fieldIndex = aggregator.dataType.fieldIndex("bitmap")
    val result = aggregator.eval(merged).getBinary(fieldIndex)
    RoaringBitmapArray.readFrom(result)
  }

  private def fillSetWithAggregator(
    aggregator: BitmapAggregator,
    dataset: Seq[Long]): RoaringBitmapArray = {
    val buffer = aggregator.createAggregationBuffer()
    for (entry <- dataset) {
      val row = InternalRow(entry)
      aggregator.update(buffer, row)
    }
    buffer
  }

  private def verifyContainsAll(
    aggregator: RoaringBitmapArray,
    dataset: Seq[Long]): Unit = {
    for (entry <- dataset) {
      assert(aggregator.contains(entry),
        s"Aggregator did not contain file $entry")
    }
  }
}

object BitmapAggregatorSuite {
  // Pick something over 64k to make sure we fill a few different bitmap containers
  val DATASET_SIZE: Int = 100000
}

