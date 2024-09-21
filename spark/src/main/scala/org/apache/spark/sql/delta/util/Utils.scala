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

import scala.util.Random

import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.actions.Metadata

import org.apache.spark.sql.Column
import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.ElementAt
import org.apache.spark.sql.functions.lit

/**
 * Various utility methods used by Delta.
 */
object Utils {

  /** Measures the time taken by function `f` */
  def timedMs[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val res = f
    val duration = System.currentTimeMillis() - start
    (res, duration)
  }

  /** Returns the length of the random prefix to use for the data files of a Delta table. */
  def getRandomPrefixLength(metadata: Metadata): Int = {
    if (DeltaConfigs.RANDOMIZE_FILE_PREFIXES.fromMetaData(metadata)) {
      DeltaConfigs.RANDOM_PREFIX_LENGTH.fromMetaData(metadata)
    } else {
      0
    }
  }

  /** Generates a string created of `randomPrefixLength` alphanumeric characters. */
  def getRandomPrefix(numChars: Int): String = {
    Random.alphanumeric.take(numChars).mkString
  }

  /**
   * Indicates whether Delta is currently running unit tests.
   */
  def isTesting: Boolean = {
    System.getenv("DELTA_TESTING") != null
  }

  /**
   * Returns value for the given key in value if column is a map and the key is present, NULL
   * otherwise.
   */
  def try_element_at(mapColumn: Column, key: Any): Column = {
    Column {
      ElementAt(mapColumn.expr, lit(key).expr, failOnError = false)
    }
  }
}
