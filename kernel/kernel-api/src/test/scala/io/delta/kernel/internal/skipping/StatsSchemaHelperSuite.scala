/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.skipping

import io.delta.kernel.expressions.Column
import io.delta.kernel.types.{CollationIdentifier, IntegerType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.Collections

class StatsSchemaHelperSuite extends AnyFunSuite {
  test("check appendCollatedStatsSchema") {
    val STATS_WITH_COLLATION = "statsWithCollation"
    val defaultCollationIdentifier = CollationIdentifier.fromString("SPARK.UTF8_BINARY")
    val MIN = "minValues"
    val MAX = "maxValues"

    Seq(
      // (startingDataSchema, referencedCollatedCols, finalDataSchema)
      (
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false), true),
        new util.HashMap[CollationIdentifier, util.Set[Column]](),
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false), true)
      ),
      (
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false), true),
        new util.HashMap[CollationIdentifier, util.Set[Column]]() {
          {
            put(
              defaultCollationIdentifier,
              Collections.singleton(new Column(Array("a3", "b1"))))
          }
        },
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false), true)
          .add(STATS_WITH_COLLATION, new StructType()
            .add(defaultCollationIdentifier.toString, new StructType()
              .add(MIN, new StructType()
                .add("a3", new StructType()
                  .add("b1", StringType.STRING, true), true), true)
              .add(MAX, new StructType()
                .add("a3", new StructType()
                  .add("b1", StringType.STRING, true), true), true), true), true)
      ),
      (
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false), true),
        new util.HashMap[CollationIdentifier, util.Set[Column]]() {
          {
            put(
              defaultCollationIdentifier,
              scala.collection.JavaConverters.setAsJavaSet(
                Set(new Column(Array("a3", "b1")), new Column("a1"))))
          }
        },
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false), true)
          .add(STATS_WITH_COLLATION, new StructType()
            .add(defaultCollationIdentifier.toString, new StructType()
              .add(MIN, new StructType()
                .add("a1", StringType.STRING, true)
                .add("a3", new StructType()
                  .add("b1", StringType.STRING, true), true), true)
              .add(MAX, new StructType()
                .add("a1", StringType.STRING, true)
                .add("a3", new StructType()
                  .add("b1", StringType.STRING, true), true), true), true), true)
      ),
      (
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false)
            .add("b2", StringType.STRING, false), true),
        new util.HashMap[CollationIdentifier, util.Set[Column]]() {
          {
            put(
              defaultCollationIdentifier,
              scala.collection.JavaConverters.setAsJavaSet(
                Set(new Column(Array("a3", "b1")), new Column("a1"))))
            put(CollationIdentifier.fromString("SPARK.UTF8_LCASE"),
              Collections.singleton(new Column(Array("a3", "b2"))))
          }
        },
        new StructType()
          .add("a1", StringType.STRING, true)
          .add("a2", IntegerType.INTEGER, false)
          .add("a3", new StructType()
            .add("b1", StringType.STRING, false)
            .add("b2", StringType.STRING, false), true)
          .add(STATS_WITH_COLLATION, new StructType()
            .add("SPARK.UTF8_LCASE", new StructType()
              .add(MIN, new StructType()
                .add("a3", new StructType()
                  .add("b2", StringType.STRING, true), true), true)
              .add(MAX, new StructType()
                .add("a3", new StructType()
                  .add("b2", StringType.STRING, true), true), true), true)
            .add(defaultCollationIdentifier.toString, new StructType()
              .add(MIN, new StructType()
                .add("a1", StringType.STRING, true)
                .add("a3", new StructType()
                  .add("b1", StringType.STRING, true), true), true)
              .add(MAX, new StructType()
                .add("a1", StringType.STRING, true)
                .add("a3", new StructType()
                  .add("b1", StringType.STRING, true), true), true), true), true)
      )
    ).foreach {
      case (startingDataSchema, referencedCollatedCols, finalDataSchema) =>
        assert(finalDataSchema.equals(StatsSchemaHelper
          .appendCollatedStatsSchema(startingDataSchema, referencedCollatedCols)))
    }
  }
}
