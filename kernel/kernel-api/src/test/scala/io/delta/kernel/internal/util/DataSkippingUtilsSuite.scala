/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util

import scala.collection.JavaConverters._
import io.delta.kernel.expressions.{AlwaysTrue, And, CollatedPredicate, CollationIdentifier, Column, Literal, Or, Predicate}
import io.delta.kernel.internal.skipping.DataSkippingUtils
import io.delta.kernel.internal.skipping.DataSkippingUtils.omitCollatedPredicateFromDataSkippingFilter
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.{DataType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class DataSkippingUtilsSuite extends AnyFunSuite {

  def col(name: String): Column = new Column(name)

  def nestedCol(name: String): Column = {
    new Column(name.split("\\."))
  }

  /* For struct type checks for equality based on field names & data type only */
  def compareDataTypeUnordered(type1: DataType, type2: DataType): Boolean = (type1, type2) match {
    case (schema1: StructType, schema2: StructType) =>
      val fields1 = schema1.fields().asScala.sortBy(_.getName)
      val fields2 = schema2.fields().asScala.sortBy(_.getName)
      if (fields1.length != fields2.length) {
        false
      } else {
        fields1.zip(fields2).forall { case (field1: StructField, field2: StructField) =>
          field1.getName == field2.getName &&
            compareDataTypeUnordered(field1.getDataType, field2.getDataType)
        }
      }
    case _ =>
      type1 == type2
  }

  def checkPruneStatsSchema(
    inputSchema: StructType, referencedCols: Set[Column], expectedSchema: StructType): Unit = {
    val prunedSchema = DataSkippingUtils.pruneStatsSchema(inputSchema, referencedCols.asJava)
    assert(compareDataTypeUnordered(expectedSchema, prunedSchema),
      s"expected=$expectedSchema\nfound=$prunedSchema")
  }

  test("omitCollatedPredicateFromDataSkippingFilter - AND, OR") {
    Seq(
      // (starting predicate, resulting predicate)
      (
        new And(
          new Predicate("=", new Column("c1"), new Column("c2")),
          new Predicate(">", Literal.ofString("a"), new Column("c1"))
        ),
        new And(
          new Predicate("=", new Column("c1"), new Column("c2")),
          new Predicate(">", Literal.ofString("a"), new Column("c1"))
        )
      ),
      (
        new And(
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
          new Predicate("=", new Column("c1"), Literal.ofString("a"))
        ),
        new Predicate("=", new Column("c1"), Literal.ofString("a"))
      ),
      (
        new And(
          new Predicate("=", new Column("c1"), Literal.ofString("a")),
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
        ),
        new Predicate("=", new Column("c1"), Literal.ofString("a"))
      ),
      (
        new And(
          new CollatedPredicate("=", new Column("c1"), new Column("c2"),
            CollationIdentifier.fromString("ICU.UNICODE")),
          new CollatedPredicate(">", new Column("c2"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Or(
          new Predicate("=", new Column("c1"), new Column("c2")),
          new Predicate(">", Literal.ofString("a"), new Column("c1"))
        ),
        new Or(
          new Predicate("=", new Column("c1"), new Column("c2")),
          new Predicate(">", Literal.ofString("a"), new Column("c1"))
        )
      ),
      (
        new Or(
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
          new Predicate("=", new Column("c1"), Literal.ofString("a"))
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Or(
          new Predicate("=", new Column("c1"), Literal.ofString("a")),
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Or(
          new CollatedPredicate("=", new Column("c1"), new Column("c2"),
            CollationIdentifier.fromString("ICU.UNICODE")),
          new CollatedPredicate(">", new Column("c2"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
        ),
        AlwaysTrue.ALWAYS_TRUE
      )
    ).foreach {
      case(startingPredicate, resultingPredicate) =>
        assert(omitCollatedPredicateFromDataSkippingFilter(startingPredicate).toString
          === resultingPredicate.toString)
    }
  }

  test("omitCollatedPredicateFromDataSkippingFilter - =") {
    Seq(
      // (starting predicate, resulting predicate)
      (
        new Predicate("=", Literal.ofString("a"), new Column("c1")),
        new Predicate("=", Literal.ofString("a"), new Column("c1")),
      ),
      (
        new Predicate("=",
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
          Literal.ofBoolean(true)
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Predicate("=",
          Literal.ofBoolean(true),
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Predicate("=",
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
          new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
            CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Predicate("=",
          new And(
            new CollatedPredicate("=", new Column("c1"), Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
            new Predicate("=", new Column("a"), Literal.ofString("a"))
          ),
          Literal.ofBoolean(true)
        ),
        AlwaysTrue.ALWAYS_TRUE
      )
    ).foreach {
      case(startingPredicate, resultingPredicate) =>
        assert(omitCollatedPredicateFromDataSkippingFilter(startingPredicate).toString
          === resultingPredicate.toString)
    }
  }

  test("omitCollatedPredicateFromDataSkippingFilter - >, >=") {
    Seq(">", ">=")
      .foreach {
        name =>
          Seq(
            // (starting predicate, resulting predicate)
            (
              new Predicate(name,
                Literal.ofString("a"),
                new Column("c1")
              ),
              new Predicate(name,
                Literal.ofString("a"),
                new Column("c1")
              )
            ),
            (
              new Predicate(name,
                new CollatedPredicate("=",
                  Literal.ofString("a"),
                  new Column("c1"),
                  CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
                ),
                Literal.ofBoolean(true)
              ),
              new Predicate(name,
                AlwaysTrue.ALWAYS_TRUE,
                Literal.ofBoolean(true)
              )
            ),
            (
              new Predicate(name,
                Literal.ofBoolean(true),
                new CollatedPredicate("=",
                  Literal.ofString("a"),
                  new Column("c1"),
                  CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
                )
              ),
              AlwaysTrue.ALWAYS_TRUE
            )
          ).foreach {
            case(startingPredicate, resultingPredicate) =>
              assert(omitCollatedPredicateFromDataSkippingFilter(startingPredicate).toString
                === resultingPredicate.toString)
          }
      }
  }

  test("omitCollatedPredicateFromDataSkippingFilter - <, <=") {
    Seq("<", "<=")
      .foreach {
        name =>
          Seq(
            // (starting predicate, resulting predicate)
            (
              new Predicate(name,
                Literal.ofString("a"),
                new Column("c1")
              ),
              new Predicate(name,
                Literal.ofString("a"),
                new Column("c1")
              )
            ),
            (
              new Predicate(name,
                Literal.ofBoolean(true),
                new CollatedPredicate("=",
                  Literal.ofString("a"),
                  new Column("c1"),
                  CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
                )
              ),
              new Predicate(name,
                Literal.ofBoolean(true),
                AlwaysTrue.ALWAYS_TRUE
              )
            ),
            (
              new Predicate(name,
                new CollatedPredicate("=",
                  Literal.ofString("a"),
                  new Column("c1"),
                  CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
                ),
                Literal.ofBoolean(true)
              ),
              AlwaysTrue.ALWAYS_TRUE
            )
          ).foreach {
            case(startingPredicate, resultingPredicate) =>
              assert(omitCollatedPredicateFromDataSkippingFilter(startingPredicate).toString
                === resultingPredicate.toString)
          }
      }
  }

  test("omitCollatedPredicateFromDataSkippingFilter - NOT") {
    Seq(
      // (starting predicate, resulting predicate)
      (
        new Predicate("NOT",
          new And(
            new Predicate("=",
              new Column("c1"), Literal.ofString("a")
            ),
            new Predicate(">",
              new Column("c2"), new Column("c1")
            )
          )
        ),
        new Or(
          new Or(
            new Predicate("<",
              new Column("c1"), Literal.ofString("a")
            ),
            new Predicate("<",
              Literal.ofString("a"), new Column("c1"))
          ),
          new Predicate("<=",
            new Column("c2"), new Column("c1")
          )
        )
      ),
      (
        new Predicate("NOT",
          new Or(
            new Predicate("=",
              new Column("c1"), Literal.ofString("a")
            ),
            new Predicate(">",
              new Column("c2"), new Column("c1")
            )
          )
        ),
        new And(
          new Or(
            new Predicate("<",
              new Column("c1"), Literal.ofString("a")
            ),
            new Predicate("<",
              Literal.ofString("a"), new Column("c1"))
          ),
          new Predicate("<=",
            new Column("c2"), new Column("c1")
          )
        )
      ),
      (
        new Predicate("NOT",
          new Predicate("=",
            Literal.ofString("a"),
            new Column("c1"))
        ),
        new Or(
          new Predicate("<",
            Literal.ofString("a"),
            new Column("c1")
          ),
          new Predicate("<",
            new Column("c1"),
            Literal.ofString("a"))
        )
      ),
      (
        new Predicate("NOT",
          new Predicate("=",
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
            ),
            Literal.ofBoolean(true)
          )
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Predicate("NOT",
          new Predicate("<",
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
            Literal.ofBoolean(true)
          )
        ),
        new Predicate(">=",
          AlwaysTrue.ALWAYS_TRUE,
          Literal.ofBoolean(true)
        )
      ),
      (
        new Predicate("NOT",
          new Predicate("<",
            Literal.ofBoolean(true),
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
          )
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Predicate("NOT",
          new Predicate("<=",
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
            Literal.ofBoolean(true)
          )
        ),
        new Predicate(">",
          AlwaysTrue.ALWAYS_TRUE,
          Literal.ofBoolean(true)
        )
      ),
      (
        new Predicate("NOT",
          new Predicate(">",
            Literal.ofBoolean(true),
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
          )
        ),
        new Predicate("<=",
          Literal.ofBoolean(true),
          AlwaysTrue.ALWAYS_TRUE)
      ),
      (
        new Predicate("NOT",
          new Predicate(">",
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER),
            Literal.ofBoolean(true)
          )
        ),
        AlwaysTrue.ALWAYS_TRUE
      ),
      (
        new Predicate("NOT",
          new Predicate(">=",
            Literal.ofBoolean(true),
            new CollatedPredicate("<",
              new Column("c1"),
              Literal.ofString("a"),
              CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)
          )
        ),
        new Predicate("<",
          Literal.ofBoolean(true),
          AlwaysTrue.ALWAYS_TRUE)
      )
    ).foreach {
      case(startingPredicate, resultingPredicate) =>
        assert(omitCollatedPredicateFromDataSkippingFilter(startingPredicate).toString
          === resultingPredicate.toString)
    }

    val reverseSign = Map(
      ">" -> "<=",
      ">=" -> "<",
      "<" -> ">=",
      "<=" -> ">"
    )

    // check >, >=
    Seq(">", ">=")
      .foreach {
        name =>
          Seq(
            (
              new Predicate("NOT",
                new Predicate(name,
                  Literal.ofString("a"),
                  new Column("c1")
                )
              ),
              new Predicate(reverseSign(name),
                Literal.ofString("a"),
                new Column("c1")
              )
            ),
            (
              new Predicate("NOT",
                new Predicate(name,
                  new CollatedPredicate("=",
                    Literal.ofString("a"),
                    new Column("c1"),
                    CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
                  ),
                  Literal.ofBoolean(true)
                )
              ),
              AlwaysTrue.ALWAYS_TRUE
            ),
            (
              new Predicate("NOT",
                new Predicate(name,
                  Literal.ofBoolean(true),
                  new CollatedPredicate("=",
                    Literal.ofString("a"),
                    new Column("c1"),
                    CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER
                  )
                )
              ),
              new Predicate(reverseSign(name),
                Literal.ofBoolean(true),
                AlwaysTrue.ALWAYS_TRUE
              )
            )
          ).foreach {
            case(startingPredicate, resultingPredicate) =>
              assert(omitCollatedPredicateFromDataSkippingFilter(startingPredicate).toString
                === resultingPredicate.toString)
          }
      }
  }

  test("pruneStatsSchema - multiple basic cases one level of nesting") {
    val nestedField = new StructField(
      "nested",
      new StructType()
        .add("col1", INTEGER)
        .add("col2", INTEGER),
      true
    )
    val testSchema = new StructType()
      .add(nestedField)
      .add("top_level_col", INTEGER)
    // no columns pruned
    checkPruneStatsSchema(
      testSchema,
      Set(col("top_level_col"), nestedCol("nested.col1"), nestedCol("nested.col2")),
      testSchema
    )
    // top level column pruned
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("nested.col1"), nestedCol("nested.col2")),
      new StructType().add(nestedField)
    )
    // nested column only one field pruned
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("top_level_col"), nestedCol("nested.col1")),
      new StructType()
        .add("nested", new StructType().add("col1", INTEGER))
        .add("top_level_col", INTEGER)
    )
    // nested column completely pruned
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("top_level_col")),
      new StructType().add("top_level_col", INTEGER)
    )
    // prune all columns
    checkPruneStatsSchema(
      testSchema,
      Set(),
      new StructType()
    )
  }

  test("pruneStatsSchema - 3 levels of nesting") {
    /*
    |--level1: struct
    |   |--level2: struct
    |       |--level3: struct
    |           |--level_4_col: int
    |       |--level_3_col: int
    |   |--level_2_col: int
     */
    val testSchema = new StructType()
      .add("level1",
        new StructType()
          .add(
            "level2",
            new StructType()
              .add(
                "level3",
                new StructType().add("level_4_col", INTEGER))
              .add("level_3_col", INTEGER)
          )
          .add("level_2_col", INTEGER)
      )
    // prune only 4th level col
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("level1.level2.level_3_col"), nestedCol("level1.level_2_col")),
      new StructType()
        .add(
          "level1",
          new StructType()
            .add("level2", new StructType().add("level_3_col", INTEGER))
            .add("level_2_col", INTEGER))
    )
    // prune only 3rd level column
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("level1.level2.level3.level_4_col"), nestedCol("level1.level_2_col")),
      new StructType()
        .add("level1",
          new StructType()
            .add(
              "level2",
              new StructType()
                .add(
                  "level3",
                  new StructType().add("level_4_col", INTEGER))
            )
            .add("level_2_col", INTEGER)
        )
    )
    // prune 4th and 3rd level column
    checkPruneStatsSchema(
      testSchema,
      Set(nestedCol("level1.level_2_col")),
      new StructType()
        .add("level1",
          new StructType()
            .add("level_2_col", INTEGER)
        )
    )
    // prune all columns
    checkPruneStatsSchema(
      testSchema,
      Set(),
      new StructType()
    )
  }
}
