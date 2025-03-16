package io.delta.kernel.expressions.internal.skipping

import io.delta.kernel.expressions.{And, CollatedPredicate, Column, Expression, Literal, Or, Predicate}
import io.delta.kernel.internal.skipping.{CollatedDataSkippingPredicate, DataSkippingPredicate, IDataSkippingPredicate}
import io.delta.kernel.internal.skipping.DataSkippingUtils.constructDataSkippingFilter
import io.delta.kernel.types.{CollationIdentifier, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import java.util.Optional
import scala.collection.JavaConverters.{mapAsJavaMapConverter, seqAsJavaListConverter, setAsJavaSetConverter}

class DataSkippingUtilsSuite extends AnyFunSuite {
  val MIN = "minValues"
  val MAX = "maxValues"
  val NULL_COUNT = "nullCount"
  val NUM_RECORDS = "numRecords"
  val STATS_WITH_COLLATION = "statsWithCollation"
  val dataSkippingPredicateConstructor =
    classOf[DataSkippingPredicate].getDeclaredConstructor(
      classOf[String],
      classOf[util.List[Expression]],
      classOf[util.Set[Column]],
      classOf[util.Map[CollationIdentifier, util.Set[Column]]])
  val collatedDataSkippingPredicateConstructor =
    classOf[CollatedDataSkippingPredicate].getDeclaredConstructor(
      classOf[String],
      classOf[util.List[Expression]],
      classOf[CollationIdentifier],
      classOf[util.Set[Column]],
      classOf[util.Map[CollationIdentifier, util.Set[Column]]])
  dataSkippingPredicateConstructor.setAccessible(true)
  collatedDataSkippingPredicateConstructor.setAccessible(true)

  test("check constructDataSkippingFilter") {
    Seq(
      // (schema, predicate, expectedDataSkippingPredicate)
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", column("a"), column("b")),
        Optional.empty
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            "<",
            List(column(MIN, "a"), Literal.ofString("b")),
            Set(column(MIN, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", Literal.ofString("b"), column("a")),
        Optional.of(
          dataSkippingPredicate(
            ">",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<=", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            "<=",
            List(column(MIN, "a"), Literal.ofString("b")),
            Set(column(MIN, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator(">", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            ">",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator(">=", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            ">=",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("=", column("a"), Literal.ofString("b")),
        Optional.of(
          dataSkippingPredicate(
            "AND",
            List(dataSkippingPredicate(
              "<=",
              List(column(MIN, "a"), Literal.ofString("b")),
              Set(column(MIN, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            ),
            dataSkippingPredicate(
              ">=",
              List(column(MAX, "a"), Literal.ofString("b")),
              Set(column(MAX, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            )),
            Set(column(MIN, "a"), column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("IS NOT DISTINCT FROM", Literal.ofString("b"), column("a")),
        Optional.of(
          dataSkippingPredicate(
            "AND",
            List(dataSkippingPredicate(
              "<",
              List(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
              Set(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
              Map.empty[CollationIdentifier, Set[Column]]),
            dataSkippingPredicate(
              "AND",
              List(dataSkippingPredicate(
                "<=",
                List(column(MIN, "a"), Literal.ofString("b")),
                Set(column(MIN, "a")),
                Map.empty[CollationIdentifier, Set[Column]]
              ),
              dataSkippingPredicate(
                ">=",
                List(column(MAX, "a"), Literal.ofString("b")),
                Set(column(MAX, "a")),
                Map.empty[CollationIdentifier, Set[Column]]
              )),
              Set(column(MIN, "a"), column(MAX, "a")),
              Map.empty[CollationIdentifier, Set[Column]])),
            Set(column(NULL_COUNT, "a"), column(NUM_RECORDS), column(MIN, "a"), column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        and(
          comparator("<", column("a"), column("b")),
          comparator(">", column("a"), Literal.ofString("b"))
        ),
        Optional.of(
          dataSkippingPredicate(
            ">",
            List(column(MAX, "a"), Literal.ofString("b")),
            Set(column(MAX, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        or(
          comparator("<", column("a"), column("b")),
          comparator(">", column("a"), Literal.ofString("b"))
        ),
        Optional.empty
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        new Predicate("IS_NULL", List[Expression](column("a")).asJava),
        Optional.of(
          dataSkippingPredicate(">", List(column(NULL_COUNT, "a"), Literal.ofInt(0)),
            Set(column(NULL_COUNT, "a")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        new Predicate("IS_NOT_NULL", List[Expression](column("a")).asJava),
        Optional.of(
          dataSkippingPredicate("<", List(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
            Set(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        and(
          comparator("<", column("a"), Literal.ofString("c")),
          comparator(">", Literal.ofString("c"), column("b"))),
        Optional.of(
          dataSkippingPredicate(
            "AND",
            List(dataSkippingPredicate(
              "<",
              List(column(MIN, "a"), Literal.ofString("c")),
              Set(column(MIN, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            ),
            dataSkippingPredicate(
              "<",
              List(column(MIN, "b"), Literal.ofString("c")),
              Set(column(MIN, "b")),
              Map.empty[CollationIdentifier, Set[Column]]
            )),
            Set(column(MIN, "a"), column(MIN, "b")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a", StringType.STRING)
          .add("b", StringType.STRING),
        or(
          comparator("<", column("a"), Literal.ofString("c")),
          comparator(">", Literal.ofString("c"), column("b"))),
        Optional.of(
          dataSkippingPredicate(
            "OR",
            List(dataSkippingPredicate(
              "<",
              List(column(MIN, "a"), Literal.ofString("c")),
              Set(column(MIN, "a")),
              Map.empty[CollationIdentifier, Set[Column]]
            ),
              dataSkippingPredicate(
                "<",
                List(column(MIN, "b"), Literal.ofString("c")),
                Set(column(MIN, "b")),
                Map.empty[CollationIdentifier, Set[Column]]
              )),
            Set(column(MIN, "a"), column(MIN, "b")),
            Map.empty[CollationIdentifier, Set[Column]]))
      ),
      (
        new StructType()
          .add("a1", new StructType()
            .add("a2", StringType.STRING)),
        comparator("<", column("a1"), Literal.ofString("b")),
        Optional.empty
      ),
      (
        new StructType()
          .add("a", StringType.STRING),
        comparator("<", column("a"), Literal.ofBinary(Array(1.toByte, 2.toByte))),
        Optional.empty
      )
    ).foreach {
      case (schema, predicate, expectedDataSkippingPredicate) =>
        checkResult(
          schema,
          predicate,
          expectedDataSkippingPredicate.asInstanceOf[Optional[IDataSkippingPredicate]])
    }
  }

  test("check constructDataSkippingFilter with CollatedPredicate") {
    val UTF8_BINARY = StringType.STRING.getCollationIdentifier
    val UTF8_LCASE = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
    val SR_CYRL = CollationIdentifier.fromString("ICU.sr_Cyrl_SRB.75.1")
    val collations = Seq(UTF8_BINARY, UTF8_LCASE, SR_CYRL)

    cross(Seq("<", "<=", ">", ">="), collations).foreach {
      case (comparatorName, collationIdentifier) =>
        for (order <- Seq(0, 1)) {
          order: Int =>
            val prefix = if (comparatorName.startsWith("<") && order == 0) MIN else MAX
            val cmp =
            if (order == 0) {
              comparator(
                comparatorName, column("a"), Literal.ofString("b"), Some(collationIdentifier))
            } else {
              comparator(
                comparatorName, Literal.ofString("b"), column("a"), Some(collationIdentifier))
            }
            checkResult(
              new StructType().add("a", StringType.STRING),
              cmp,
              Optional.of(
                collatedDataSkippingPredicate(
                  comparatorName,
                  List(
                    column(STATS_WITH_COLLATION, collationIdentifier.toString, prefix, "a"),
                    Literal.ofString("b")),
                  collationIdentifier,
                  Set(column(STATS_WITH_COLLATION, collationIdentifier.toString, prefix, "a")),
                  Map(collationIdentifier ->
                    Set(column(STATS_WITH_COLLATION, collationIdentifier.toString, prefix, "a"))))))
        }
      }

    cross(collations, collations.map(new StringType(_))).foreach {
      case (comparatorCollation, stringType) =>
        Seq(
          // (schema, predicate, expectedDataSkippingPredicate)
          (
            new StructType().add("a", stringType),
            comparator("=", column("a"), Literal.ofString("b"), Some(comparatorCollation)),
            dataSkippingPredicate(
              "AND",
              List(
                collatedDataSkippingPredicate(
                  "<=",
                  List(column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                    Literal.ofString("b")),
                  comparatorCollation,
                  Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a")),
                  Map(comparatorCollation ->
                    Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a")))),
                collatedDataSkippingPredicate(
                  ">=",
                  List(column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a"),
                    Literal.ofString("b")),
                  comparatorCollation,
                  Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a")),
                  Map(comparatorCollation ->
                    Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a"))))),
              Set(
                column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a")),
              Map(comparatorCollation ->
                Set(
                  column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                  column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a"))))
          ),
          (
            new StructType().add("a", stringType),
            comparator(
              "IS NOT DISTINCT FROM",
              Literal.ofString("b"),
              column("a"),
              Some(comparatorCollation)),
            dataSkippingPredicate(
              "AND",
              List(
                dataSkippingPredicate(
                  "<",
                  List(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
                  Set(column(NULL_COUNT, "a"), column(NUM_RECORDS)),
                  Map.empty[CollationIdentifier, Set[Column]]),
                dataSkippingPredicate(
                  "AND",
                  List(
                    collatedDataSkippingPredicate(
                      "<=",
                      List(
                        column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                        Literal.ofString("b")),
                      comparatorCollation,
                      Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a")),
                      Map(comparatorCollation ->
                        Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a")))),
                    collatedDataSkippingPredicate(
                      ">=",
                      List(
                        column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a"),
                        Literal.ofString("b")),
                      comparatorCollation,
                      Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a")),
                      Map(comparatorCollation ->
                        Set(column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a")))
                    )),
                  Set(
                    column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                    column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a")),
                  Map(comparatorCollation ->
                    Set(
                      column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                      column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a"))))),
                Set(
                  column(NULL_COUNT, "a"),
                  column(NUM_RECORDS),
                  column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                  column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a")),
                Map(comparatorCollation ->
                  Set(
                    column(STATS_WITH_COLLATION, comparatorCollation.toString, MIN, "a"),
                    column(STATS_WITH_COLLATION, comparatorCollation.toString, MAX, "a"))))
          )
        ).foreach {
          case (schema, predicate, expectedDataSkippingPredicate) =>
            checkResult(
              schema,
              predicate,
              Optional.of(expectedDataSkippingPredicate))
        }
      }

    def cross[X, Y](x: Seq[X], y: Seq[Y]): Seq[(X, Y)] = {
      for { i <- x ; j <- y } yield (i, j)
    }
  }

  def checkResult(
                   schema: StructType,
                   predicate: Predicate,
                   expectedDataSkippingPredicate: Optional[IDataSkippingPredicate]): Unit = {
    val actualDataSkippingPredicate = constructDataSkippingFilter(predicate, schema)
    assert(actualDataSkippingPredicate.isPresent == expectedDataSkippingPredicate.isPresent)
    if (actualDataSkippingPredicate.isPresent) {
      assert(actualDataSkippingPredicate.get.toString
        == expectedDataSkippingPredicate.get.toString)
      assert(actualDataSkippingPredicate.get.getReferencedCols
        == expectedDataSkippingPredicate.get.getReferencedCols)
      assert(actualDataSkippingPredicate.get.getReferencedCollatedCols
        == expectedDataSkippingPredicate.get.getReferencedCollatedCols)
    }
  }

  def and(left: Predicate, right: Predicate): And = {
    new And(left, right)
  }

  def or(left: Predicate, right: Predicate): Or = {
    new Or(left, right)
  }

  def column(name: String*): Column = {
    new Column(name.toArray)
  }

  def comparator(
                            symbol: String, left: Expression,
                            right: Expression,
                            collationIdentifier: Option[CollationIdentifier] = None): Predicate = {
    if (collationIdentifier.isDefined && collationIdentifier.get != null) {
      new CollatedPredicate(symbol, left, right, collationIdentifier.get)
    } else {
      new Predicate(symbol, left, right)
    }
  }

  def dataSkippingPredicate(
                             predicateName: String,
                             children: List[Expression],
                             columns: Set[Column],
                             collationMap: Map[CollationIdentifier, Set[Column]])
  : DataSkippingPredicate = {
    dataSkippingPredicateConstructor.newInstance(
      predicateName, children.asJava, columns.asJava, collationMap.mapValues(_.asJava).asJava)
  }

  def collatedDataSkippingPredicate(
                                     predicateName: String,
                                     children: List[Expression],
                                     collationIdentifier: CollationIdentifier,
                                     columns: Set[Column],
                                     collationMap: Map[CollationIdentifier, Set[Column]])
  : CollatedDataSkippingPredicate = {
    collatedDataSkippingPredicateConstructor.newInstance(
      predicateName,
      children.asJava,
      collationIdentifier,
      columns.asJava,
      collationMap.mapValues(_.asJava).asJava)
  }
}
