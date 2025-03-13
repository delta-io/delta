package io.delta.kernel.expressions.internal.skipping

import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.skipping.StatsSchemaHelper.appendCollatedStatsSchema
import io.delta.kernel.types.{CollationIdentifier, FloatType, IntegerType, StringType, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, setAsJavaSetConverter}

class StatsSchemaHelperSuite extends AnyFunSuite {
  test("check appendCollatedStatsSchema") {
    val STATS_WITH_COLLATION = "statsWithCollation"
    val MIN = "minValues"
    val MAX = "maxValues"
    val UTF8_BINARY = CollationIdentifier.fromString("SPARK.UTF8_BINARY")
    val UTF8_LCASE = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
    val UTF8_LCASE_STRING = new StringType(UTF8_LCASE)

    Seq(
      (
        new StructType()
          .add("a1", new StructType()
            .add("b1", StringType.STRING))
          .add("a2", UTF8_LCASE_STRING),
        new StructType()
          .add("a1", new StructType()
            .add("b1", StringType.STRING))
          .add("a2", UTF8_LCASE_STRING)
          .add(MIN, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING))
            .add("a2", UTF8_LCASE_STRING))
          .add(MAX, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING))
            .add("a2", UTF8_LCASE_STRING)),
        new util.HashMap[CollationIdentifier, util.Set[Column]](),
        new StructType()
          .add("a1", new StructType()
            .add("b1", StringType.STRING))
          .add("a2", UTF8_LCASE_STRING)
          .add(MIN, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING))
            .add("a2", UTF8_LCASE_STRING))
          .add(MAX, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING))
            .add("a2", UTF8_LCASE_STRING))
      ),
      (
        new StructType()
          .add("a1", new StructType()
            .add("b1", StringType.STRING)
            .add("b2", new StructType()
              .add("c1", IntegerType.INTEGER)
              .add("c2", UTF8_LCASE_STRING)))
          .add("a2", UTF8_LCASE_STRING)
          .add("a3", FloatType.FLOAT),
        new StructType()
          .add("a1", new StructType()
            .add("b1", StringType.STRING)
            .add("b2", new StructType()
              .add("c1", IntegerType.INTEGER)
              .add("c2", UTF8_LCASE_STRING)))
          .add("a2", UTF8_LCASE_STRING)
          .add("a3", FloatType.FLOAT)
          .add(MIN, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING)
              .add("b2", new StructType()
                .add("c1", IntegerType.INTEGER)
                .add("c2", UTF8_LCASE_STRING)))
            .add("a2", UTF8_LCASE_STRING)
            .add("a3", FloatType.FLOAT))
          .add(MAX, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING)
              .add("b2", new StructType()
                .add("c1", IntegerType.INTEGER)
                .add("c2", UTF8_LCASE_STRING)))
            .add("a2", UTF8_LCASE_STRING)
            .add("a3", FloatType.FLOAT)),
        new util.HashMap[CollationIdentifier, util.Set[Column]]() {
          put(UTF8_LCASE, Set(new Column(Array("a1", "b1")),
            new Column(Array("a1", "b2", "c2"))).asJava)
          put(UTF8_BINARY, Set(new Column("a2"), new Column(Array("a1", "b1"))).asJava)
        },
        new StructType()
          .add("a1", new StructType()
            .add("b1", StringType.STRING)
            .add("b2", new StructType()
              .add("c1", IntegerType.INTEGER)
              .add("c2", UTF8_LCASE_STRING)))
          .add("a2", UTF8_LCASE_STRING)
          .add("a3", FloatType.FLOAT)
          .add(MIN, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING)
              .add("b2", new StructType()
                .add("c1", IntegerType.INTEGER)
                .add("c2", UTF8_LCASE_STRING)))
            .add("a2", UTF8_LCASE_STRING)
            .add("a3", FloatType.FLOAT))
          .add(MAX, new StructType()
            .add("a1", new StructType()
              .add("b1", StringType.STRING)
              .add("b2", new StructType()
                .add("c1", IntegerType.INTEGER)
                .add("c2", UTF8_LCASE_STRING)))
            .add("a2", UTF8_LCASE_STRING)
            .add("a3", FloatType.FLOAT))
          .add(STATS_WITH_COLLATION, new StructType()
            .add(UTF8_LCASE.toString, new StructType()
              .add(MIN, new StructType()
                .add("a1", new StructType()
                  .add("b1", StringType.STRING)
                  .add("b2", new StructType()
                    .add("c2", UTF8_LCASE_STRING))))
              .add(MAX, new StructType()
                .add("a1", new StructType()
                  .add("b1", StringType.STRING)
                  .add("b2", new StructType()
                    .add("c2", UTF8_LCASE_STRING)))))
            .add(UTF8_BINARY.toString, new StructType()
              .add(MIN, new StructType()
                .add("a1", new StructType()
                  .add("b1", StringType.STRING))
                .add("a2", UTF8_LCASE_STRING))
              .add(MAX, new StructType()
                .add("a1", new StructType()
                  .add("b1", StringType.STRING))
                .add("a2", UTF8_LCASE_STRING))))
      )
    ).foreach {
      case (schema, schemaWithStats, collationMap, expectedSchema) =>
        val fullStatsSchema = appendCollatedStatsSchema(schema, schemaWithStats, collationMap)
        // We compare StructTypes field by field to avoid the order of fields
        for (field <- fullStatsSchema.fields().asScala) {
          if (field.getName.equals(STATS_WITH_COLLATION)) {
            for (collationStats <- field.getDataType.asInstanceOf[StructType].fields().asScala) {
              assert(checkStatsExist(collationStats.getDataType.asInstanceOf[StructType],
                collationStats.getName, expectedSchema))
            }
          } else {
            assert(expectedSchema.fields().contains(field))
          }
        }
    }

    def checkStatsExist(schema: StructType, collationName: String,
                        expectedSchema: StructType): Boolean = {
      val expectedStats = expectedSchema.get(STATS_WITH_COLLATION).getDataType
        .asInstanceOf[StructType].get(collationName).getDataType.asInstanceOf[StructType]
      val expectedMinStats = expectedStats.get(MIN).getDataType.asInstanceOf[StructType]
      val expectedMaxStats = expectedStats.get(MAX).getDataType.asInstanceOf[StructType]
      val minStats = schema.get(MIN).getDataType.asInstanceOf[StructType]
      val maxStats = schema.get(MAX).getDataType.asInstanceOf[StructType]

      expectedMinStats.fields().asScala.forall { field =>
        minStats.fields().contains(field)
      } &&
      expectedMaxStats.fields().asScala.forall { field =>
        maxStats.fields().contains(field)
      }
    }
  }
}