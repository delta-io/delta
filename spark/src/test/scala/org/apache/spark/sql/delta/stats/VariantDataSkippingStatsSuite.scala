/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.stats

import java.math.BigDecimal

import scala.util.Random

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.shims.VariantStatsShims
import org.apache.spark.sql.delta.test.shims.VariantShreddingTestShims
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.util.Codec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.variant.SchemaOfVariant
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.internal.LegacyBehaviorPolicy.{CORRECTED, EXCEPTION, LEGACY}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.types.variant.{Variant, VariantUtil}

class VariantDataSkippingStatsSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with ParquetTest {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.variant.writeShredding.enabled", "true")
    spark.conf.set(SQLConf.VARIANT_ALLOW_READING_SHREDDED.key, "true")
    spark.conf.set(DeltaSQLConf.COLLECT_VARIANT_DATA_SKIPPING_STATS.key, "true")
    spark.conf.set(DeltaSQLConf.DELTA_STATS_LIMIT_PER_VARIANT.key, "10")
    spark.conf.set(DeltaSQLConf.PARSE_FOOTER_FOR_VARIANT_DATA_SKIPPING_STATS.key, "true")
  }

  /**
   * Create a Delta table using the provided select expression, and return the JSON stats for each
   * file. If `minRowGroups > 0`, the test verifies that the number of row groups in the sole
   * parquet file (assuming `numPartitions = 1`) is at least `minRowGroups`.
   */
  def createVariantTableWithPartitionsAndExtractStats(
      numPartitions: Int,
      variantQuery: String,
      options: Map[String, String] = Map.empty,
      minRowGroups: Int = 0): Seq[JValue] = {
    var json: Seq[JValue] = null
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val data = spark.sql(variantQuery).repartition(numPartitions)

      data.write.format("delta")
        .options(options ++ Map("delta.enableVariantShredding" -> "true"))
        .save(path)

      val deltaLog = DeltaLog.forTable(spark, path)
      val snapshot = deltaLog.update()
      json = snapshot.allFiles.collect().map(f => parse(f.stats)).toSeq
      if (minRowGroups > 0) {
        assert(numPartitions == 1)
        val absPath = snapshot.allFiles.collect().map(f => f.absolutePath(deltaLog).toString).head
        val conf = new Configuration()
        val reader = ParquetFileReader.open(conf, new Path(absPath))
        val footer = reader.getFooter
        assert(footer.getBlocks.size() >= minRowGroups)
        reader.close()
      }
    }
    assert(json.size == numPartitions)
    json
  }

  // Extract stats for a single file.
  def createVariantTableAndExtractStats(
      variantQuery: String,
      options: Map[String, String] = Map.empty,
      minRowGroups: Int = 0): JValue = {
    createVariantTableWithPartitionsAndExtractStats(
      numPartitions = 1, variantQuery = variantQuery, options = options,
      minRowGroups = minRowGroups).head
  }

  // Get the relevant stat from JSON.
  def extractJson(json: JValue, statsType: String, columnName: String): JValue = {
    implicit val formats = DefaultFormats
    // The normal case is a single column name, but split on `.` to test variant nested in struct
    // fields.
    val columnNameParts = columnName.split("\\.")
    // Navigate through the JSON structure using each part.
    val statsJson = (json \ statsType)
    columnNameParts.foldLeft(statsJson) { (current, part) =>
      current \ part
    }
  }

  // Extract a Variant min/max from the provdied stats JSON.
  def extractVariant(json: JValue, statsType: String, columnName: String): Variant = {
    implicit val formats = DefaultFormats
    val encodedValues = extractJson(json, statsType, columnName).extract[String]
    val decoded = Codec.Base85Codec.decodeBytes(encodedValues, encodedValues.length)
    val metadataSize = VariantStatsShims.metadataSize(decoded)
    val value = decoded.slice(metadataSize, decoded.length)
    new Variant(value, decoded)
  }

  def nullCount(json: JValue, columnName: String): Option[Long] = {
    implicit val formats = DefaultFormats
    val nullField = extractJson(json, "nullCount", columnName)
    if (nullField == JNothing) {
      None
    } else {
      Some(nullField.extract[Long])
    }
  }

  def statsSchema(json: JValue, columnName: String): StructType = {
    val variant = extractVariant(json, "minValues", columnName)
    val schema = SchemaOfVariant.schemaOf(variant)
    schema match {
      case s: StructType => s
      case _ => fail(s"Expected StructType but got ${schema.getClass.getName} for $columnName")
    }
  }

  /**
   * Helper method to decode and validate a single variant value from stats.
   */
  def decodeAndValidateVariantStatsValue(
      json: JValue,
      columnName: String,
      statsType: String, // "minValues" or "maxValues"
      expectedFieldPath: String,
      expectedType: VariantUtil.Type,
      expectedValue: Any,
      expectedNulls: Long,
      expectedDecimalType: Int): Unit = {
    val variant = extractVariant(json, statsType, columnName)

    val field = variant.getFieldByKey(expectedFieldPath)
    assert(field != null,
      s"$expectedFieldPath not found. Fields are ${SchemaOfVariant.schemaOf(variant)}")
    val actualType = field.getType
    assert(actualType == expectedType,
      s"Expected $expectedType but got $actualType for field $expectedFieldPath in $statsType")
    val typeInfo = field.getTypeInfo
    val actualValue = expectedValue match {
      case _: Byte =>
        assert(typeInfo == VariantUtil.INT1)
        field.getLong
      case _: Short =>
        assert(typeInfo == VariantUtil.INT2)
        field.getLong
      case _: Int =>
        if (actualType == VariantUtil.Type.DATE) {
          assert(typeInfo == VariantUtil.DATE)
        } else {
          assert(typeInfo == VariantUtil.INT4)
        }
        field.getLong
      case _: Long =>
        if (actualType == VariantUtil.Type.TIMESTAMP) {
          assert(typeInfo == VariantUtil.TIMESTAMP)
        } else if (actualType == VariantUtil.Type.TIMESTAMP_NTZ) {
          assert(typeInfo == VariantUtil.TIMESTAMP_NTZ)
        } else {
          assert(typeInfo == VariantUtil.INT8)
        }
        field.getLong
      case _: String => field.getString()
      case _: Boolean => field.getBoolean()
      case _: Double => field.getDouble()
      case _: Float => field.getFloat()
      case _: BigDecimal =>
        assert(expectedDecimalType != -1)
        assert(typeInfo == expectedDecimalType)
        VariantShreddingTestShims.getDecimalWithOriginalScale(field)
      case _ => fail(s"Unexpected value type: $expectedValue")
    }

    val valuesMatch = (actualValue, expectedValue) match {
      case (a: Double, e: Double) if a.isNaN && e.isNaN => true
      case (a: Float, e: Float) if a.isNaN && e.isNaN => true
      case _ => actualValue == expectedValue
    }
    assert(valuesMatch,
      s"Expected $expectedValue but got $actualValue for field $expectedFieldPath in $statsType")

    assert(nullCount(json, columnName) == Some(expectedNulls))
  }

  /**
   * Check that the expected min and max stats exist at the expected field path.
   */
  def decodeAndValidateVariantStats(
      json: JValue,
      columnName: String,
      expectedFieldPath: String,
      expectedType: VariantUtil.Type,
      expectedMinValue: Any,
      expectedMaxValue: Any,
      expectedNulls: Long = 0,
      expectedDecimalType: Int = -1): Unit = {
    decodeAndValidateVariantStatsValue(
      json, columnName, "minValues", expectedFieldPath, expectedType, expectedMinValue,
      expectedNulls, expectedDecimalType)
    decodeAndValidateVariantStatsValue(
      json, columnName, "maxValues", expectedFieldPath, expectedType, expectedMaxValue,
      expectedNulls, expectedDecimalType)
  }

  test("basic variant stats") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  to_variant_object(named_struct('int_field', id, 'str_field', cast(id as string))) as v
         |  from range(3)""".stripMargin)
    decodeAndValidateVariantStats(json, "v", "$['int_field']", VariantUtil.Type.LONG, 0L, 2L)
    decodeAndValidateVariantStats(json, "v", "$['str_field']", VariantUtil.Type.STRING, "0", "2")
  }

  test("nested variant stats") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  struct(to_variant_object(
         |   named_struct('int_field', id, 'str_field', cast(id as string))) as v) as s,
         |  array(to_variant_object(
         |   named_struct('int_field', id, 'str_field', cast(id as string)))) as a,
         |  map(id, to_variant_object(
         |   named_struct('int_field', id, 'str_field', cast(id as string)))) as m
         |  from range(3)""".stripMargin)
    decodeAndValidateVariantStats(json, "s.v", "$['int_field']", VariantUtil.Type.LONG, 0L, 2L)
    decodeAndValidateVariantStats(json, "s.v", "$['str_field']", VariantUtil.Type.STRING, "0", "2")
    // Stats are not collected for arrays and maps.
    assert((json \ "minValues" \ "a") == JNothing)
    assert((json \ "minValues" \ "a") == JNothing)
    assert((json \ "maxValues" \ "m") == JNothing)
    assert((json \ "maxValues" \ "m") == JNothing)
  }

  test("Non-objects in variant") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  to_variant_object(array(id, id + 1, id + 2)) as v_array,
         |  cast(id as variant) as v_scalar
         |  from range(3)""".stripMargin)
    decodeAndValidateVariantStats(json, "v_scalar", "$", VariantUtil.Type.LONG, 0L, 2L)
    // We don't collect stats for Variant arrays.
    assert((json \ "minValues" \ "v_array") == JNothing)
    assert((json \ "minValues" \ "v_array") == JNothing)
  }

  test("null counts") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  cast(null as variant) as v_all_null,
         |  case when id % 2 = 0 then cast(null as variant) else
         |   to_variant_object(named_struct('int_field', id, 'str_field', cast(id as string)))
         |   end as v
         |  from range(102)""".stripMargin)
    decodeAndValidateVariantStats(
      json, "v", "$['int_field']", VariantUtil.Type.LONG, 1L, 101L,
      expectedNulls = 51)
    decodeAndValidateVariantStats(json, "v", "$['str_field']", VariantUtil.Type.STRING, "1", "99",
      expectedNulls = 51)
    assert((json \ "minValues" \ "v_all_null") == JNothing)
    assert((json \ "minValues" \ "v_all_null") == JNothing)
    assert(nullCount(json, "v_all_null") == Some(102))
  }

  test("variant stats with different data types") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  to_variant_object(named_struct(
         |    'int_field', id,
         |    'long_field', cast(id as long),
         |    'float_field', cast(id as float),
         |    'double_field', cast(id as double),
         |    'string_field', cast(id as string),
         |    'boolean_field', id % 2 == 0,
         |    'decimal_field', cast(id as decimal(10,2)),
         |    'date_field', date_from_unix_date(12345 + id),
         |    'timestamp_field', cast(1.234567 + id as timestamp),
         |    'timestamp_ntz_field', cast('1970-01-01 00:00:00.123456Z' as timestamp_ntz)
         |       + make_dt_interval(0, 0, 0, id)
         |  )) as v
         |  from range(3)""".stripMargin
    )
    // Delta doesn't collect boolean stats, so we also don't for Variant.
    val schema = statsSchema(json, "v")
    assert(!schema.fields.exists(_.name == "boolean_field"))
    decodeAndValidateVariantStats(json, "v", "$['int_field']", VariantUtil.Type.LONG, 0L, 2L)
    decodeAndValidateVariantStats(json, "v", "$['long_field']", VariantUtil.Type.LONG, 0L, 2L)
    decodeAndValidateVariantStats(json, "v", "$['float_field']", VariantUtil.Type.FLOAT, 0.0f, 2.0f)
    decodeAndValidateVariantStats(json, "v", "$['double_field']", VariantUtil.Type.DOUBLE, 0.0, 2.0)
    decodeAndValidateVariantStats(json, "v", "$['string_field']",
      VariantUtil.Type.STRING, "0", "2")
    decodeAndValidateVariantStats(
      json, "v", "$['decimal_field']",
      VariantUtil.Type.DECIMAL, new BigDecimal("0.00"), new BigDecimal("2.00"),
      expectedDecimalType = VariantUtil.DECIMAL8)
    decodeAndValidateVariantStats(json, "v", "$['date_field']",
      VariantUtil.Type.DATE, 12345, 12347)
    decodeAndValidateVariantStats(json, "v", "$['timestamp_field']",
      VariantUtil.Type.TIMESTAMP, 1234567L, 3234567L)
    val ntzBase = java.time.Instant.parse("1970-01-01T00:00:00Z")
      .toEpochMilli * 1000L + 123456L
    decodeAndValidateVariantStats(json, "v", "$['timestamp_ntz_field']",
      VariantUtil.Type.TIMESTAMP_NTZ, ntzBase, ntzBase + (2 * 1000 * 1000))
  }

  test("variant stats with multiple partitions") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableWithPartitionsAndExtractStats(numPartitions = 5,
      s"""select
         |    id long_field,
         |    cast(id as double) double_field,
         |    cast(id as string) string_field,
         |    id % 2 == 0 as boolean_field,
         |    cast(id as decimal(10,2)) decimal_field,
         |    date_from_unix_date(12345 + id) date_field,
         |    cast(1.234 + id as timestamp) timestamp_field,
         |  to_variant_object(named_struct(
         |    'int_field', id,
         |    'long_field', cast(id as long),
         |    'double_field', cast(id as double),
         |    'string_field', cast(id as string),
         |    'boolean_field', id % 2 == 0,
         |    'decimal_field', cast(id as decimal(10,2)),
         |    'date_field', date_from_unix_date(12345 + id),
         |    'timestamp_field', cast(1.234 + id as timestamp)
         |  )) as v
         |  from range(100)""".stripMargin
    )
    json.foreach { j =>
      // We don't know how values are distributed across partitions, so check against the identical
      // non-variant values.
      implicit val formats = DefaultFormats

      // Delta doesn't collect boolean stats, so the boolean field should be missing.
      assert((j \ "minValues" \ "boolean_field") == JNothing)
      assert((j \ "maxValues" \ "boolean_field") == JNothing)
      val schema = statsSchema(j, "v")
      assert(!schema.fields.exists(_.name == "boolean_field"))

      val longMin = (j \ "minValues" \ "long_field").extract[Long]
      val longMax = (j \ "maxValues" \ "long_field").extract[Long]
      decodeAndValidateVariantStats(j, "v", "$['long_field']",
        VariantUtil.Type.LONG, longMin, longMax)

      val doubleMin = (j \ "minValues" \ "double_field").extract[Double]
      val doubleMax = (j \ "maxValues" \ "double_field").extract[Double]
      decodeAndValidateVariantStats(j, "v", "$['double_field']",
        VariantUtil.Type.DOUBLE, doubleMin, doubleMax)

      val stringMin = (j \ "minValues" \ "string_field").extract[String]
      val stringMax = (j \ "maxValues" \ "string_field").extract[String]
      decodeAndValidateVariantStats(j, "v", "$['string_field']",
        VariantUtil.Type.STRING, stringMin, stringMax)

      // Extracting decimal is awkward, and the actual values we're using losslessly convert.
      val decimalMin = (j \ "minValues" \ "decimal_field").extract[Double]
      val decimalMax = (j \ "maxValues" \ "decimal_field").extract[Double]
      decodeAndValidateVariantStats(j, "v", "$['decimal_field']",
        VariantUtil.Type.DECIMAL,
        (new BigDecimal(decimalMin)).setScale(2),
        (new BigDecimal(decimalMax)).setScale(2), expectedDecimalType = VariantUtil.DECIMAL8)

      val dateMin = (j \ "minValues" \ "date_field").extract[String]
      val dateMax = (j \ "maxValues" \ "date_field").extract[String]
      // Variant stores days since Unix epoch.
      val dateMinDays = java.time.LocalDate.parse(dateMin).toEpochDay.toInt
      val dateMaxDays = java.time.LocalDate.parse(dateMax).toEpochDay.toInt
      decodeAndValidateVariantStats(j, "v", "$['date_field']",
        VariantUtil.Type.DATE, dateMinDays, dateMaxDays)

      val timestampMin = (j \ "minValues" \ "timestamp_field").extract[String]
      val timestampMax = (j \ "maxValues" \ "timestamp_field").extract[String]
      // Variant stores microseconds since Unix epoch. Note that non-variant timestamps are
      // truncated to ms precision, so there's no need to extract the microseconds: this check
      // is only valid because the values in our table don't have sub-ms digits.
      val timestampMinMicros = java.time.Instant.parse(timestampMin).toEpochMilli * 1000
      val timestampMaxMicros = java.time.Instant.parse(timestampMax).toEpochMilli * 1000
      decodeAndValidateVariantStats(j, "v", "$['timestamp_field']",
        VariantUtil.Type.TIMESTAMP, timestampMinMicros, timestampMaxMicros)
    }
  }

  test("more than 10 fields") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val fields = (0 until 50).map { i =>
      s"'field_$i', $i"
    }.mkString(", ")
    val variantQuery = s"to_variant_object(named_struct($fields))"

    val json = createVariantTableAndExtractStats(
      s"""select
         |  $variantQuery as v
         |  from range(3)""".stripMargin
    )
    // Stats should only be kept for the first `DELTA_STATS_LIMIT_PER_VARIANT` fields. We use the
    // runtime conf value rather than `defaultValue.get` because the OSS default is 0; `beforeAll`
    // sets it to 10 for this suite.
    val schema = statsSchema(json, "v")
    val expectedFields =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_LIMIT_PER_VARIANT)
    assert(schema.fields.length == expectedFields,
      s"Expected $expectedFields fields but got $schema")
    // The retained fields are based on lexicographic order, so sort the fields as strings.
    (0 until 50).map(_.toString).sorted.take(expectedFields).foreach { i =>
      decodeAndValidateVariantStats(json, "v", s"$$['field_$i']",
        VariantUtil.Type.LONG, i.toLong, i.toLong)
    }
  }

  test("more than 32 variant columns") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val columns = (0 until 50).map { i =>
      s"to_variant_object(named_struct('id', id + $i, 'name', 'col_$i')) as v$i"
    }.mkString(", ")
    val query = s"select id, $columns from range(100)"

    // Run with a few different configs that affect the expectations
    def runTest(expectedStats: Set[Int], options: Map[String, String]): Unit = {
      implicit val formats = DefaultFormats
      val json = createVariantTableWithPartitionsAndExtractStats(numPartitions = 5, query, options)
      json.foreach { j =>
        val expectedMin = (j \ "minValues" \ "id").extract[Long]
        val expectedMax = (j \ "maxValues" \ "id").extract[Long]
        for (i <- 0 until 50) {
          if (expectedStats.contains(i)) {
            assert((j \ "minValues" \ s"v$i") != JNothing)
            assert((j \ "maxValues" \ s"v$i") != JNothing)
            decodeAndValidateVariantStats(j, s"v$i", "$['id']",
              VariantUtil.Type.LONG, expectedMin + i, expectedMax + i)
            decodeAndValidateVariantStats(j, s"v$i", "$['name']",
              VariantUtil.Type.STRING, s"col_$i", s"col_$i")
          } else {
            assert((j \ "minValues" \ s"v$i") == JNothing)
            assert((j \ "maxValues" \ s"v$i") == JNothing)
          }
        }
      }
    }

    // By default, the first 32 columns should have stats, but the first is the id column,
    // so only 31 variant columns
    runTest((0 until 31).toSet, options = Map.empty)
    // Limit number of columns we collect stats for.
    runTest((0 until 9).toSet, options = Map(
      "delta.dataSkippingNumIndexedCols" -> "10"))
    // Only collect stats for specific columns.
    runTest(Set(20, 40, 49), options = Map(
      "delta.dataSkippingStatsColumns" -> "v20,v40,v49,id"))
  }

  test("unusual characters") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    // First element is the name in the query, second is the escaped name in the JSON path.
    // scalastyle:off nonascii
    val unusualNames = Seq(
      ("normal_field", "normal_field"),
      ("", ""),
      (".", "."),
      ("path.with.dots", "path.with.dots"),
      ("\ttab\t", "\\ttab\\t"),
      // JsonPath uses escaped versions for ASCII control characters
      ("\nnew.l\nine\n", "\\nnew.l\\nine\\n"),
      ("\r\f\b/control\r", "\\r\\f\\b/control\\r"),
      ("\\\\back\\\\slash\\\\", "\\\\back\\\\slash\\\\"),
      ("\"qu\"ote\"", "\"qu\"ote\""),
      ("brackets[0][1][2]", "brackets[0][1][2]"),
      ("\\'single\\'_quote\\'", "\\'single\\'_quote\\'"),
      ("😀unicode_ñáéíóú😀🎉🚀中文ñáéíóú😀🎉🚀中文", "😀unicode_ñáéíóú😀🎉🚀中文ñáéíóú😀🎉🚀中文"),
      // 0x0A is \n and 0x02 is space. Non-special code points below 0x20 are escaped as \\u00xx.
      ("\\u0000\\u000A\\u001F\\u0020\\U0001F600", "\\u0000\\n\\u001f 😀")
    )
    // scalastyle:on nonascii

    val fields = unusualNames.zipWithIndex.map { case ((name, _), i) =>
      s"'$name', $i"
    }.mkString(", ")

    val expr = s"to_variant_object(named_struct($fields))"

    withSQLConf(
      DeltaSQLConf.DELTA_STATS_LIMIT_PER_VARIANT.key -> unusualNames.length.toString) {
      val json = createVariantTableAndExtractStats(
        s"""select
           |  $expr as v
           |  from range(3)""".stripMargin
      )

      // Test various field names with special characters
      unusualNames.zipWithIndex.foreach { case ((_, escapedName), i) =>
        decodeAndValidateVariantStats(json, "v", s"$$['$escapedName']",
          VariantUtil.Type.LONG, i.toLong, i.toLong)
      }
    }
  }

  test("nested fields") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  to_variant_object(named_struct(
         |    'level1', named_struct(
         |      'level2', named_struct(
         |        'level3', named_struct(
         |          'int_field', id,
         |          'string_field', cast(id as string)
         |        )
         |      )
         |    )
         |  )) as v
         |  from range(3)""".stripMargin
    )
    decodeAndValidateVariantStats(json, "v", "$['level1']['level2']['level3']['int_field']",
      VariantUtil.Type.LONG, 0L, 2L)
    decodeAndValidateVariantStats(
      json, "v", "$['level1']['level2']['level3']['string_field']",
      VariantUtil.Type.STRING, "0", "2")
  }

  test("no stats for arrays") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  to_variant_object(named_struct(
         |    'array_field', array(id, id + 1, id + 2),
         |    'string_field', cast(id as string),
         |    'another_array_field', array(cast(id as string))
         |  )) as v
         |  from range(3)""".stripMargin
    )
    // Arrays don't produce stats, but other fields should still have them.
    val schema = statsSchema(json, "v")
    assert(schema.fields.length == 1)
    decodeAndValidateVariantStats(json, "v", "$['string_field']", VariantUtil.Type.STRING, "0", "2")
  }

  test("missing values") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      s"""select
         |  parse_json( '{' ||
         |    case when id % 2 = 0 then ('"int_field": ' || id || ',') else '' end ||
         |    case when id % 2 = 1 then ('"string_field": "' || cast(id as string) || '",')
         |                         else '' end ||
         |    '"null_field": null' ||
         |    '}') as v
         |  from range(6)""".stripMargin
    )
    // Should still be able to extract stats for non-null values
    decodeAndValidateVariantStats(json, "v", "$['int_field']", VariantUtil.Type.LONG, 0L, 4L)
    decodeAndValidateVariantStats(json, "v", "$['string_field']", VariantUtil.Type.STRING, "1", "5")
    val schema = statsSchema(json, "v")
    // The null field should not have stats.
    assert(schema.fields.length == 2)
  }

  test("mixed types") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val json = createVariantTableAndExtractStats(
      // Use 10000 rows so that the shredding decision isn't affected by the mismatched type in row
      // 9999.
      s"""select
         |  parse_json(
         |    case when id = 0 then
         |      '{"shred_int": 0, "shred_decimal": 1.234,
         |        "int_with_null": null, "shred_decimal_9_1": 1.1}'
         |    when id = 9999 then
         |      '{"shred_int": "123", "shred_decimal": 1.1,
         |         "int_with_null": 2, "shred_decimal_9_1": 1.234}'
         |    else
         |      '{"shred_int": 456, "shred_decimal": -10,
         |        "int_with_null": 2, "shred_decimal_9_1": -10}'
         |    end
         |  ) as v
         |  from range(10000)""".stripMargin
    )
    // Mixed int/decimal should be shredded as decimal, the rest will use `value` field, so
    // shouldn't have stats.
    decodeAndValidateVariantStats(
      json, "v", "$['shred_decimal']",
      VariantUtil.Type.DECIMAL, new BigDecimal("-10.000"), new BigDecimal("1.234"),
      expectedDecimalType = VariantUtil.DECIMAL8)
    val schema = statsSchema(json, "v")
    // Remaining fields should not have stats.
    assert(schema.fields.length == 1)
  }

  test("conf disables stats") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    for (stats <- Seq(false, true)) {
      for (shredding <- Seq(false, true)) {
        withSQLConf(
          DeltaSQLConf.COLLECT_VARIANT_DATA_SKIPPING_STATS.key -> stats.toString,
          "spark.sql.variant.writeShredding.enabled" -> shredding.toString) {
          val json = createVariantTableAndExtractStats(
            s"""select
               |  to_variant_object(named_struct(
               |    'int_field', id, 'null_field', cast(null as string)
               |  )) as v,
               |  id
               |  from range(3)""".stripMargin
          )
          implicit val formats = DefaultFormats
          if (stats && shredding) {
            // Should still be able to extract stats for non-null values
            decodeAndValidateVariantStats(
              json, "v", "$['int_field']", VariantUtil.Type.LONG, 0L, 2L)
            val schema = statsSchema(json, "v")
            // The null field should not have stats.
            assert(schema.fields.length == 1)
          } else {
            // id stats are still present; variant stats are not.
            assert((json \ "minValues" \ "id").extract[Long] == 0L)
            assert((json \ "maxValues" \ "id").extract[Long] == 2L)
            assert((json \ "minValues" \ "v") == JNothing)
            assert((json \ "maxValues" \ "v") == JNothing)
          }
        }
      }
    }
  }

  test("parse footer conf disables variant stats") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    withSQLConf(
      DeltaSQLConf.PARSE_FOOTER_FOR_VARIANT_DATA_SKIPPING_STATS.key -> "false") {
      val json = createVariantTableAndExtractStats(
        s"""select
           |  to_variant_object(named_struct(
           |    'int_field', id, 'str_field', cast(id as string)
           |  )) as v,
           |  id
           |  from range(3)""".stripMargin
      )
      implicit val formats = DefaultFormats
      assert((json \ "minValues" \ "id").extract[Long] == 0L)
      assert((json \ "maxValues" \ "id").extract[Long] == 2L)
      assert((json \ "minValues" \ "v") == JNothing)
      assert((json \ "maxValues" \ "v") == JNothing)
    }
  }

  test("extreme values") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    withSQLConf(DeltaSQLConf.DELTA_STATS_LIMIT_PER_VARIANT.key -> "11") {
      val json = createVariantTableAndExtractStats(
        s"""select
           |  to_variant_object(named_struct(
           |    'min_int', -9223372036854775808,
           |    'max_int', 9223372036854775807,
           |    'min_double', -1.7976931348623157E308,
           |    'max_double', 1.7976931348623157E308,
           |    'min_decimal', -9.9999999999999999999999999999999999999,
           |    'max_decimal', 9.9999999999999999999999999999999999999,
           |    'nan', acos(id),
           |    'empty_string', '',
           |    'very_long_string', repeat('x', 1000),
           |    'timestamp_boundary', case when id % 2 = 0 then
           |        timestamp_micros(-9223372036854775808)::variant else
           |        timestamp_micros(9223372036854775807)::variant end,
           |    'date_boundary', case when id % 2 = 1 then
           |        date_add(date'1970-01-01', -2147483648)::variant else
           |        date_add(date'1970-01-01', 2147483647)::variant end
           |  )) as v
           |  from range(3)""".stripMargin
      )
      decodeAndValidateVariantStats(
        json, "v", "$['min_int']",
        VariantUtil.Type.LONG, -9223372036854775808L, -9223372036854775808L)
      decodeAndValidateVariantStats(
        json, "v", "$['max_int']",
        VariantUtil.Type.LONG, 9223372036854775807L, 9223372036854775807L)
      decodeAndValidateVariantStats(
        json, "v", "$['min_double']",
        VariantUtil.Type.DOUBLE, -1.7976931348623157E308, -1.7976931348623157E308)
      decodeAndValidateVariantStats(
        json, "v", "$['max_double']",
        VariantUtil.Type.DOUBLE, 1.7976931348623157E308, 1.7976931348623157E308)
      decodeAndValidateVariantStats(
        json, "v", "$['min_decimal']",
        VariantUtil.Type.DECIMAL, new BigDecimal("-9.9999999999999999999999999999999999999"),
        new BigDecimal("-9.9999999999999999999999999999999999999"),
        expectedDecimalType = VariantUtil.DECIMAL16)
      decodeAndValidateVariantStats(
        json, "v", "$['max_decimal']",
        VariantUtil.Type.DECIMAL, new BigDecimal("9.9999999999999999999999999999999999999"),
        new BigDecimal("9.9999999999999999999999999999999999999"),
        expectedDecimalType = VariantUtil.DECIMAL16)
      // Parquet footer stats do not store NaN for floating-point columns, so the `nan` field is
      // expected to be absent from the variant stats.
      val minVariant = extractVariant(json, "minValues", "v")
      assert(minVariant.getFieldByKey("$['nan']") == null)
      decodeAndValidateVariantStats(json, "v", "$['empty_string']", VariantUtil.Type.STRING, "", "")
      // For max, append the surrogate pair for 0x10FFFF.
      // scalastyle:off nonascii
      decodeAndValidateVariantStats(
        json, "v", "$['very_long_string']",
        VariantUtil.Type.STRING, "x".repeat(32), "x".repeat(32) + "\uDBFF\uDFFF")
      // scalastyle:on nonascii
      decodeAndValidateVariantStats(
        json, "v", "$['timestamp_boundary']",
        VariantUtil.Type.TIMESTAMP, -9223372036854775808L, 9223372036854775807L)
      decodeAndValidateVariantStats(
        json, "v", "$['date_boundary']",
        VariantUtil.Type.DATE, -2147483648, 2147483647)
    }
  }

  test("range of different data types") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    // Generate jumbled data - the second column can be cast to any data type
    // We jumble so the max can come from any row group
    val data = (0L until 20000L).zip(Random.shuffle((-10000L until 10000L).toList))
    val initialDf = data.toDF("id", "col")
    // Make block size small so the min and max are spread across different row groups. This way, we
    // make sure that the stats are being aggregated correctly across row groups
    withSQLConf("spark.sql.session.timeZone" -> "UTC", "parquet.block.size" -> "10") {
      Seq(
        ("try_cast(col as tinyint)", VariantUtil.Type.LONG, -128L, 127L, -1),
        ("col::short", VariantUtil.Type.LONG, -10000L, 9999L, -1),
        ("col::int", VariantUtil.Type.LONG, -10000L, 9999L, -1),
        ("col::long", VariantUtil.Type.LONG, -10000L, 9999L, -1),
        ("col::float", VariantUtil.Type.FLOAT, -10000f, 9999f, -1),
        ("col::double", VariantUtil.Type.DOUBLE, -10000d, 9999d, -1),
        ("date_from_unix_date(col)", VariantUtil.Type.DATE, -10000, 9999, -1),
        ("col::timestamp", VariantUtil.Type.TIMESTAMP, -10000000000L, 9999000000L, -1),
        (
          "col::timestamp::timestamp_ntz",
          VariantUtil.Type.TIMESTAMP_NTZ,
          -10000000000L,
          9999000000L,
          -1
        ),
        ("col::string", VariantUtil.Type.STRING, "-1", "9999", -1),
        // Decimal4
        (
          "col::decimal(9, 2) / 100",
          VariantUtil.Type.DECIMAL,
          new BigDecimal("-100.000000"),
          new BigDecimal("99.990000"),
          VariantUtil.DECIMAL8
        ),
        // Decimal8
        (
          "col::decimal(11, 2) / 100",
          VariantUtil.Type.DECIMAL,
          new BigDecimal("-100.000000"),
          new BigDecimal("99.990000"),
          VariantUtil.DECIMAL8
        ),
        // Decimal16
        (
          "col::decimal(22, 2) * 1000000000000000000000000",
          VariantUtil.Type.DECIMAL,
          new BigDecimal("-10000000000000000000000000000.00"),
          new BigDecimal("9999000000000000000000000000.00"),
          VariantUtil.DECIMAL16
        )
      ).foreach {
        case (dt, variantType, expectedMin, expectedMax, expectedDecimalType) =>
          withTable("tbl") {
            val df = initialDf.filter(s"$dt is not null").selectExpr(
              "id",
              s"to_variant_object(named_struct('col', $dt)) v"
            )
            df.coalesce(1).write.format("parquet").saveAsTable("tbl")
            val json = createVariantTableAndExtractStats("select * from tbl",
              minRowGroups = if (dt == "try_cast(col as tinyint)") { 1 } else { 3 })
            decodeAndValidateVariantStats(
              json, "v", "$['col']",
              variantType, expectedMin, expectedMax, expectedDecimalType = expectedDecimalType)
          }
      }
    }
  }

  test("shredding with mixed decimals") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    withSQLConf("parquet.block.size" -> "10") {
      // decimal16 towards the end - doesn't get buffered so the inferred type will be too narrow
      // => no stats because one row does not fit with the rest.
      val json1 = createVariantTableAndExtractStats("select case when id < 14999 then " +
        "to_variant_object(named_struct('i', id, 'd', id::decimal(9,2))) else " +
        "to_variant_object(" +
        "named_struct('i', id, 'd', id::decimal(9,2) * 1000000000000000000000000)" +
        ") end v " +
        "from range(0, 15000, 1, 1)", minRowGroups = 3)
      val schema1 = statsSchema(json1, "v")
      assert(schema1.fields.length == 1)
      decodeAndValidateVariantStats(json1, "v", "$['i']", VariantUtil.Type.LONG, 0L, 14999L)

      // decimal16 at the beginning - gets buffered to schema is inferred as dec16
      val json2 = createVariantTableAndExtractStats("select case when id > 10 then " +
        "to_variant_object(named_struct('i', id, 'd', id::decimal(9,2))) else " +
        "to_variant_object(" +
        "named_struct('i', id, 'd', id::decimal(22,2) * 1000000000000000000000000)" +
        ") end v " +
        "from range(0, 15000, 1, 1)", minRowGroups = 3)
      val schema2 = statsSchema(json2, "v")
      assert(schema2.fields.length == 2)
      decodeAndValidateVariantStats(json2, "v", "$['i']", VariantUtil.Type.LONG, 0L, 14999L)
      decodeAndValidateVariantStats(json2, "v", "$['d']", VariantUtil.Type.DECIMAL,
        new BigDecimal("0.00"), new BigDecimal("10000000000000000000000000.00"),
        expectedDecimalType = VariantUtil.DECIMAL16)
    }
  }

  test("force narrow int/decimal types in shredding") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    // Currently, we never choose the narrower int8, int16, int32 or dec4 types during schema
    // inference. This test makes sure that stats would work well for these columns as well if the
    // shredding strategy is altered in the future.
    withSQLConf(
      SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key ->
        "i8 BYTE, i16 SHORT, i32 INT, d4 DECIMAL(9, 2)",
      "spark.sql.variant.inferShreddingSchema" -> "false"
    ) {
      val json = createVariantTableAndExtractStats("select " +
        "case when id = 0 then " +
        "to_variant_object(named_struct(" +
        "'i8', 127::byte, 'i16', -32768::short, 'i32', -2147483648::int" +
        ")) when id = 1 then " +
        "to_variant_object(named_struct(" +
        "'i32', 2147483647::int, 'd4', 9999999.99::decimal(9, 2)" +
        ")) else to_variant_object(named_struct(" +
        "'i8', -128::byte, 'i16', 32767::short, 'd4', -9999999.99::decimal(9, 2)" +
        ")) end v from range(3)")
      decodeAndValidateVariantStats(
        json, "v", "$['i8']", VariantUtil.Type.LONG, -128.toByte, 127.toByte)
      decodeAndValidateVariantStats(
        json, "v", "$['i16']", VariantUtil.Type.LONG, -32768.toShort, 32767.toShort)
      decodeAndValidateVariantStats(
        json, "v", "$['i32']", VariantUtil.Type.LONG, -2147483648, 2147483647)
      decodeAndValidateVariantStats(
        json, "v", "$['d4']",
        VariantUtil.Type.DECIMAL,
        new BigDecimal("-9999999.99"),
        new BigDecimal("9999999.99"),
        expectedDecimalType = VariantUtil.DECIMAL4
      )
    }
  }

  test("rebasing dates in write") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val modes = Seq(LEGACY, CORRECTED)
    Seq(false, true).foreach { dictionaryEncoding =>
      modes.foreach { writeMode =>
        (modes :+ EXCEPTION).foreach { readMode =>
          withSQLConf(
            SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> writeMode.toString,
            SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> readMode.toString
          ) {
            val json = createVariantTableAndExtractStats(
              "select '1001-01-01'::date::variant v " +
              "from range(10)",
              options = Map("parquet.enable.dictionary" -> dictionaryEncoding.toString))
            decodeAndValidateVariantStats(
              json, "v", "$", VariantUtil.Type.DATE, -353920, -353920)
          }
        }
      }
    }
  }

  test("rebasing timestamps in write") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    val modes = Seq(LEGACY, CORRECTED)
    Seq(VariantUtil.Type.TIMESTAMP, VariantUtil.Type.TIMESTAMP_NTZ).foreach { timestampType =>
      Seq(false, true).foreach { dictionaryEncoding =>
        modes.foreach { writeMode =>
          (modes :+ EXCEPTION).foreach { readMode =>
            withSQLConf(
              SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> writeMode.toString,
              SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> readMode.toString,
              SQLConf.SESSION_LOCAL_TIMEZONE.key -> DateTimeTestUtils.UTC.getId
            ) {
              val typeString = if (timestampType == VariantUtil.Type.TIMESTAMP) {
                "timestamp"
              } else {
                "timestamp_ntz"
              }
              val json = createVariantTableAndExtractStats(
                s"select '1001-01-01 01:02:03.123456'::$typeString::variant v " +
                  "from range(10)",
                options = Map("parquet.enable.dictionary" -> dictionaryEncoding.toString))
              decodeAndValidateVariantStats(json, "v", "$", timestampType,
                -30578684276876544L, -30578684276876544L)
            }
          }
        }
      }
    }
  }

  test("json path escaper test") {
    assume(!org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    // scalastyle:off nonascii
    Seq(
      ("normal_field", "normal_field"),
      ("field\\name", "field\\\\name"),
      ("field'name", "field\\'name"),
      ("field\"name", "field\"name"),
      ("field\nname", "field\\nname"),
      ("field\tname", "field\\tname"),
      ("field\rname", "field\\rname"),
      ("field\bname", "field\\bname"),
      ("field\fname", "field\\fname"),
      ("field\u0000\u000bname", "field\\u0000\\u000bname"),
      ("field\\'name\n", "field\\\\\\'name\\n"),
      ("field\t'name\\", "field\\t\\'name\\\\"),
      ("field_ñame", "field_ñame"),
      ("field_名字", "field_名字"),
      ("field_🌍", "field_🌍"),
      ("field_ñ\\name", "field_ñ\\\\name"),
      ("field_名字\t", "field_名字\\t")
    ).foreach { case (source, result) =>
      assert(VariantStatsUtils.escapeJsonField(source) == result)
    }
    // scalastyle:on nonascii
  }

  test("Spark 4.0: variant stats are not collected even when shredding is forced") {
    assume(org.apache.spark.SPARK_VERSION.startsWith("4.0"))
    withSQLConf(
      SQLConf.VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST.key -> "a LONG",
      "spark.sql.variant.writeShredding.enabled" -> "true",
      "spark.sql.variant.inferShreddingSchema" -> "false") {
      val json = createVariantTableAndExtractStats(
        "select to_variant_object(named_struct('a', id)) as v from range(3)")
      implicit val formats = DefaultFormats
      assert((json \ "minValues" \ "v") == JNothing)
      assert((json \ "maxValues" \ "v") == JNothing)
    }
  }
}
