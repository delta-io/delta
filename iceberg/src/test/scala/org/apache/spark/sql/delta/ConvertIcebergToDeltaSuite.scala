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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.File
import java.text.SimpleDateFormat
import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.commands.ConvertToDeltaCommand
import org.apache.spark.sql.delta.commands.convert.{ConvertUtils, IcebergTable}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatsUtils
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{Table, TableProperties}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
// scalastyle:on import.ordering.noEmptyLine

class IcebergCompatibleDeltaTestSparkSession(sparkConf: SparkConf)
    extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new DeltaSparkSessionExtension().apply(extensions)
    new IcebergSparkSessionExtensions().apply(extensions)
    extensions
  }
}

trait ConvertIcebergToDeltaUtils extends SharedSparkSession {

  protected var warehousePath: File = null
  protected lazy val table: String = "local.db.table"
  protected lazy val tablePath: String = "file://" + warehousePath.getCanonicalPath + "/db/table"
  protected lazy val nestedTable: String = "local.db.nested_table"
  protected lazy val nestedTablePath: String =
    "file://" + warehousePath.getCanonicalPath + "/db/nested_table"

  protected def collectStatisticsStringOption(collectStats: Boolean): String = Option(collectStats)
    .filterNot(identity).map(_ => "NO STATISTICS").getOrElse("")


  override def beforeAll(): Unit = {
    warehousePath = Utils.createTempDir()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (warehousePath != null) Utils.deleteRecursively(warehousePath)
  }

  override def afterEach(): Unit = {
    sql(s"DROP TABLE IF EXISTS $table")
    super.afterEach()
  }

  /**
   * Setting the java default timezone, as we use java.util.TimeZone.getDefault for partition
   * values...
   *
   * In production clusters, the default timezone is always set as UTC.
   */
  def withDefaultTimeZone(timeZoneId: String)(func: => Unit): Unit = {
    val previousTimeZone = TimeZone.getDefault()
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(timeZoneId))
      func
    } finally {
      TimeZone.setDefault(previousTimeZone)
    }
  }

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new IcebergCompatibleDeltaTestSparkSession(sparkConf)
    session.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
    session
  }

  protected override def sparkConf = super.sparkConf
    .set(
      "spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .set(
      "spark.sql.catalog.local.type", "hadoop")
    .set(
      "spark.sql.catalog.local.warehouse", warehousePath.getCanonicalPath)
    .set("spark.sql.session.timeZone", "UTC")

  protected val schemaDDL = "id bigint, data string, ts timestamp, dt date"
  protected lazy val schema = StructType.fromDDL(schemaDDL)

  protected def readIcebergHadoopTable(tablePath: String): Table = {
    // scalastyle:off deltahadoopconfiguration
    new HadoopTables(spark.sessionState.newHadoopConf).load(tablePath)
     // scalastyle:on deltahadoopconfiguration
  }
}

trait ConvertIcebergToDeltaSuiteBase
  extends QueryTest
  with ConvertIcebergToDeltaUtils
  with StatsUtils {

  import testImplicits._

  protected def convert(tableIdentifier: String, partitioning: Option[String] = None,
      collectStats: Boolean = true): Unit

  test("convert with statistics") {
      withTable(table) {
        spark.sql(
          s"""CREATE TABLE $table (id bigint, data string)
             |USING iceberg PARTITIONED BY (data)""".stripMargin)
        spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
        spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
        convert(s"iceberg.`$tablePath`", collectStats = true)

        // Check statistics
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
          .select(
            from_json(col("stats"), deltaLog.unsafeVolatileSnapshot.statsSchema).as("stats"))
          .select("stats.*")
        assert(statsDf.filter(col("numRecords").isNull).count == 0)
        val history = io.delta.tables.DeltaTable.forPath(tablePath).history()
        assert(history.count == 1)
      }
  }

  test("table with deleted files") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      spark.sql(s"DELETE FROM $table WHERE data > 'a'")
      checkAnswer(
        spark.sql(s"SELECT * from $table"), Row(1, "a") :: Nil)

      convert(s"iceberg.`$tablePath`")
      assert(SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
        spark.read.format("delta").load(tablePath).schema,
        new StructType().add("id", LongType).add("data", StringType)))
      checkAnswer(
        spark.read.format("delta").load(tablePath),
        Row(1, "a") :: Nil)
    }
  }


  test("missing iceberg library should throw a sensical error") {
    val validIcebergSparkTableClassPath = ConvertUtils.icebergSparkTableClassPath

    Seq(
      () => {
        ConvertUtils.icebergSparkTableClassPath = validIcebergSparkTableClassPath + "2"
      }).foreach { makeInvalid =>
      try {
        makeInvalid()
        withTable(table) {
          spark.sql(
            s"""CREATE TABLE $table (`1 id` bigint, 2data string)
               |USING iceberg PARTITIONED BY (2data)""".stripMargin)
          spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
          val e = intercept[DeltaIllegalStateException] {
            convert(s"iceberg.`$tablePath`")
          }
          assert(e.getErrorClass == "DELTA_MISSING_ICEBERG_CLASS")
        }
      } finally {
        ConvertUtils.icebergSparkTableClassPath = validIcebergSparkTableClassPath
      }
    }
  }

  test("non-parquet table") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)
           |TBLPROPERTIES ('write.format.default'='orc')
           |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      val e = intercept[UnsupportedOperationException] {
        convert(s"iceberg.`$tablePath`")
      }
      assert(e.getMessage.contains("Cannot convert") && e.getMessage.contains("orc"))
    }
  }

  test("external location") {
    withTempDir { dir =>
      withTable(table) {
        spark.sql(
          s"""CREATE TABLE $table (id bigint, data string)
             |USING iceberg PARTITIONED BY (data)""".stripMargin)
        spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
        spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
        ConvertToDeltaCommand(
          TableIdentifier(tablePath, Some("iceberg")),
          None,
          collectStats = true,
          Some(dir.getCanonicalPath)).run(spark)

        checkAnswer(
          spark.read.format("delta").load(dir.getCanonicalPath),
          Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
      }
    }
  }

  test("table with renamed columns") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
      spark.sql("ALTER TABLE local.db.table RENAME COLUMN id TO id2")
      spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
      convert(s"iceberg.`$tablePath`")

      // The converted delta table will get the updated schema
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          spark.read.format("delta").load(tablePath).schema,
          new StructType().add("id2", LongType).add("data", StringType)))

      // Parquet files still have the old schema
        assert(
          SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
            spark.read.format("parquet").load(tablePath + "/data").schema,
            new StructType().add("id", LongType).add("data", StringType)))

      val properties = readIcebergHadoopTable(tablePath).properties()

      // This confirms that name mapping is not used for this case
      assert(properties.get(TableProperties.DEFAULT_NAME_MAPPING) == null)

      // As of right now, the data added before rename will be nulls.
      checkAnswer(
        spark.read.format("delta").load(tablePath),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  test("columns starting with numbers") {
    val table2 = "local.db.table2"
    val tablePath2 = tablePath + "2"
    withTable(table2) {
      spark.sql(
        s"""CREATE TABLE $table2 (1id bigint, 2data string)
           |USING iceberg PARTITIONED BY (2data)""".stripMargin)
      spark.sql(s"INSERT INTO $table2 VALUES (1, 'a'), (2, 'b')")
      spark.sql(s"INSERT INTO $table2 VALUES (3, 'c')")
      assert(spark.sql(s"select * from $table2").schema ==
        new StructType().add("1id", LongType).add("2data", StringType))

      checkAnswer(
        spark.sql(s"select * from $table2"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

      val properties = readIcebergHadoopTable(tablePath2).properties()

      // This confirms that name mapping is not used for this case
      assert(properties.get(TableProperties.DEFAULT_NAME_MAPPING) == null)

      convert(s"iceberg.`$tablePath2`")
      // The converted delta table gets the updated schema
      assert(
        SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          spark.read.format("delta").load(tablePath2).schema,
          new StructType().add("1id", LongType).add("2data", StringType)))

        // parquet file schema has been modified
        assert(
          spark.read.format("parquet").load(tablePath2 + "/data").schema ==
            new StructType()
              .add("_1id", LongType)
              .add("_2data", StringType)
              // this is the partition column, which stays as-is
              .add("2data", StringType))

      checkAnswer(
        spark.read.format("delta").load(tablePath2),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
    }
  }

  test("nested schema") {
    withTable(table) {
      def createDDL(tname: String): String =
        s"""CREATE TABLE $tname (id bigint, person struct<name:string,phone:int>)
           |USING iceberg PARTITIONED BY (truncate(person.name, 2))""".stripMargin
      def insertDDL(tname: String): String =
        s"INSERT INTO $tname VALUES (1, ('aaaaa', 10)), (2, ('bbbbb', 20))"
      testNestedColumnIDs(createDDL(nestedTable), insertDDL(nestedTable))

      spark.sql(createDDL(table))

      spark.sql(s"INSERT INTO $table VALUES (1, ('aaaaa', 10)), (2, ('bbbbb', 20))")
      checkAnswer(
        spark.sql(s"SELECT * from $table"),
        Row(1, Row("aaaaa", 10)) :: Row(2, Row("bbbbb", 20)) :: Nil)

      convert(s"iceberg.`$tablePath`")

      val tblSchema = spark.read.format("delta").load(tablePath).schema

      val expectedSchema = new StructType()
        .add("id", LongType)
        .add("person", new StructType().add("name", StringType).add("phone", IntegerType))
        .add("person.name_trunc", StringType)

      assert(SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(expectedSchema, tblSchema))

      checkAnswer(
        spark.read.format("delta").load(tablePath),
        Row(1, Row("aaaaa", 10), "aa") :: Row(2, Row("bbbbb", 20), "bb") :: Nil)

      assert(
        spark.sql(s"select * from delta.`$tablePath` where person.name > 'b'")
          .inputFiles.length == 1)

      spark.sql(
        s"""
           |insert into $table (id, person)
           |values (3, struct("ccccc", 30))
           |""".stripMargin)

      val insertDataSchema = StructType.fromDDL("id bigint, person struct<name:string,phone:int>")
      val df = spark.createDataFrame(Seq(Row(3L, Row("ccccc", 30))).asJava, insertDataSchema)
      df.write.format("delta").mode("append").save(tablePath)

        checkAnswer(
          // check the raw parquet partition directories written out by Iceberg
          spark.sql(s"select * from parquet.`$tablePath/data`"),
          spark.sql(s"select * from delta.`$tablePath`")
        )
      assert(
        spark.sql(s"select * from delta.`$tablePath` where person.name > 'b'")
          .inputFiles.length == 2)
    }
  }

  private def schemaTestNoDataSkipping(
      createTableSql: String,
      initialInsertValuesSql: String,
      expectedInitialRows: Seq[Row],
      expectedSchema: StructType,
      finalInsertValuesSql: String) : Unit = {
    withTable(table) {
      spark.sql(s"DROP TABLE IF EXISTS $table")
      spark.sql(s"CREATE TABLE $table $createTableSql USING iceberg")
      spark.sql(s"INSERT INTO $table VALUES $initialInsertValuesSql")
      checkAnswer(spark.sql(s"SELECT * FROM $table"), expectedInitialRows)

      convert(s"iceberg.`$tablePath`")

      val tblSchema = spark.read.format("delta").load(tablePath).schema

      assert(SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(expectedSchema, tblSchema))

      checkAnswer(spark.read.format("delta").load(tablePath), expectedInitialRows)

      spark.sql(
        s"""
           |INSERT INTO $table
           |VALUES $finalInsertValuesSql
           |""".stripMargin)

      spark.sql(
        s"""
           |INSERT INTO delta.`$tablePath`
           |VALUES $finalInsertValuesSql
           |""".stripMargin)

        checkAnswer(
          // check the raw parquet partition directories written out by Iceberg
          spark.sql(s"SELECT * FROM parquet.`$tablePath/data`"),
          spark.sql(s"SELECT * FROM delta.`$tablePath`")
        )
    }
  }

  test("array of struct schema") {
    val createTableSql = "(id bigint, grades array<struct<class:string, score:int>>)"
    val initialInsertValuesSql = "(1, array(('mat', 10), ('cs', 90))), (2, array(('eng', 80)))"
    val expectedInitialRows = Row(1, Seq(Row("mat", 10), Row("cs", 90))) ::
      Row(2, Seq(Row("eng", 80))) :: Nil
    val arrayType = ArrayType(new StructType().add("class", StringType).add("score", IntegerType))
    val expectedSchema = new StructType()
      .add("id", LongType)
      .add("grades", arrayType)
    val finalInsertValuesSql = "(3, array(struct(\"mat\", 100), struct(\"cs\", 100)))"

    schemaTestNoDataSkipping(createTableSql, initialInsertValuesSql, expectedInitialRows,
      expectedSchema, finalInsertValuesSql)
  }

  test("map schema") {
    val createTableSql = "(id bigint, grades map<string,int>)"
    val initialInsertValuesSql = "(1, map('mat', 10, 'cs', 90)), (2, map('eng', 80))"
    val expectedInitialRows = Row(1, Map[String, Int]("mat" ->  10, "cs" -> 90)) ::
      Row(2, Map[String, Int]("eng" -> 80)) :: Nil
    val expectedSchema = new StructType()
      .add("id", LongType)
      .add("grades", MapType(StringType, IntegerType))
    val finalInsertValuesSql = "(3, map(\"mat\", 100, \"cs\", 100))"

    schemaTestNoDataSkipping(createTableSql, initialInsertValuesSql, expectedInitialRows,
      expectedSchema, finalInsertValuesSql)
  }

  test("partition schema is not allowed") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)
           |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      val e = intercept[IllegalArgumentException] {
        convert(s"iceberg.`$tablePath`", Some("data string"))
      }
      assert(e.getMessage.contains("Partition schema cannot be specified"))
    }
  }

  test("copy over Iceberg table properties") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      spark.sql(
        s"""ALTER TABLE $table SET TBLPROPERTIES(
           |  'read.split.target-size'='268435456'
           |)""".stripMargin)
      convert(s"iceberg.`$tablePath`")
      checkAnswer(
        spark.sql(s"SHOW TBLPROPERTIES delta.`$tablePath`")
          .filter(col("key").startsWith("read.")),
        Row("read.split.target-size", "268435456") :: Nil
      )
    }
  }

  test("converted table columns have metadata containing iceberg column ids") {

    val nested1 = s"""CREATE TABLE $nestedTable (name string, age int,
                   |pokemon array<struct<name:string,type:string>>)
                   |USING iceberg""".stripMargin

    val insert1 = s"""INSERT INTO $nestedTable VALUES ('Ash', 10,
                   |array(struct('Charizard', 'Fire/Flying'), struct('Pikachu', 'Electric')))
                   """.stripMargin
    testNestedColumnIDs(nested1, insert1)

    val nested2 = s"""CREATE TABLE $nestedTable (name string,
                     |info struct<region:struct<name:string,rarity:string>, id:int>)
                     |USING iceberg""".stripMargin

    val insert2 = s"""INSERT INTO $nestedTable VALUES ('Zigzagoon',
                     |struct(struct('Hoenn', 'Common'), 263))
                   """.stripMargin
    testNestedColumnIDs(nested2, insert2)

    val nested3 = s"""CREATE TABLE $nestedTable (name string,
                     |moves map<string, struct<level:int, gen:int>>)
                     |USING iceberg""".stripMargin

    val insert3 = s"""INSERT INTO $nestedTable VALUES ('Heatran',
                     |map('Fire Fang', struct(17, 7)))
                   """.stripMargin
    testNestedColumnIDs(nested3, insert3)
  }

  test("comments are retained from Iceberg") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint comment "myexample", data string comment "myexample")
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      convert(s"iceberg.`$tablePath`")

      val readSchema = spark.read.format("delta").load(tablePath).schema
      readSchema.foreach { field =>
        assert(field.getComment().contains("myexample"))
      }
    }
  }

  private def testNestedColumnIDs(createString: String, insertString: String): Unit = {
    // Nested schema
    withTable(nestedTable) {
      // Create table and insert into it
      spark.sql(createString)

      spark.sql(insertString)

      // Convert to Delta
      convert(s"iceberg.`$nestedTablePath`")

      // Check Delta schema
      val schema = DeltaLog.forTable(spark, new Path(nestedTablePath)).update().schema

      // Get initial Iceberg schema
      val icebergTable = readIcebergHadoopTable(nestedTablePath)
      val icebergSchema = icebergTable.schema()

      // Check all nested fields to see if they all have a column ID then check the iceberg schema
      // for whether that column ID corresponds to the same column name
      val columnIds = mutable.Set[Long]()
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        assert(DeltaColumnMapping.hasColumnId(field))
        // nest column ids should be distinct
        val id = DeltaColumnMapping.getColumnId(field)
        assert(!columnIds.contains(id))
        columnIds.add(id)
        // the id can either be a data schema id or a identity transform partition field
        // or it is generated bc it's a non-identity transform partition field
        assert(
          Option(icebergSchema.findField(id)).map(_.name()).contains(field.name) ||
          icebergTable.spec().fields().asScala.map(_.name()).contains(field.name)
        )
        field
      }
    }
  }

  test("conversion should fail if had partition evolution / multiple partition specs") {
    /**
     * Per https://iceberg.apache.org/evolution/#partition-evolution, if partition evolution happens
     * in Iceberg, multiple partition specs are persisted, thus convert to Delta cannot be
     * supported w/o repartitioning because Delta only supports one consistent spec
     */
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string, data2 string)
           |USING iceberg PARTITIONED BY (data)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')")
      // add new partition spec
      readIcebergHadoopTable(tablePath).updateSpec().addField("data2").commit()
      spark.sql(s"INSERT INTO $table VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')")
      // partition evolution happens, convert will fail
      val e1 = intercept[UnsupportedOperationException] {
        convert(s"iceberg.`$tablePath`")
      }
      assert(e1.getMessage.contains(IcebergTable.ERR_MULTIPLE_PARTITION_SPECS))

      // drop old partition spec
      readIcebergHadoopTable(tablePath).updateSpec().removeField("data2").commit()
      spark.sql(s"INSERT INTO $table VALUES (1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')")
      // partition spec is reverted, but partition evolution happens already
      // use assert explicitly bc we do not want checks in IcebergPartitionUtils to run first
      assert(readIcebergHadoopTable(tablePath).specs().size() > 1)
    }
  }

  test("convert Iceberg table with not null columns") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint NOT NULL, data string, name string NOT NULL)
           |USING iceberg PARTITIONED BY (id)""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a', 'b'), (2, 'b', 'c'), (3, 'c', 'd')")
      convert(s"iceberg.`$tablePath`")
      val data = spark.read.format("delta").load(tablePath)
      // verify data is converted properly
      checkAnswer(data, Seq(Row(1, "a", "b"), Row(2, "b", "c"), Row(3, "c", "d")))

      // Verify schema contains not null constraint where appropriate
      val dataSchema = data.schema
      dataSchema.foreach { field =>
        // both partition columns and data columns should have the correct nullability
        if (field.name == "id" || field.name == "name") {
          assert(!field.nullable)
        } else {
          assert(field.nullable)
        }
      }

      // Should not be able to write nulls to not null data column
      var ex = intercept[Exception] {
        spark.sql(s"INSERT INTO $table VALUES (4, 'd', null)")
      }
      assert(ex.getMessage.contains("Null value appeared in non-nullable field") ||
        // TODO: remove it after OSS 3.4 release.
        ex.getMessage.contains("""Cannot write nullable values to non-null column 'name'"""))

      // Should not be able to write nulls to not null partition column
      ex = intercept[Exception] {
        spark.sql(s"INSERT INTO $table VALUES (null, 'e', 'e')")
      }
      assert(ex.getMessage.contains("Null value appeared in non-nullable field") ||
        // TODO: remove it after OSS 3.4 release.
        ex.getMessage.contains("""Cannot write nullable values to non-null column 'id'"""))

      // Should be able to write nulls to nullable column
      spark.sql(s"INSERT INTO $table VALUES (5, null, 'e')")
    }
  }

  test("convert Iceberg table with case sensitive columns") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable(table) {
        spark.sql(
          s"""CREATE TABLE $table (i bigint NOT NULL, I string)
             |USING iceberg PARTITIONED BY (I)""".stripMargin)
        spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        val ex = intercept[UnsupportedOperationException] {
          convert(s"iceberg.`$tablePath`")
        }

        assert(ex.getMessage.contains("contains column names that only differ by case"))
      }
    }
  }

  test("should block converting Iceberg table with name mapping") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg PARTITIONED BY (data)
           |""".stripMargin
      )
      spark.sql(
        s"""ALTER TABLE $table SET TBLPROPERTIES(
           |  'schema.name-mapping.default' =
           |  '[{"field-id": 1, "names": ["my_id"]},{"field-id": 2, "names": ["my_data"]}]'
           |)""".stripMargin)

      val e = intercept[UnsupportedOperationException] {
        convert(s"iceberg.`$tablePath`")
      }
      assert(e.getMessage.contains(IcebergTable.ERR_CUSTOM_NAME_MAPPING))

    }
  }

  private def testNullPartitionValues(): Unit = {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string, dt date)
           |USING iceberg PARTITIONED BY (dt)""".stripMargin)
      spark.sql(s"INSERT INTO $table" +
        s" VALUES (1, 'a', null), (2, 'b', null), (3, 'c', cast('2021-01-03' as date))")
      convert(s"iceberg.`$tablePath`")
      val data = spark.read.format("delta").load(tablePath)
      val fmt = new SimpleDateFormat("yyyy-MM-dd")
      checkAnswer(data,
        Seq(
          Row(1, "a", null),
          Row(2, "b", null),
          Row(3, "c", new java.sql.Date(fmt.parse("2021-01-03").getTime))))
    }
  }

  test("partition columns are null") {
    withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES.key -> "false") {
      val e = intercept[RuntimeException] {
        testNullPartitionValues()
      }
      assert(e.getMessage.contains("Failed to cast partition value"))
    }

    withSQLConf(
      DeltaSQLConf.DELTA_CONVERT_PARTITION_VALUES_IGNORE_CAST_FAILURE.key -> "true",
      DeltaSQLConf.DELTA_CONVERT_ICEBERG_USE_NATIVE_PARTITION_VALUES.key -> "false") {
      testNullPartitionValues()
    }

    // default setting should work
    testNullPartitionValues()
  }

  test("arbitrary name") {
    def col(name: String): String = name + "with_special_chars_;{}()\n\t="

    // turns out Iceberg would fail when partition col names have special chars
    def partCol(name: String): String = "0123" + name

    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (
          |  `${col("data")}` int,
          |  `${partCol("part1")}` bigint,
          |  `${partCol("part2")}` string)
          |USING iceberg
          |PARTITIONED BY (
          |  `${partCol("part1")}`,
          |   truncate(`${partCol("part2")}`, 4))
          |""".stripMargin)

      spark.sql(
        s"""
           |INSERT INTO $table
           |VALUES (123, 1234567890123, 'str11')
           |""".stripMargin)

      convert(s"iceberg.`$tablePath`")

      spark.sql(
        s"""
           |INSERT INTO delta.`$tablePath`
           |VALUES (456, 4567890123456, 'str22', 'str2')
           |""".stripMargin)

      checkAnswer(spark.sql(s"select * from delta.`$tablePath`"),
        Seq(
          Row(123, 1234567890123L, "str11", "str1"),
          Row(456, 4567890123456L, "str22", "str2")))

      // projection and filter
      checkAnswer(
        spark.table(s"delta.`$tablePath`")
          .select(s"`${col("data")}`", s"`${partCol("part1")}`")
          .where(s"`${partCol("part2")}` = 'str22'"),
        Seq(Row(456, 4567890123456L)))
    }
  }

  test("partition by identity, using native partition values") {
    withDefaultTimeZone("UTC") {
      withTable(table) {
        spark.sql(
          s"""CREATE TABLE $table (
             | data_binary binary,
             | part_ts timestamp,
             | part_date date,
             | part_bool boolean,
             | part_int integer,
             | part_long long,
             | part_float float,
             | part_double double,
             | part_decimal decimal(3, 2),
             | part_string string
             | )
             |USING iceberg PARTITIONED BY (part_ts, part_date, part_bool, part_int, part_long,
             | part_float, part_double, part_decimal, part_string)""".stripMargin)

        def insertData(targetTable: String): Unit = {
          spark.sql(
            s"""
               |INSERT INTO $targetTable
               |VALUES (cast('this is binary' as binary),
               |        cast(1635728400000 as timestamp),
               |        cast('2021-11-15' as date),
               |        true,
               |        123,
               |        12345678901234,
               |        123.4,
               |        123.4,
               |        1.23,
               |        'this is a string')""".stripMargin)
        }

        insertData(table)
        withTempDir { dir =>
          val deltaPath = dir.getCanonicalPath
          ConvertToDeltaCommand(
            tableIdentifier = TableIdentifier(tablePath, Some("iceberg")),
            partitionSchema = None,
            collectStats = true,
            Some(deltaPath)).run(spark)
          // check that all the partition value types can be converted correctly
          checkAnswer(spark.table(s"delta.`$deltaPath`"), spark.table(table))

          insertData(s"delta.`$deltaPath`")
          insertData(table)
          // check that new writes to both Delta and Iceberg can be read back the same
          checkAnswer(spark.table(s"delta.`$deltaPath`"), spark.table(table))
        }
      }
    }
  }

  test("mor table without deletion files") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg
           |TBLPROPERTIES (
           |  "format-version" = "2",
           |  "write.delete.mode" = "merge-on-read"
           |)
           |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'a')")
      spark.sql(s"INSERT INTO $table VALUES (2, 'b')")
      spark.sql(s"DELETE FROM $table WHERE id = 1")
      // The two rows above should've been in separate files, and DELETE will remove all rows from
      // one file completely, in this case, we could still convert the table as Spark scan will
      // ignore the completely deleted file.
      convert(s"iceberg.`$tablePath`")
      checkAnswer(
        spark.read.format("delta").load(tablePath),
        Row(2, "b") :: Nil
      )
    }
  }

  test("block convert: mor table with deletion files") {
    def setupBulkMorTable(): Unit = {
      spark.sql(
        s"""CREATE TABLE $table (id bigint, data string)
           |USING iceberg
           |TBLPROPERTIES (
           |  "format-version" = "2",
           |  "write.delete.mode" = "merge-on-read",
           |  "write.update.mode" = "merge-on-read",
           |  "write.merge.mode" = "merge-on-read"
           |)
           |""".stripMargin)
      // Now we need to write a considerable amount of data in a dataframe fashion so Iceberg can
      // combine multiple records in one Parquet file.
      (0 until 100).map(i => (i.toLong, s"name_$i")).toDF("id", "data")
        .write.format("iceberg").mode("append").saveAsTable(table)
    }

    def assertConversionFailed(): Unit = {
      // By default, conversion should fail because it is unsafe.
      val e = intercept[UnsupportedOperationException] {
        convert(s"iceberg.`$tablePath`")
      }
      assert(e.getMessage.contains("merge-on-read"))
    }

    // --- DELETE
    withTable(table) {
      setupBulkMorTable()
      // This should touch part of one Parquet file
      spark.sql(s"DELETE FROM $table WHERE id = 1")
      // By default, conversion should fail because it is unsafe.
      assertConversionFailed()
      // Force escape should work
      withSQLConf(DeltaSQLConf.DELTA_CONVERT_ICEBERG_UNSAFE_MOR_TABLE_ENABLE.key -> "true") {
        convert(s"iceberg.`$tablePath`")
        // ... but with data duplication
        checkAnswer(
          spark.read.format("delta").load(tablePath),
          (0 until 100).map(i => Row(i.toLong, s"name_$i"))
        )
      }
    }

    // --- UPDATE
    withTable(table) {
      setupBulkMorTable()
      // This should touch part of one Parquet file
      spark.sql(s"UPDATE $table SET id = id * 2 WHERE id = 1")
      // By default, conversion should fail because it is unsafe.
      assertConversionFailed()
    }

    // --- MERGE
    withTable(table) {
      setupBulkMorTable()
      (0 until 100).filter(_ % 2 == 0)
        .toDF("id")
        .createOrReplaceTempView("tempdata")

      // This should touch part of one Parquet file
      spark.sql(
        s"""
           |MERGE INTO $table t
           |USING tempdata s
           |ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.data = "some_other"
           |""".stripMargin)
      // By default, conversion should fail because it is unsafe.
      assertConversionFailed()
    }
  }

  test("block convert: binary type partition columns") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (
           |  data int,
           |  part binary)
           |USING iceberg
           |PARTITIONED BY (part)
           |""".stripMargin)
      spark.sql(s"insert into $table values (123, cast('str1' as binary))")
      val e = intercept[UnsupportedOperationException] {
        convert(s"iceberg.`$tablePath`")
      }
      assert(e.getMessage.contains("Unsupported partition transform expression"))
    }
  }

  test("block convert: partition transform truncate decimal type") {
    withTable(table) {
      spark.sql(
        s"""CREATE TABLE $table (
           |  data int,
           |  part decimal)
           |USING iceberg
           |PARTITIONED BY (truncate(part, 3))
           |""".stripMargin)
      spark.sql(s"insert into $table values (123, 123456)")
      val e = intercept[UnsupportedOperationException] {
        convert(s"iceberg.`$tablePath`")
      }
      assert(e.getMessage.contains("Unsupported partition transform expression"))
    }
  }
}

class ConvertIcebergToDeltaScalaSuite extends ConvertIcebergToDeltaSuiteBase {
  override protected def convert(
      tableIdentifier: String,
      partitioning: Option[String] = None,
      collectStats: Boolean = true): Unit = {
    if (partitioning.isDefined) {
      io.delta.tables.DeltaTable.convertToDelta(spark, tableIdentifier, partitioning.get)
    } else {
      io.delta.tables.DeltaTable.convertToDelta(spark, tableIdentifier)
    }
  }
}

class ConvertIcebergToDeltaSQLSuite extends ConvertIcebergToDeltaSuiteBase {
  override protected def convert(
      tableIdentifier: String,
      partitioning: Option[String] = None,
      collectStats: Boolean = true): Unit = {
    val statement = partitioning.map(p => s" PARTITIONED BY ($p)").getOrElse("")
    spark.sql(s"CONVERT TO DELTA ${tableIdentifier}${statement} " +
      s"${collectStatisticsStringOption(collectStats)}")
  }

  // TODO: Move to base once DeltaAPI support collectStats parameter
  test("convert without statistics") {
    withTempDir { dir =>
      withTable(table) {
        spark.sql(
          s"""CREATE TABLE $table (id bigint, data string)
             |USING iceberg PARTITIONED BY (data)""".stripMargin)
        spark.sql(s"INSERT INTO $table VALUES (1, 'a'), (2, 'b')")
        spark.sql(s"INSERT INTO $table VALUES (3, 'c')")
        ConvertToDeltaCommand(
          TableIdentifier(tablePath, Some("iceberg")),
          None,
          collectStats = false,
          Some(dir.getCanonicalPath)).run(spark)

        // Check statistics
        val deltaLog = DeltaLog.forTable(spark, new Path(dir.getPath))
        val statsDf = deltaLog.unsafeVolatileSnapshot.allFiles
          .select(from_json(col("stats"), deltaLog.unsafeVolatileSnapshot.statsSchema).as("stats"))
          .select("stats.*")
        assert(statsDf.filter(col("numRecords").isNotNull).count == 0)
        val history = io.delta.tables.DeltaTable.forPath(dir.getPath).history()
        assert(history.count == 1)
      }
    }
  }
}
