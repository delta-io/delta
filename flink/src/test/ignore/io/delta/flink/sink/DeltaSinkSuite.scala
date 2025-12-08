/*
 *  Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink

import java.io.File
import java.time.{Duration, LocalDate, LocalDateTime, ZoneOffset}
import java.util

import scala.jdk.CollectionConverters.{MapHasAsJava, SeqHasAsJava}
import scala.util.Random

import io.delta.flink.{MockHttp, TestHelper}
import io.delta.flink.table.{CatalogManagedTable, HadoopTable, UnityCatalog}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.TableManager
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.types.{ArrayType, IntegerType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterable

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.{DecimalData, GenericArrayData, GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.{logical => ftype}
import org.apache.flink.table.types.logical.{ArrayType => FlinkArrayType, DayTimeIntervalType, IntType, RowType, TinyIntType, VarCharType}
import org.apache.flink.util.InstantiationUtil
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaSinkSuite extends AnyFunSuite with TestHelper {

  def runSink(
      sink: DeltaSink,
      flinkSchema: RowType,
      rounds: Int,
      supplier: (Int) => String,
      parser: String => RowData): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(5)
    env.enableCheckpointing(100)

    // Use String to make the StreamSource serializable
    val dataList: util.List[String] = (0 until rounds).map[String](supplier(_)).toList.asJava

    val dataType = DataTypes.of(flinkSchema);
    val rowType = dataType.getLogicalType.asInstanceOf[RowType]
    val rowDataTypeInfo: TypeInformation[RowData] = InternalTypeInfo.of(rowType)

    val input: DataStream[RowData] = env
      .addSource(
        // This DataSource will prevent env from turning off before checkpoints containing data
        // is processed
        new DelayFinishTestSource[String](dataList, 2),
        Types.STRING)
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness[String](Duration.ofSeconds(1))
          .withTimestampAssigner((_, recordTs) => recordTs))
      .map[RowData](
        new MapFunction[String, RowData]() {
          override def map(value: String): RowData = parser(value)
        }).returns(rowDataTypeInfo)
    input.sinkTo(sink).uid("deltaSink")
    env.execute("DeltaSink integration test")
  }

  test("mini e2e test to empty table") {
    withTempDir { dir =>
      val tablePath = dir.getPath
      val flinkSchema = RowType.of(
        Array(
          new ftype.BooleanType(),
          new ftype.TinyIntType(),
          new ftype.SmallIntType(),
          new ftype.IntType(),
          new ftype.BigIntType(),
          new ftype.FloatType(),
          new ftype.DoubleType(),
          new ftype.DecimalType(18, 6),
          new ftype.CharType(10),
          new VarCharType(50),
          new VarCharType(VarCharType.MAX_LENGTH),
          new ftype.BinaryType(8),
          new ftype.VarBinaryType(16),
          new ftype.VarBinaryType(ftype.VarBinaryType.MAX_LENGTH),
          new ftype.DateType(),
          new ftype.TimeType(3),
          new ftype.TimestampType(3),
          new ftype.LocalZonedTimestampType(3),
          new ftype.YearMonthIntervalType(ftype.YearMonthIntervalType.YearMonthResolution.YEAR, 3),
          new ftype.DayTimeIntervalType(ftype.DayTimeIntervalType.DayTimeResolution.DAY, 3, 6)),
        Array[String](
          "f_boolean",
          "f_tinyint",
          "f_smallint",
          "f_int",
          "f_bigint",
          "f_float",
          "f_double",
          "f_decimal",
          "f_char",
          "f_varchar",
          "f_string",
          "f_binary",
          "f_varbinary",
          "f_bytes",
          "f_date",
          "f_time",
          "f_timestamp",
          "f_timestamp_ltz",
          "f_interval_ym",
          "f_interval_dt"))
      val deltaSchema = Conversions.FlinkToDelta.schema(flinkSchema)

      val deltaSink = DeltaSink.builder()
        .withTablePath(tablePath)
        .withFlinkSchema(flinkSchema)
        .withPartitionColNames(Seq.empty.asJava)
        .build()
      // Use String to make the StreamSource serializable
      val supplier = (idx: Int) =>
        s"$idx"
      val parser: String => RowData = (value: String) => {
        val idx = Integer.parseInt(value)
        val row = new GenericRowData(20)

        row.setField(0, idx % 10 == 0) // BOOLEAN
        row.setField(1, (idx % 8).toByte) // TINYINT
        row.setField(2, idx.toShort) // SMALLINT
        row.setField(3, idx) // INT
        row.setField(4, Long.MaxValue - idx) // BIGINT
        row.setField(5, 1.5f * idx) // FLOAT
        row.setField(6, 3.14159d * idx) // DOUBLE
        // DECIMAL(18,6)
        val decimal = DecimalData.fromBigDecimal(new java.math.BigDecimal("12345.678900"), 18, 6)
        row.setField(7, decimal)
        // CHAR / VARCHAR / STRING
        row.setField(8, StringData.fromString(s"char$idx"))
        row.setField(9, StringData.fromString(s"varchar$idx"))
        row.setField(10, StringData.fromString(s"string$idx"))
        // BINARY / VARBINARY / BYTES
        row.setField(11, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
        row.setField(12, Array[Byte](9, 10, 11))
        row.setField(13, Array[Byte](42))
        // DATE → days since epoch
        val days = LocalDate.of(2026, 1, 19).toEpochDay.asInstanceOf[Int]
        row.setField(14, days)
        // TIME(3) → millis of day
        val millisOfDay = 12 * 60 * 60 * 1000
        row.setField(15, millisOfDay)
        // TIMESTAMP(3)
        val ts = TimestampData.fromLocalDateTime(LocalDateTime.of(2026, 1, 19, 12, 0))
        row.setField(16, ts)
        // TIMESTAMP_LTZ(3) → epoch millis
        val epochMillis =
          LocalDateTime.of(2026, 1, 19, 12, 0).toInstant(ZoneOffset.UTC).toEpochMilli
        row.setField(17, TimestampData.fromEpochMillis(epochMillis))
        // INTERVAL YEAR TO MONTH → total months
        val months = 2 * 12 + 3 // 2 years 3 months
        row.setField(18, months)
        // INTERVAL DAY TO SECOND(3) → milliseconds
        val millis = 3L * 24 * 60 * 60 * 1000 // 3 days + 5L * 60 * 1000// 5 minutes + 123// +123 ms
        row.setField(19, millis)

        row
      }

      val rounds = 100000
      runSink(deltaSink, flinkSchema, rounds, supplier, parser)

      verifyTableContent(
        tablePath,
        (version, actions, props) => {
          val actionSeq = actions.iterator.toSeq
          assert(actionSeq.map[Long](_.getNumRecords.get()).sum == rounds)
          actionSeq.foreach(addfile => {
            readParquet(new File(tablePath).toPath.resolve(addfile.getPath), deltaSchema)
              .foreach { row =>
                val id = row.getInt(3)
                assert(row.getBoolean(0) == (id % 10 == 0))
                assert(row.getByte(1) == (id % 8))
                assert(row.getShort(2) == id.toShort)
                assert(row.getLong(4) == Long.MaxValue - id)
                assert(row.getString(8) == s"char$id")
                assert(row.getString(9) == s"varchar$id")
                assert(row.getString(10) == s"string$id")
                assert(row.getInt(14) == 20472)
                assert(row.getInt(15) == 43200000)
                assert(row.getLong(16) == 1768824000000000L)
                assert(row.getLong(17) == 1768824000000000L)
                assert(row.getInt(18) == 27)
                assert(row.getLong(19) == 259200000L)
              }
          })
        })
    }
  }

  test("min e2e test to existing table") {
    withTempDir { dir =>
      val tablePath = dir.getPath
      val defaultEngine = DefaultEngine.create(new Configuration)
      val deltaSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add(
          "content",
          new StructType()
            .add("nested_name", StringType.STRING)
            .add("nested_list", new ArrayType(StringType.STRING, true)))
        .add("part", StringType.STRING)
      // Create non-empty table
      val createdTable =
        TableManager.buildCreateTableTransaction(tablePath, deltaSchema, "dummyEngine")
          .build(defaultEngine).commit(defaultEngine, CloseableIterable.emptyIterable())
      createdTable.getPostCommitSnapshot.ifPresent(_.writeChecksum(
        defaultEngine,
        ChecksumWriteMode.SIMPLE))

      val flinkSchema = RowType.of(
        Array(
          new IntType(),
          RowType.of(
            Array(
              new VarCharType(VarCharType.MAX_LENGTH),
              new FlinkArrayType(new VarCharType(VarCharType.MAX_LENGTH))),
            Array[String]("nested_name", "nested_list")),
          new VarCharType(VarCharType.MAX_LENGTH)),
        Array[String]("id", "content", "part"))

      val deltaSink = DeltaSink.builder()
        .withTablePath(tablePath)
        .withFlinkSchema(flinkSchema)
        .withPartitionColNames(Seq.empty.asJava)
        .build()
      // Use String to make the StreamSource serializable
      val random = new Random(System.currentTimeMillis())
      val supplier = (idx: Int) =>
        s"""
           |{
           |  "id": $idx,
           |  "content": {
           |    "nested_name": "n$idx",
           |    "nested_list":
           |      [${(0 until (5 + random.nextInt(10))).map(i => s"\"d$i\"").mkString(",")}]
           |  },
           |  "part": "p${idx % 10}"
           |}""".stripMargin
      val parser: String => RowData = (value: String) => {
        val tree = new ObjectMapper().readTree(value)
        val id = tree.get("id").asInt()
        val part = StringData.fromString(tree.get("part").asText())
        val nested = tree.get("content")
        val nname = StringData.fromString(nested.get("nested_name").asText)
        val narray = nested.get("nested_list").asInstanceOf[ArrayNode]
        val list = new Array[Object](narray.size())
        for (i <- 0 until narray.size()) {
          list(i) = StringData.fromString(narray.get(i).asText())
        }
        GenericRowData.of(id, GenericRowData.of(nname, new GenericArrayData(list)), part)
      }

      val rounds = 100000
      runSink(deltaSink, flinkSchema, rounds, supplier, parser)

      // Read the table to make sure the data is correct.
      verifyTableContent(
        tablePath,
        (version, actions, props) => {
          val actionSeq = actions.iterator.toSeq
          assert(actionSeq.map[Long](_.getNumRecords.get()).sum == rounds)
          actionSeq.foreach(addfile => {
            readParquet(new File(tablePath).toPath.resolve(addfile.getPath), deltaSchema)
              .foreach { row =>
                val id = row.getInt(0)
                assert(row.getString(2) == s"p${id % 10}")
                val content = row.getStruct(1)
                assert(content.getString(0) == s"n$id")
                val array = content.getArray(1)
                assert(array.getSize <= 15)
                for (i <- 0 until array.getSize) {
                  assert(array.getElements.getString(i) == s"d$i")
                }
              }
          })
        })
    }
  }

  test("create writer and committer") {
    withTempDir { dir =>
      val tablePath = dir.getPath
      val flinkSchema = RowType.of(
        Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
        Array[String]("id", "part"))

      val deltaSink = DeltaSink.builder()
        .withTablePath(tablePath)
        .withFlinkSchema(flinkSchema)
        .withPartitionColNames(Seq("part").asJava)
        .build()

      assert(deltaSink.createWriter(new TestWriterInitContext(1, 1, 1)) != null)
      assert(deltaSink.createCommitter(new TestCommitterInitContext(1, 1, 1)) != null)
    }
  }

  test("sink is serializable") {
    withTempDir { dir =>
      val tablePath = dir.getPath
      val flinkSchema = RowType.of(
        Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
        Array[String]("id", "part"))

      val deltaSink = DeltaSink.builder()
        .withTablePath(tablePath)
        .withFlinkSchema(flinkSchema)
        .withPartitionColNames(Seq("part").asJava)
        .build()
      val serialized: Array[Byte] = InstantiationUtil.serializeObject(deltaSink)
      val copy = InstantiationUtil.deserializeObject(serialized, getClass.getClassLoader)
        .asInstanceOf[DeltaSink]
      assert(copy != null)
    }
  }

  test("sink builder") {
    val flinkSchema = RowType.of(
      Array(new IntType(), new VarCharType(VarCharType.MAX_LENGTH)),
      Array[String]("id", "part"))
    val sink1 = DeltaSink.builder()
      .withFlinkSchema(flinkSchema)
      .withConfigurations(
        Map("type" -> "hadoop", "hadoop.table_path" -> "file:///table-path").asJava)
      .build()
    assert(sink1.getTable.isInstanceOf[HadoopTable])
    assert(sink1.getTable.asInstanceOf[HadoopTable].getTablePath.toString == "file:///table-path/")

    withTempDir { dir =>
      val dummyHttp = MockHttp.forUC(dir.getAbsolutePath)

      val sink2 = DeltaSink.builder().withFlinkSchema(flinkSchema)
        .withConfigurations(
          Map(
            "type" -> "unitycatalog",
            "unitycatalog.name" -> "ab",
            "unitycatalog.table_name" -> "ab.cd.ef",
            "unitycatalog.endpoint" -> s"http://localhost:${dummyHttp.port()}/",
            "unitycatalog.token" -> "wow").asJava).build()
      assert(sink2.getTable.isInstanceOf[CatalogManagedTable])
      val table = sink2.getTable.asInstanceOf[CatalogManagedTable]
      assert(table.getCatalog.isInstanceOf[UnityCatalog])
      assert(table.getId == "ab.cd.ef")
      assert(table.getCatalog.asInstanceOf[UnityCatalog].getName == "ab")
    }
  }
}
