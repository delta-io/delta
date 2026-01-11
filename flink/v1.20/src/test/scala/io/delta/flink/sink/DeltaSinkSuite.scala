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

import java.util

import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.kernel.Table
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.ScanImpl
import io.delta.kernel.internal.actions.AddFile

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.{IntType, RowType, VarCharType}
import org.apache.flink.util.InstantiationUtil
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaSinkSuite extends AnyFunSuite with TestHelper {

  test("mini e2e test to empty table") {
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

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(5)
      env.enableCheckpointing(100)

      // Use String to make the StreamSource serializable
      val dataList: util.List[String] = (0 until 100000).map[String] { idx =>
        s"$idx,p${idx % 10}"
      }.toList.asJava

      val dataType = DataTypes.of(flinkSchema);
      val rowType = dataType.getLogicalType.asInstanceOf[RowType]
      val rowDataTypeInfo: TypeInformation[RowData] = InternalTypeInfo.of(rowType)

      val input: DataStream[RowData] = env
        .addSource(
          // This DataSource will prevent env from turning off before checkpoints containing data
          // is processed
          new DelayFinishTestSource[String](dataList, 2),
          Types.STRING).map[RowData](
          new MapFunction[String, RowData]() {
            override def map(value: String): RowData = {
              val parts = value.split(",")
              GenericRowData.of(Integer.valueOf(parts(0)), StringData.fromString(parts(1)))
            }
          }).returns(rowDataTypeInfo)

      input.sinkTo(deltaSink).uid("deltaSink")

      env.execute("DeltaSink integration test")

      // Read the table to make sure the data is correct.
      val engine = DefaultEngine.create(new Configuration())
      val table = Table.forPath(engine, tablePath)
      val scan = table.getLatestSnapshot(engine).getScanBuilder.build()
      // AddFiles
      val results = scan.asInstanceOf[ScanImpl]
        .getScanFiles(engine, true)
        .asScala.flatMap { file =>
          file.getData.getRows.toInMemoryList.asScala
            .filter { _.getStruct(0) != null }
            .map { row => new AddFile(row.getStruct(0)) }
        }.toList

      val partitions = results.map(_.getPartitionValues.getValues.getString(0)).toSet
      assert((0 until 10).map { i => "p" + i }.toSet == partitions)
      assert(100000 == results.map(_.getNumRecords.get().longValue()).sum)
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

      val writer = deltaSink.createWriter(new TestWriterInitContext(1, 1, 1))
      val committer = deltaSink.createCommitter(new TestCommitterInitContext(1, 1, 1))
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
}
