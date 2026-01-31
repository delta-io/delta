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

import java.math.{MathContext, RoundingMode}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time._

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsJava, SeqHasAsJava}

import io.delta.flink.TestHelper
import io.delta.flink.table.HadoopTable
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.types._

import org.apache.flink.table.data._
import org.apache.flink.table.runtime.typeutils.RowDataSerializer
import org.apache.flink.table.types.logical.{IntType, RowType, VarCharType}
import org.scalatest.funsuite.AnyFunSuite

class DeltaWriterTaskSuite extends AnyFunSuite with TestHelper {

  test("write to empty table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()

      val partitionValues = Map("part" -> Literal.ofString("p0")).asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      for (i <- 0 until 10) {
        writerTask.write(
          GenericRowData.of(i, StringData.fromString("p0")),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        assert(1 == result.getDeltaActions.size())
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        assert(addFile.getPath.contains("test-job-id-2-0"))
        // Stats are present
        assert(10 == addFile.getNumRecords.get())
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath
        assert(Files.exists(fullPath))

        val partitionMap = addFile.getPartitionValues
        assert(1 == partitionMap.getSize)
        assert("part" == partitionMap.getKeys.getString(0))
        assert("p0" == partitionMap.getValues.getString(0))

        assert(900 == result.getContext.getHighWatermark)
        assert(0 == result.getContext.getLowWatermark)
        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(10 == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))
        }
      })
    }
  }

  test("write to existing table") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List("part").asJava)
      table.open()
      val partitionValues = Map("part" -> Literal.ofString("p0")).asJava
      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      for (i <- 0 until 10) {
        writerTask.write(
          GenericRowData.of(i, StringData.fromString("p0")),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        assert(1 == result.getDeltaActions.size())
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        assert(addFile.getPath.contains("test-job-id-2-0"))
        assert(10 == addFile.getNumRecords.get())
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath
        assert(Files.exists(fullPath))

        assert(900 == result.getContext.getHighWatermark)
        assert(0 == result.getContext.getLowWatermark)

        val partitionMap = addFile.getPartitionValues
        assert(1 == partitionMap.getSize)
        assert("part" == partitionMap.getKeys.getString(0))
        assert("p0" == partitionMap.getValues.getString(0))

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(10 == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))
        }
      })
    }
  }

  test("write primitive types") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("i", IntegerType.INTEGER)
        .add("b", BooleanType.BOOLEAN)
        .add("s", StringType.STRING)
        .add("bin", BinaryType.BINARY)
        .add("l", LongType.LONG)
        .add("f", FloatType.FLOAT)
        .add("d", DoubleType.DOUBLE)
        .add("dec", new DecimalType(10, 2))
        .add("dt", DateType.DATE)
        .add("ts", TimestampType.TIMESTAMP)
        .add("tsn", TimestampNTZType.TIMESTAMP_NTZ)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)
      val numRecords = 15
      for (i <- 0 until numRecords) {
        if (i % 10 == 0) {
          writerTask.write(
            GenericRowData.of(null, null, null, null, null, null,
              null, null, null, null, null, null),
            new TestSinkWriterContext(i * 100, i * 100))
        } else {
          writerTask.write(
            GenericRowData.of(
              i,
              i % 2 == 0,
              StringData.fromString("p" + i),
              ("binary data " + i).getBytes(StandardCharsets.UTF_8),
              100L + i,
              3.75f + i,
              4.28 + i,
              DecimalData.fromBigDecimal(new java.math.BigDecimal("" + i + "3.17"), 10, 2),
              LocalDate.of(2025, 10, 10 + i).toEpochDay().asInstanceOf[Int],
              TimestampData.fromInstant(Instant.parse("2025-10-" + (10 + i) + "T00:00:00Z")),
              TimestampData.fromLocalDateTime(
                LocalDateTime.of(2025, 10, 10, i, 0, 0))),
            new TestSinkWriterContext(i * 100, i * 100))
        }
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(numRecords == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          if (idx % 10 == 0) {
            (0 until 11).foreach { i => assert(row.isNullAt(i)) }
          } else {
            assert(idx == row.getInt(0))
            assert((idx % 2 == 0) == row.getBoolean(1))
            assert("p" + idx == row.getString(2))
            assert("binary data " + idx == new String(row.getBinary(3), StandardCharsets.UTF_8))
            assert(100L + idx == row.getLong(4))
            assert(3.75f + idx == row.getFloat(5))
            assert(4.28 + idx == row.getDouble(6))
            assert(new java.math.BigDecimal("" + idx + "3.17").setScale(2, RoundingMode.HALF_UP)
              .round(new MathContext(10, RoundingMode.HALF_UP)) == row.getDecimal(7))
            assert(LocalDate.of(2025, 10, 10 + idx).toEpochDay().asInstanceOf[Int] == row.getInt(8))
            assert(Instant.parse("2025-10-" + (10 + idx) + "T00:00:00Z").toEpochMilli * 1000 ==
              row.getLong(9))
            assert(LocalDateTime.of(2025, 10, 10, idx, 0, 0)
              .toInstant(ZonedDateTime.now(ZoneId.of("UTC")).getOffset()).toEpochMilli * 1000 ==
              row.getLong(10))
          }
        }
      })
    }
  }

  test("write nested struct types") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val nestedStructType = new StructType()
        .add("nested_id", IntegerType.INTEGER)
        .add("nested_name", StringType.STRING)
      val nestedStructType2 = new StructType()
        .add("nested_sth", IntegerType.INTEGER)
        .add("nested2", nestedStructType)
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("nested", nestedStructType2)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      val numRecords = 15
      for (i <- 0 until numRecords) {
        if (i % 4 == 0) {
          writerTask.write(GenericRowData.of(i, null), new TestSinkWriterContext(i * 100, i * 100))
        } else if (i % 7 == 0) {
          writerTask.write(
            GenericRowData.of(i, GenericRowData.of(i, null)),
            new TestSinkWriterContext(i * 100, i * 100))
        } else {
          writerTask.write(
            GenericRowData.of(
              i,
              GenericRowData.of(i, GenericRowData.of(i, StringData.fromString("p" + i)))),
            new TestSinkWriterContext(i * 100, i * 100))
        }
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(numRecords == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))
          if (idx % 4 == 0) {
            assert(row.isNullAt(1))
          } else if (idx % 7 == 0) {
            assert(idx == row.getStruct(1).getInt(0))
            assert(row.getStruct(1).isNullAt(1))
          } else {
            assert(idx == row.getStruct(1).getInt(0))
            assert(idx == row.getStruct(1).getStruct(1).getInt(0))
            assert("p" + idx == row.getStruct(1).getStruct(1).getString(1))
          }
        }
      })
    }
  }

  test("write list") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val nestedStructType = new StructType()
        .add("nested_id", IntegerType.INTEGER)
        .add("nested_name", StringType.STRING)
        .add("nested_list", new ArrayType(IntegerType.INTEGER, true))
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("intlist", new ArrayType(IntegerType.INTEGER, true))
        .add("nestedlist", new ArrayType(nestedStructType, true))

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      val numRecords = 15
      for (i <- 0 until numRecords) {
        writerTask.write(
          GenericRowData.of(
            i,
            new GenericArrayData(
              Array[Object](
                Integer.valueOf(1),
                null,
                Integer.valueOf(3 * i))),
            new GenericArrayData(
              Array[Object](
                GenericRowData.of(
                  100 + i,
                  StringData.fromString("1p" + i),
                  null),
                null,
                GenericRowData.of(
                  300 + i,
                  StringData.fromString("3p" + i),
                  new GenericArrayData(
                    Array[Object](
                      Integer.valueOf(i),
                      Integer.valueOf(i + 1),
                      null,
                      Integer.valueOf(4 * i))))))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(numRecords == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))
          assert(3 == row.getArray(1).getSize)
          assert(1 == row.getArray(1).getElements.getInt(0))
          assert(row.getArray(1).getElements.isNullAt(1))
          assert(3 * idx == row.getArray(1).getElements.getInt(2))

          assert(3 == row.getArray(2).getSize)
          val elements = row.getArray(2).getElements

          assert(100 + idx == elements.getChild(0).getInt(0))
          assert("1p" + idx == elements.getChild(1).getString(0))
          assert(elements.getChild(2).isNullAt(0))

          assert(elements.getChild(0).isNullAt(1))
          assert(elements.getChild(1).isNullAt(1))
          assert(elements.getChild(2).isNullAt(1))

          assert(300 + idx == elements.getChild(0).getInt(2))
          assert("3p" + idx == elements.getChild(1).getString(2))
          val subarray = elements.getChild(2).getArray(2)
          assert(4 == subarray.getSize)
          assert(idx == subarray.getElements.getInt(0))
          assert(idx + 1 == subarray.getElements.getInt(1))
          assert(subarray.getElements.isNullAt(2))
          assert(idx * 4 == subarray.getElements.getInt(3))
        }
      })
    }
  }

  test("write list of all types") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("i", new ArrayType(IntegerType.INTEGER, true))
        .add("b", new ArrayType(BooleanType.BOOLEAN, true))
        .add("s", new ArrayType(StringType.STRING, true))
        .add("bin", new ArrayType(BinaryType.BINARY, true))
        .add("l", new ArrayType(LongType.LONG, true))
        .add("f", new ArrayType(FloatType.FLOAT, true))
        .add("d", new ArrayType(DoubleType.DOUBLE, true))
        .add("dec", new ArrayType(new DecimalType(10, 2), true))
        .add("dt", new ArrayType(DateType.DATE, true))
        .add("ts", new ArrayType(TimestampType.TIMESTAMP, true))
        .add("tsn", new ArrayType(TimestampNTZType.TIMESTAMP_NTZ, true))

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)
      val numRecords = 15
      for (i <- 0 until numRecords) {
        writerTask.write(
          GenericRowData.of(
            new GenericArrayData(Array(i)),
            new GenericArrayData(Array(i % 2 == 0)),
            new GenericArrayData(Array[Object](StringData.fromString("p" + i))),
            new GenericArrayData(
              Array[Object](("binary data " + i).getBytes(StandardCharsets.UTF_8))),
            new GenericArrayData(Array(100L + i)),
            new GenericArrayData(Array(3.75f + i)),
            new GenericArrayData(Array(4.28 + i)),
            new GenericArrayData(
              Array[Object](
                DecimalData.fromBigDecimal(new java.math.BigDecimal("" + i + "3.17"), 10, 2))),
            new GenericArrayData(
              Array(LocalDate.of(2025, 10, 10 + i).toEpochDay().intValue)),
            new GenericArrayData(
              Array[Object](
                TimestampData.fromInstant(Instant.parse("2025-10-" + (10 + i) + "T00:00:00Z")))),
            new GenericArrayData(Array[Object](
              TimestampData.fromLocalDateTime(LocalDateTime.of(2025, 10, 10, i, 0, 0))))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(numRecords == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getArray(0).getElements.getInt(0))
          assert((idx % 2 == 0) == row.getArray(1).getElements.getBoolean(0))
          assert("p" + idx == row.getArray(2).getElements.getString(0))
          assert("binary data " + idx ==
            new String(row.getArray(3).getElements.getBinary(0), StandardCharsets.UTF_8))
          assert(100L + idx == row.getArray(4).getElements.getLong(0))
          assert(3.75f + idx == row.getArray(5).getElements.getFloat(0))
          assert(4.28 + idx == row.getArray(6).getElements.getDouble(0))
          assert(new java.math.BigDecimal("" + idx + "3.17").setScale(2, RoundingMode.HALF_UP)
            .round(new MathContext(10, RoundingMode.HALF_UP)) ==
            row.getArray(7).getElements.getDecimal(0))
          assert(LocalDate.of(2025, 10, 10 + idx).toEpochDay().asInstanceOf[Int] ==
            row.getArray(8).getElements.getInt(0))
          assert(Instant.parse("2025-10-" + (10 + idx) + "T00:00:00Z").toEpochMilli * 1000 ==
            row.getArray(9).getElements.getLong(0))
          assert(LocalDateTime.of(2025, 10, 10, idx, 0, 0)
            .toInstant(ZonedDateTime.now(ZoneId.of("UTC")).getOffset()).toEpochMilli * 1000 ==
            row.getArray(10).getElements.getLong(0))
        }
      })
    }
  }

  test("write map") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val nestedStructType = new StructType()
        .add("nested_id", IntegerType.INTEGER)
        .add("nested_name", StringType.STRING)
        .add("nested_map", new MapType(IntegerType.INTEGER, StringType.STRING, true))
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("simplemap", new MapType(IntegerType.INTEGER, StringType.STRING, true))
        .add("structmap", new MapType(IntegerType.INTEGER, nestedStructType, true))

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      val numRecords = 15
      for (i <- 0 until numRecords) {
        writerTask.write(
          GenericRowData.of(
            i,
            new GenericMapData(
              Map(
                i -> StringData.fromString("p" + i),
                i + 1 -> StringData.fromString("q" + i),
                i + 2 -> null,
                i + 3 -> StringData.fromString("w" + i)).asJava),
            new GenericMapData(
              Map(
                i -> GenericRowData.of(
                  i + 100,
                  StringData.fromString("n" + i),
                  null),
                i + 1 -> null,
                i + 2 -> GenericRowData.of(
                  i + 200,
                  StringData.fromString("m" + i),
                  new GenericMapData(
                    Map(i -> StringData.fromString("pwd" + i)).asJava))).asJava)),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(numRecords == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(idx == row.getInt(0))

          assert(4 == row.getMap(1).getSize)
          assert(idx == row.getMap(1).getKeys.getInt(0))
          assert(idx + 1 == row.getMap(1).getKeys.getInt(1))
          assert(idx + 2 == row.getMap(1).getKeys.getInt(2))
          assert(idx + 3 == row.getMap(1).getKeys.getInt(3))
          assert("p" + idx == row.getMap(1).getValues.getString(0))
          assert("q" + idx == row.getMap(1).getValues.getString(1))
          assert(row.getMap(1).getValues.isNullAt(2))
          assert("w" + idx == row.getMap(1).getValues.getString(3))

          assert(3 == row.getMap(2).getSize)
          val keys = row.getMap(2).getKeys
          val values = row.getMap(2).getValues

          assert(idx == keys.getInt(0))
          assert(idx + 1 == keys.getInt(1))
          assert(idx + 2 == keys.getInt(2))
          assert(idx + 100 == values.getChild(0).getInt(0))
          assert(values.getChild(0).isNullAt(1))
          assert(idx + 200 == values.getChild(0).getInt(2))

          assert("n" + idx == values.getChild(1).getString(0))
          assert(values.getChild(1).isNullAt(1))
          assert("m" + idx == values.getChild(1).getString(2))

          assert(values.getChild(2).isNullAt(0))
          assert(values.getChild(2).isNullAt(1))
          assert(1 == values.getChild(2).getMap(2).getSize)
          assert(idx == values.getChild(2).getMap(2).getKeys.getInt(0))
          assert("pwd" + idx == values.getChild(2).getMap(2).getValues.getString(0))
        }
      })
    }
  }

  test("write complex combination") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val nestedStructType = new StructType()
        .add(
          "nested_map",
          new MapType(
            IntegerType.INTEGER,
            new ArrayType(StringType.STRING, true),
            true))
      val schema = new StructType()
        .add(
          "base",
          new ArrayType(
            new MapType(IntegerType.INTEGER, nestedStructType, true),
            true))

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val conf = new DeltaSinkConf(schema, Map.empty[String, String].asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      val numRecords = 15
      for (i <- 0 until numRecords) {
        writerTask.write(
          GenericRowData.of(
            new GenericArrayData(
              Array[Object](
                new GenericMapData(
                  Map(i -> GenericRowData.of(
                    new GenericMapData(Map(
                      i -> new GenericArrayData(
                        Array[Object](
                          StringData.fromString("p" + i),
                          StringData.fromString("q" + i)))).asJava))).asJava),
                new GenericMapData(
                  Map(i + 1 -> GenericRowData.of(
                    new GenericMapData(Map(
                      i -> new GenericArrayData(
                        Array[Object](StringData.fromString("w" + i)))).asJava))).asJava)))),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      assert(1 == results.size())
      results.forEach(result => {
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        assert(numRecords == rows.size)
        rows.zipWithIndex.iterator.foreach { case (row, idx) =>
          assert(2 == row.getArray(0).getSize)
          val map1 = row.getArray(0).getElements.getMap(0)
          val map2 = row.getArray(0).getElements.getMap(1)

          assert(1 == map1.getSize)
          assert(idx == map1.getKeys.getInt(0))
          assert(1 == map1.getValues.getChild(0).getMap(0).getSize)
          assert(idx == map1.getValues.getChild(0).getMap(0).getKeys.getInt(0))
          assert(2 == map1.getValues.getChild(0).getMap(0).getValues.getArray(0).getSize)
          val values = map1.getValues.getChild(0).getMap(0).getValues
          assert("p" + idx == values.getArray(0).getElements.getString(0))
          assert("q" + idx == values.getArray(0).getElements.getString(1))

          assert(1 == map2.getSize)
          assert(idx + 1 == map2.getKeys.getInt(0))
          assert(1 == map2.getValues.getChild(0).getMap(0).getSize)
          assert(idx == map2.getValues.getChild(0).getMap(0).getKeys.getInt(0))
          assert(1 == map2.getValues.getChild(0).getMap(0).getValues.getArray(0).getSize)
          val values2 = map2.getValues.getChild(0).getMap(0).getValues
          assert("w" + idx == values2.getArray(0).getElements.getString(0))
        }
      })
    }
  }

  test("file rolling by size") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val flinkSchema = RowType.of(
        Array(new IntType, new VarCharType(VarCharType.MAX_LENGTH)),
        Array("id", "part"))
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val rollSize = 500
      val rowCount = 1000

      val conf = new DeltaSinkConf(
        schema,
        Map(
          "file_rolling.strategy" -> "size",
          "file_rolling.size" -> s"$rollSize").asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      var fileCounter = 0
      var sizeCounter = 0L

      for (i <- 0 until rowCount) {
        val binaryRow = new RowDataSerializer(flinkSchema).toBinaryRow(
          GenericRowData.of(i, StringData.fromString("p" + i)))
        sizeCounter += binaryRow.getSizeInBytes
        if (sizeCounter >= rollSize) {
          sizeCounter = 0
          fileCounter += 1
        }
        writerTask.write(
          binaryRow,
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      if (sizeCounter != 0) {
        fileCounter += 1
      }

      assert(fileCounter == results.size())
      val ids = results.asScala.flatMap { result =>
        assert(1 == result.getDeltaActions.size())
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath
        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        rows.iterator.map(_.getInt(0)).toSeq
      }.toSet
      assert(ids.size == rowCount)
    }
  }

  test("file rolling by count") {
    withTempDir { dir =>
      val tablePath = dir.getAbsolutePath
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("part", StringType.STRING)

      val table = new HadoopTable(
        URI.create(tablePath),
        Map.empty[String, String].asJava,
        schema,
        List().asJava)
      table.open()
      val partitionValues = Map.empty[String, Literal].asJava

      val rollSize = 200
      val rowCount = 1001

      val conf = new DeltaSinkConf(
        schema,
        Map(
          "file_rolling.strategy" -> "count",
          "file_rolling.count" -> s"$rollSize").asJava)
      val writerTask = new DeltaWriterTask(
        /* jobId= */ "test-job-id",
        /* subtaskId= */ 2,
        /* attemptNumber= */ 0,
        /* table = */ table,
        /* conf = */ conf,
        /* partitionValues= */ partitionValues)

      for (i <- 0 until rowCount) {
        writerTask.write(
          GenericRowData.of(i, StringData.fromString("p0")),
          new TestSinkWriterContext(i * 100, i * 100))
      }
      val results = writerTask.complete()

      val expectedFileCount = rowCount / rollSize + 1
      assert(expectedFileCount == results.size())
      assert(results.asScala.zipWithIndex.flatMap { case (result, idx) =>
        assert(1 == result.getDeltaActions.size())
        val action = result.getDeltaActions.get(0)
        val addFile = new AddFile(action.getStruct(SingleAction.ADD_FILE_ORDINAL))
        val fullPath = dir.toPath.resolve(addFile.getPath).toAbsolutePath

        assert(result.getContext.getHighWatermark ==
          Math.min((rowCount - 1) * 100, (((idx + 1) * rollSize) - 1) * 100))
        assert(result.getContext.getLowWatermark == 0)

        // check the Parquet file content
        val rows = readParquet(fullPath, schema)
        rows.iterator.map(_.getInt(0)).toSeq
      }.toSet.size == rowCount)
    }
  }
}
