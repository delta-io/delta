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

package io.delta.kernel

import java.util.Optional

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.delta.kernel.client.{DefaultTableClient, TableClient}
import io.delta.kernel.data.{ColumnarBatch, ColumnVector, JsonRow, Row}
import io.delta.kernel.expressions.{Expression, Literal}
import io.delta.kernel.types.{ArrayType, BooleanType, IntegerType, LongType, MapType, StringType, StructType}
import io.delta.kernel.util.GoldenTableUtils
import io.delta.kernel.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class DeltaKernelAPISuite extends AnyFunSuite with GoldenTableUtils {
  test("end-to-end usage: reading a table") {
    withGoldenTable("basic-no-checkpoint") { path =>
      val tableClient = DefaultTableClient.create(new Configuration())
      val table = Table.forPath(path)
      val snapshot = table.getLatestSnapshot(tableClient)

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema(tableClient)

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(tableClient, snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles(tableClient)
      val scanState = scanObject.getScanState(tableClient);

      // There should be just one element in the scan state
      val serializedScanState = convertColumnarBatchRowToJSON(scanState)

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq.range(start = 0, end = fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          val dataBatches = Scan.readData(
            tableClient,
            scanState,
            // convertJSONToRow(serializedScanState, scanState.getSchema),
            Utils.singletonCloseableIterator(
              convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema)),
            Optional.empty()
          )

          while(dataBatches.hasNext) {
            val batch = dataBatches.next()
            val valueColVector = batch.getData.getColumnVector(0)
            actualValueColumnValues.append(vectorToLongs(valueColVector): _*)
          }
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 90).toSet)
    }
  }

  test("end-to-end usage: reading a table with checkpoint") {
    withGoldenTable("basic-with-checkpoint") { path =>
      val tableClient = DefaultTableClient.create(new Configuration())
      val table = Table.forPath(path)
      val snapshot = table.getLatestSnapshot(tableClient)

      // Contains both the data schema and partition schema
      val tableSchema = snapshot.getSchema(tableClient)

      // Go through the tableSchema and select the columns interested in reading
      val readSchema = new StructType().add("id", LongType.INSTANCE)
      val filter = Literal.TRUE

      val scanObject = scan(tableClient, snapshot, readSchema, filter)

      val fileIter = scanObject.getScanFiles(tableClient)
      val scanState = scanObject.getScanState(tableClient);

      // There should be just one element in the scan state
      val serializedScanState = convertColumnarBatchRowToJSON(scanState)

      val actualValueColumnValues = ArrayBuffer[Long]()
      while(fileIter.hasNext) {
        val fileColumnarBatch = fileIter.next()
        Seq.range(start = 0, end = fileColumnarBatch.getSize).foreach(rowId => {
          val serializedFileInfo = convertColumnarBatchRowToJSON(fileColumnarBatch, rowId)

          // START OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
          val dataBatches = Scan.readData(
            tableClient,
            scanState,
            // convertJSONToRow(serializedScanState, scanState.getSchema),
            Utils.singletonCloseableIterator(
              convertJSONToRow(serializedFileInfo, fileColumnarBatch.getSchema)),
            Optional.empty()
          )

          while(dataBatches.hasNext) {
            val batch = dataBatches.next()
            val valueColVector = batch.getData.getColumnVector(0)
            actualValueColumnValues.append(vectorToLongs(valueColVector): _*)
          }
          // END OF THE CODE THAT WILL BE EXECUTED ON THE EXECUTOR
        })
      }
      assert(actualValueColumnValues.toSet === Seq.range(start = 0, end = 150).toSet)
    }
  }

  private def convertColumnarBatchRowToJSON(columnarBatch: ColumnarBatch, rowIndex: Int): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = columnarBatch.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getStruct(rowIndex))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getArray(rowIndex))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, columnarBatch.getColumnVector(index).getMap(rowIndex))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName,
            new Integer(columnarBatch.getColumnVector(index).getInt(rowIndex)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName,
            new java.lang.Long(columnarBatch.getColumnVector(index).getLong(rowIndex)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName,
            new java.lang.Boolean(columnarBatch.getColumnVector(index).getBoolean(rowIndex)));
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName,
            columnarBatch.getColumnVector(index).getString(rowIndex))
        } else {
          throw new UnsupportedOperationException(field.getDataType.toString)
        }
    }
    new ObjectMapper().writeValueAsString(rowObject)
  }

  private def convertColumnarBatchRowToJSON(row: Row): String = {
    val rowObject = new java.util.HashMap[String, Object]()

    import scala.collection.JavaConverters._
    val schema = row.getSchema
    schema.fields().asScala.zipWithIndex.foreach {
      case (field, index) =>
        val dataType = field.getDataType
        if (dataType.isInstanceOf[StructType]) {
          rowObject.put(field.getName, row.getRecord(index))
        } else if (dataType.isInstanceOf[ArrayType]) {
          rowObject.put(field.getName, row.getList(index))
        } else if (dataType.isInstanceOf[MapType]) {
          rowObject.put(field.getName, row.getMap(index))
        } else if (dataType.isInstanceOf[IntegerType]) {
          rowObject.put(field.getName, new Integer(row.getInt(index)))
        } else if (dataType.isInstanceOf[LongType]) {
          rowObject.put(field.getName, new java.lang.Long(row.getLong(index)))
        } else if (dataType.isInstanceOf[BooleanType]) {
          rowObject.put(field.getName, new java.lang.Boolean(row.getBoolean(index)))
//        } else if (dataType.isInstanceOf[DoubleType]) {
//          rowObject.put(field.getName,
//            new java.lang.Double(columnarBatch.getColumnVector(index).getDouble(0)))
//        } else if (dataType.isInstanceOf[FloatType]) {
//          rowObject.put(field.getName,
//            new java.lang.Float(columnarBatch.getColumnVector(index).getFloat(0)))
        } else if (dataType.isInstanceOf[StringType]) {
          rowObject.put(field.getName, row.getString(index))
        } else {
          throw new UnsupportedOperationException(field.getDataType.toString)
        }
    }

    new ObjectMapper().writeValueAsString(rowObject)
  }

  private def convertJSONToRow(json: String, readSchema: StructType): Row = {
    try {
      val jsonNode = new ObjectMapper().readTree(json)
      new JsonRow(jsonNode.asInstanceOf[ObjectNode], readSchema)
    } catch {
      case ex: JsonProcessingException =>
        throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex)
    }
  }

  private def scan(
    tableClient: TableClient,
    snapshot: Snapshot,
    readSchema: StructType,
    filter: Expression = null): Scan = {
    var builder =
      snapshot.getScanBuilder(tableClient).withReadSchema(tableClient, readSchema)
    if (filter != null) {
      builder = builder.withFilter(tableClient, filter)
    }
    builder.build()
  }

  private def vectorToInts(intColumnVector: ColumnVector): Seq[Int] = {
    Seq.range(start = 0, end = intColumnVector.getSize)
      .map(rowId => intColumnVector.getInt(rowId))
      .toSeq
  }

  private def vectorToLongs(longColumnVector: ColumnVector): Seq[Long] = {
    Seq.range(start = 0, end = longColumnVector.getSize)
      .map(rowId => longColumnVector.getLong(rowId))
      .toSeq
  }
}
