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
package io.delta.kernel.internal.actions

import io.delta.kernel.data.Row
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import org.scalatest.funsuite.AnyFunSuite

import java.util.{HashMap => JHashMap}
import java.util.Optional
import scala.collection.JavaConverters._

class AddFileSuite extends AnyFunSuite {

  /**
   * Generate a GenericRow representing an AddFile action with provided fields.
   */
  private def generateTestAddFileRow(
      path: String = "path",
      partitionValues: Map[String, String] = Map.empty,
      size: Long = 10L,
      modificationTime: Long = 20L,
      dataChange: Boolean = true,
      deletionVector: Option[DeletionVectorDescriptor] = Option.empty,
      tags: Option[Map[String, String]] = Option.empty,
      baseRowId: Option[Long] = Option.empty,
      defaultRowCommitVersion: Option[Long] = Option.empty,
      stats: Option[String] = Option.empty
  ): Row = {
    // Generate a GenericRow representing an AddFile action with provided values for testing
    val schema = AddFile.FULL_SCHEMA
    val nameToOrdinal: Map[String, Int] = (0 until schema.length).map { i =>
      schema.at(i).getName -> i
    }.toMap

    val valueMap = new JHashMap[Integer, Object]()

    valueMap.put(nameToOrdinal("path"), path)
    valueMap.put(nameToOrdinal("partitionValues"), stringStringMapValue(partitionValues.asJava))
    valueMap.put(nameToOrdinal("size"), size.asInstanceOf[java.lang.Long])
    valueMap.put(nameToOrdinal("modificationTime"), modificationTime.asInstanceOf[java.lang.Long])
    valueMap.put(nameToOrdinal("dataChange"), dataChange.asInstanceOf[java.lang.Boolean])

    if (deletionVector.isDefined) {
      // DeletionVectorDescriptor currently does not provide a way to convert to a Row
      assert(false, "DeletionVectorDescriptor is not supported in AddFileSuite")
    }
    if (tags.isDefined) {
      valueMap.put(nameToOrdinal("tags"), stringStringMapValue(tags.get.asJava))
    }
    if (baseRowId.isDefined) {
      valueMap.put(nameToOrdinal("baseRowId"), baseRowId.get.asInstanceOf[java.lang.Long])
    }
    if (defaultRowCommitVersion.isDefined) {
      valueMap.put(
        nameToOrdinal("defaultRowCommitVersion"),
        defaultRowCommitVersion.get.asInstanceOf[java.lang.Long]
      )
    }
    if (stats.isDefined) {
      valueMap.put(nameToOrdinal("stats"), stats.get)
    }

    new GenericRow(schema, valueMap)
  }

  test("getters can read AddFile's fields from the backing row") {
    val addFileRow = generateTestAddFileRow(
      path = "test/path",
      partitionValues = Map("a" -> "1"),
      size = 1L,
      modificationTime = 10L,
      dataChange = true,
      deletionVector = Option.empty,
      tags = Option(Map("tag1" -> "value1")),
      baseRowId = Option(30L),
      defaultRowCommitVersion = Option(40L),
      stats = Option("{\"numRecords\":100}")
    )

    val addFile = new AddFile(addFileRow)

    assert(addFile.getPath === "test/path")
    assert(
      VectorUtils.toJavaMap(addFile.getPartitionValues).asScala.equals(Map("a" -> "1"))
    )
    assert(addFile.getSize === 1L)
    assert(addFile.getModificationTime === 10L)
    assert(addFile.getDataChange === true)
    assert(addFile.getDeletionVector === Optional.empty())
    assert(
      VectorUtils.toJavaMap(addFile.getTags.get()).asScala.equals(Map("tag1" -> "value1"))
    )
    assert(addFile.getBaseRowId === Optional.of(30L))
    assert(addFile.getDefaultRowCommitVersion === Optional.of(40L))
    // DataFileStatistics doesn't have an equals() override, so we need to compare the string
    assert(addFile.getStats.get().toString === "{\"numRecords\":100}")
    assert(addFile.getNumRecords === Optional.of(100L))
  }

  test("update a single field of an AddFile") {
    val addFileRow = generateTestAddFileRow(baseRowId = Option(1L))
    val addFileAction = new AddFile(addFileRow)

    val updatedAddFileAction = addFileAction.withNewBaseRowId(2L)
    assert(updatedAddFileAction.getBaseRowId === Optional.of(2L))

    val updatedAddFileRow = updatedAddFileAction.toRow
    assert(new AddFile(updatedAddFileRow).getBaseRowId === Optional.of(2L))
  }

  test("update multiple fields of an AddFile multiple times") {
    val baseAddFileRow =
      generateTestAddFileRow(
        path = "test/path",
        baseRowId = Option(0L),
        defaultRowCommitVersion = Option(0L)
      )
    var addFileAction = new AddFile(baseAddFileRow)

    (1L until 10L).foreach { i =>
      addFileAction = addFileAction
        .withNewBaseRowId(i)
        .withNewDefaultRowCommitVersion(i * 10)

      assert(addFileAction.getPath === "test/path")
      assert(addFileAction.getBaseRowId === Optional.of(i))
      assert(addFileAction.getDefaultRowCommitVersion === Optional.of(i * 10))
    }
  }

  test("toString correctly prints out AddFile and with partition values / tags in sorted order") {
    val addFileRow = generateTestAddFileRow(
      path = "test/path",
      partitionValues = Map("b" -> "2", "a" -> "1"),
      size = 100L,
      modificationTime = 1234L,
      dataChange = true,
      tags = Option(Map("tag1" -> "value1", "tag2" -> "value2")),
      baseRowId = Option(12345L),
      defaultRowCommitVersion = Option(67890L),
      stats = Option("{\"numRecords\":10000}")
    )
    val addFile = new AddFile(addFileRow)
    val expectedString = "AddFile{" +
      "path='test/path', " +
      "partitionValues={a=1, b=2}, " +
      "size=100, " +
      "modificationTime=1234, " +
      "dataChange=true, " +
      "deletionVector=Optional.empty, " +
      "tags=Optional[{tag1=value1, tag2=value2}], " +
      "baseRowId=Optional[12345], " +
      "defaultRowCommitVersion=Optional[67890], " +
      "stats=Optional[{\"numRecords\":10000}]}"
    assert(addFile.toString == expectedString)
  }
}
