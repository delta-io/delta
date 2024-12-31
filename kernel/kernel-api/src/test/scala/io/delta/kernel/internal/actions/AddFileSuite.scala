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

import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.AddFile.createAddFileRow
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import io.delta.kernel.utils.DataFileStatistics.deserializeFromJson

import java.util.Optional
import java.lang.{Boolean => JBoolean, Long => JLong}

class AddFileSuite extends AnyFunSuite {

  /**
   * Generate a Row representing an AddFile action with provided fields.
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
    def toJavaOptional[T](option: Option[T]): Optional[T] = option match {
      case Some(value) => Optional.of(value)
      case None => Optional.empty()
    }

    createAddFileRow(
      path,
      stringStringMapValue(partitionValues.asJava),
      size.asInstanceOf[JLong],
      modificationTime.asInstanceOf[JLong],
      dataChange.asInstanceOf[JBoolean],
      toJavaOptional(deletionVector),
      toJavaOptional(tags.map(_.asJava).map(stringStringMapValue)),
      toJavaOptional(baseRowId.asInstanceOf[Option[JLong]]),
      toJavaOptional(defaultRowCommitVersion.asInstanceOf[Option[JLong]]),
      deserializeFromJson(stats.getOrElse(""))
    )
  }

  test("getters can read AddFile's fields from the backing row") {
    val addFileRow = generateTestAddFileRow(
      path = "test/path",
      partitionValues = Map("a" -> "1"),
      size = 1L,
      modificationTime = 10L,
      dataChange = false,
      deletionVector = Option.empty,
      tags = Option(Map("tag1" -> "value1")),
      baseRowId = Option(30L),
      defaultRowCommitVersion = Option(40L),
      stats = Option("{\"numRecords\":100}")
    )

    val addFile = new AddFile(addFileRow)
    assert(addFile.getPath === "test/path")
    assert(VectorUtils.toJavaMap(addFile.getPartitionValues).asScala.equals(Map("a" -> "1")))
    assert(addFile.getSize === 1L)
    assert(addFile.getModificationTime === 10L)
    assert(addFile.getDataChange === false)
    assert(addFile.getDeletionVector === Optional.empty())
    assert(VectorUtils.toJavaMap(addFile.getTags.get()).asScala.equals(Map("tag1" -> "value1")))
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

  test("toString() prints all fields of AddFile with partitionValues / tags in sorted order") {
    val addFileRow = generateTestAddFileRow(
      path = "test/path",
      partitionValues = Map("b" -> "2", "a" -> "1", "c" -> "3"),
      size = 100L,
      modificationTime = 1234L,
      dataChange = false,
      tags = Option(Map("tag1" -> "value1", "tag2" -> "value2")),
      baseRowId = Option(12345L),
      defaultRowCommitVersion = Option(67890L),
      stats = Option("{\"numRecords\":10000}")
    )
    val addFile = new AddFile(addFileRow)
    val expectedString = "AddFile{" +
      "path='test/path', " +
      "partitionValues={a=1, b=2, c=3}, " +
      "size=100, " +
      "modificationTime=1234, " +
      "dataChange=false, " +
      "deletionVector=Optional.empty, " +
      "tags=Optional[{tag1=value1, tag2=value2}], " +
      "baseRowId=Optional[12345], " +
      "defaultRowCommitVersion=Optional[67890], " +
      "stats=Optional[{\"numRecords\":10000}]}"
    assert(addFile.toString == expectedString)
  }

  test("equals() compares AddFile instances correctly") {
    val addFileRow1 = generateTestAddFileRow(
      path = "test/path",
      size = 100L,
      partitionValues = Map("a" -> "1"),
      baseRowId = Option(12345L),
      stats = Option("{\"numRecords\":100}")
    )

    // Create an identical AddFile
    val addFileRow2 = generateTestAddFileRow(
      path = "test/path",
      size = 100L,
      partitionValues = Map("a" -> "1"),
      baseRowId = Option(12345L),
      stats = Option("{\"numRecords\":100}")
    )

    // Create a AddFile with different path
    val addFileRowDiffPath = generateTestAddFileRow(
      path = "different/path",
      size = 100L,
      partitionValues = Map("a" -> "1"),
      baseRowId = Option(12345L),
      stats = Option("{\"numRecords\":100}")
    )

    // Create a AddFile with different partition values, which is handled specially in equals()
    val addFileRowDiffPartition = generateTestAddFileRow(
      path = "test/path",
      size = 100L,
      partitionValues = Map("x" -> "0"),
      baseRowId = Option(12345L),
      stats = Option("{\"numRecords\":100}")
    )

    val addFile1 = new AddFile(addFileRow1)
    val addFile2 = new AddFile(addFileRow2)
    val addFileDiffPath = new AddFile(addFileRowDiffPath)
    val addFileDiffPartition = new AddFile(addFileRowDiffPartition)

    // Test equality
    assert(addFile1 === addFile2)
    assert(addFile1 != addFileDiffPath)
    assert(addFile1 != addFileDiffPartition)
    assert(addFile2 != addFileDiffPath)
    assert(addFile2 != addFileDiffPartition)
    assert(addFileDiffPath != addFileDiffPartition)

    // Test null and different type
    assert(!addFile1.equals(null))
    assert(!addFile1.equals(new DomainMetadata("domain", "config", false)))
  }

  test("hashCode is consistent with equals") {
    val addFileRow1 = generateTestAddFileRow(
      path = "test/path",
      size = 100L,
      partitionValues = Map("a" -> "1"),
      baseRowId = Option(12345L),
      stats = Option("{\"numRecords\":100}")
    )

    val addFileRow2 = generateTestAddFileRow(
      path = "test/path",
      size = 100L,
      partitionValues = Map("a" -> "1"),
      baseRowId = Option(12345L),
      stats = Option("{\"numRecords\":100}")
    )

    val addFile1 = new AddFile(addFileRow1)
    val addFile2 = new AddFile(addFileRow2)

    // Equal objects should have equal hash codes
    assert(addFile1.hashCode === addFile2.hashCode)

    // Hash code should be consistent across multiple calls
    assert(addFile1.hashCode === addFile1.hashCode)
  }
}
