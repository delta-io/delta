/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.sql.Timestamp

import org.scalatest.FunSuite

import io.delta.standalone.types.{IntegerType, StructField, StructType}

import io.delta.standalone.internal.actions._
import io.delta.standalone.internal.util.ConversionUtils.{convertAction, convertActionJ}

class ConversionUtilsSuite extends FunSuite {
  private val schema = new StructType(Array(
    new StructField("col1", new IntegerType()),
    new StructField("col2", new IntegerType())
  ))

  private val addFile = AddFile("path", Map("col1" -> "val2", "col2" -> "val2"), 123L, 456L,
    dataChange = true, "stats", Map("tagKey" -> "tagVal"))

  private val cdcFile = AddCDCFile("path", Map("col1" -> "val2", "col2" -> "val2"), 700L,
    Map("tagKey" -> "tagVal"))

  private val removeFile = addFile.removeWithTimestamp()

  private val metadata = Metadata("id", "name", "desc", Format(), schema.toJson,
    Seq("col1", "col2"), Map("configKey" -> "configVal"), Some(789L))

  private val jobInfo = JobInfo("jobId", "jobName", "runId", "jobOwnerId", "triggerType")

  private val notebookInfo = NotebookInfo("notebookId")

  private val commitInfo = CommitInfo(Some(1L), new Timestamp(1000000), Some("userId"),
    Some("userName"), "WRITE", Map("paramKey" -> "paramVal"), Some(jobInfo), Some(notebookInfo),
    Some("clusterId"), Some(9L), Some("Serializable"), Some(true),
    Some(Map("opMetricKey" -> "opMetricVal")), Some("userMetadata"), Some("engineInfo"))

  private val setTransaction = SetTransaction("appId", 1L, Some(2000L))

  private val protocol = Protocol()

  private val actions =
    Seq(addFile, cdcFile, removeFile, metadata, commitInfo, setTransaction, protocol)

  test("convert actions") {
    actions.foreach { scalaAction =>
      val javaAction = convertAction(scalaAction)
      val newScalaAction = convertActionJ(javaAction)

      assert(newScalaAction == scalaAction,
        s"""
          |New Scala action: ${newScalaAction.toString}
          |did not equal
          |Original Scala action ${scalaAction.toString}
          |""".stripMargin)
    }
  }
}
