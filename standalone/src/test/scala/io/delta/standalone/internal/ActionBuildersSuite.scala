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
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.standalone.actions.{AddFile => AddFileJ, CommitInfo => CommitInfoJ, Format => FormatJ, JobInfo => JobInfoJ, Metadata => MetadataJ, NotebookInfo => NotebookInfoJ, RemoveFile => RemoveFileJ}
import io.delta.standalone.types.{IntegerType, StructField => StructFieldJ, StructType => StructTypeJ}

import org.scalatest.FunSuite

class ActionBuildersSuite extends FunSuite {
  test("builder action class constructor for Metadata") {
    val metadataFromBuilderDefaults = MetadataJ.builder().build()
    val metadataFromConstructorDefaults = new MetadataJ(
      metadataFromBuilderDefaults.getId(),
      null,
      null,
      new FormatJ("parquet", Collections.emptyMap()),
      Collections.emptyList(),
      Collections.emptyMap(),
      metadataFromBuilderDefaults.getCreatedTime(),
      null);
    assert(metadataFromBuilderDefaults == metadataFromConstructorDefaults)

    val metadataFromBuilder = MetadataJ.builder()
      .id("test_id")
      .name("test_name")
      .description("test_description")
      .format(new FormatJ("csv", Collections.emptyMap()))
      .partitionColumns(List("id", "name").asJava)
      .configuration(Map("test"->"foo").asJava)
      .createdTime(0L)
      .schema(new StructTypeJ(Array(new StructFieldJ("test_field", new IntegerType()))))
      .build()
    val metadataFromConstructor = new MetadataJ(
      "test_id",
      "test_name",
      "test_description",
      new FormatJ("csv", Collections.emptyMap()),
      List("id", "name").asJava,
      Map("test"->"foo").asJava,
      Optional.of(0L),
      new StructTypeJ(Array(new StructFieldJ("test_field", new IntegerType()))))
    assert(metadataFromBuilder == metadataFromConstructor)
  }

  test("builder action class constructor for AddFile") {
    val addFileFromBuilderDefaults = AddFileJ.builder(
      "/test",
      Collections.emptyMap(),
      0L,
      0L,
      true).build()
    val addFileFromConstructorDefaults = new AddFileJ(
      "/test",
      Collections.emptyMap(),
      0L,
      0L,
      true,
      null,
      null)
    assert(addFileFromBuilderDefaults == addFileFromConstructorDefaults)

    val addFileFromBuilder = AddFileJ.builder(
      "/test",
      Collections.emptyMap(),
      0L,
      0L,
      true)
      .stats("test_stats")
      .tags(Map("test"->"foo").asJava)
      .build()
    val addFileFromConstructor = new AddFileJ(
      "/test",
      Collections.emptyMap(),
      0L,
      0L,
      true,
      "test_stats",
      Map("test"->"foo").asJava)
    assert(addFileFromBuilder == addFileFromConstructor)
  }

  test("builder action class constructor for JobInfo") {
    val jobInfoFromBuilderDefaults = JobInfoJ.builder("test").build()
    val jobInfoFromConstructorDefaults = new JobInfoJ(
      "test",
      null,
      null,
      null,
      null)
    assert(jobInfoFromBuilderDefaults == jobInfoFromConstructorDefaults)

    val jobInfoFromBuilder = JobInfoJ.builder("test")
      .jobName("test_name")
      .runId("test_id")
      .jobOwnerId("test_job_id")
      .triggerType("test_trigger_type")
      .build()
    val jobInfoFromConstructor = new JobInfoJ(
      "test",
      "test_name",
      "test_id",
      "test_job_id",
      "test_trigger_type")
    assert(jobInfoFromBuilder == jobInfoFromConstructor)
  }

  test("builder action class constructor for RemoveFile") {
    val removeFileJFromBuilderDefaults = RemoveFileJ.builder("/test").build()
    val removeFileJFromConstructorDefaults = new RemoveFileJ(
      "/test",
      Optional.empty(),
      true,
      false,
      null,
      0L,
      null)
    assert(removeFileJFromBuilderDefaults == removeFileJFromConstructorDefaults)

    val removeFileJFromBuilder = RemoveFileJ.builder("/test")
      .deletionTimestamp(0L)
      .dataChange(false)
      .extendedFileMetadata(true)
      .partitionValues(Map("test"->"foo").asJava)
      .size(1L)
      .tags(Map("tag"->"foo").asJava)
      .build()
    val removeFileJFromConstructor = new RemoveFileJ(
      "/test",
      Optional.of(0L),
      false,
      true,
      Map("test"->"foo").asJava,
      1L,
      Map("tag"->"foo").asJava)
    assert(removeFileJFromBuilder == removeFileJFromConstructor)
  }

  test("builder action class constructor for CommitInfo") {
    val commitInfoFromBuilderDefaults = CommitInfoJ.builder().build()
    val commitInfoFromConstructorDefaults = new CommitInfoJ(
      Optional.empty(),
      null,
      Optional.empty(),
      Optional.empty(),
      null,
      null,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty())
    assert(commitInfoFromBuilderDefaults == commitInfoFromConstructorDefaults)

    val commitInfoFromBuilder = CommitInfoJ.builder()
      .version(0L)
      .timestamp(new Timestamp(1540415658000L))
      .userId("test_id")
      .userName("test_name")
      .operation("test_op")
      .operationParameters(Map("test"->"op").asJava)
      .jobInfo(JobInfoJ.builder("test").build())
      .notebookInfo(new NotebookInfoJ("test"))
      .clusterId("test_clusterId")
      .readVersion(0L)
      .isolationLevel("test_level")
      .isBlindAppend(true)
      .operationMetrics(Map("test"->"metric").asJava)
      .userMetadata("user_metadata")
      .engineInfo("engine_info")
      .build()
    val commitInfoFromConstructor = new CommitInfoJ(
      Optional.of(0L),
      new Timestamp(1540415658000L),
      Optional.of("test_id"),
      Optional.of("test_name"),
      "test_op",
      Map("test"->"op").asJava,
      Optional.of(JobInfoJ.builder("test").build()),
      Optional.of(new NotebookInfoJ("test")),
      Optional.of("test_clusterId"),
      Optional.of(0L),
      Optional.of("test_level"),
      Optional.of(true),
      Optional.of(Map("test"->"metric").asJava),
      Optional.of("user_metadata"),
      Optional.of("engine_info"))
    assert(commitInfoFromBuilder == commitInfoFromConstructor)
  }
}
