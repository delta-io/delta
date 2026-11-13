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

package org.apache.spark.sql.delta.actions

import com.databricks.spark.util.TagDefinition
import org.apache.spark.sql.delta.{DeltaLog, DeltaUnsupportedTableFeatureException, InvalidProtocolVersionException, SnapshotDescriptor, TestUnsupportedReaderWriterFeature}
import org.apache.spark.sql.delta.metering.DeltaLoggingProvider
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}

class ActionSuite extends SparkFunSuite with SharedSparkSession {

  protected val loggingProvider: DeltaLoggingProvider = new DeltaLoggingProvider {
    override def getCommonTags: Map[TagDefinition, String] = Map.empty
  }

  protected def readSnapshot(tableMetadata: Metadata): SnapshotDescriptor = new SnapshotDescriptor {
    override def deltaLog: DeltaLog =
      throw new UnsupportedOperationException("Must not access DeltaLog")
    override val version: Long = 1L
    override val metadata: Metadata = tableMetadata
    override val protocol: Protocol = Protocol(1, 2)
    override val dataPath: Path = new Path("/table")
    override val logPath: Path = new Path(dataPath, "_delta_log")
    override protected[delta] def numOfFilesIfKnown: Option[Long] = None
    override protected[delta] def sizeInBytesIfKnown: Option[Long] = None
    override def getCommonTags: Map[TagDefinition, String] = Map.empty
  }

  test("Protocol.protocolCheck accepts supported protocols and ignores writer-only features") {
    Seq(
      Protocol(1, 2),
      Protocol(3, 7).withReaderFeatures(Set("deletionVectors"))
        .withWriterFeatures(Set("deletionVectors")),
      Protocol(1, 7).withWriterFeatures(Set("unknownWriterFeature"))).foreach { protocol =>
      Protocol.protocolCheck(spark, protocol, "read", "/table", loggingProvider)
    }
  }

  test("Protocol validation rejects unsupported versions and preserves the table path") {
    val error = intercept[InvalidProtocolVersionException] {
      Protocol.protocolCheck(spark, Protocol(99, 7), "read", "/table", loggingProvider)
    }
    assert(error.getMessage.contains("99"))
    assert(error.getMessage.contains("/table"))
    Protocol.protocolCheck(spark, Protocol(1, 99), "read", "/table", loggingProvider)
    intercept[InvalidProtocolVersionException] {
      Protocol.protocolCheck(spark, Protocol(1, 99), "write", "/table", loggingProvider)
    }
  }

  test("Protocol.protocolCheck rejects unsupported reader features") {
    val protocol = Protocol(3, 7).withReaderFeatures(Set("unknownReaderFeature"))
      .withWriterFeatures(Set("unknownReaderFeature"))
    val error = intercept[DeltaUnsupportedTableFeatureException] {
      Protocol.protocolCheck(spark, protocol, "read", "/table", loggingProvider)
    }
    assert(error.getErrorClass === "DELTA_UNSUPPORTED_FEATURES_FOR_READ")
    assert(error.unsupported.toSet === Set("unknownReaderFeature"))
  }

  test("Protocol.protocolCheck honors the session's unsupported testing features setting") {
    withSQLConf(DeltaSQLConf.TABLE_FEATURES_TEST_FEATURES_ENABLED.key -> "true") {
      val protocol = Protocol(3, 7).withFeature(TestUnsupportedReaderWriterFeature)
      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> "false") {
        Protocol.protocolCheck(spark, protocol, "read", "/table", loggingProvider)
      }
      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> "true") {
        val error = intercept[DeltaUnsupportedTableFeatureException] {
          Protocol.protocolCheck(spark, protocol, "read", "/table", loggingProvider)
        }
        assert(error.unsupported.toSet === Set(TestUnsupportedReaderWriterFeature.name))
      }
    }
  }

  test("Protocol.protocolRead validates commit protocols without accessing DeltaLog") {
    val snapshot = readSnapshot(Metadata())
    Protocol.protocolRead(spark, Protocol(1, 2), snapshot.dataPath.toString, snapshot)
    intercept[InvalidProtocolVersionException] {
      Protocol.protocolRead(spark, Protocol(99, 7), snapshot.dataPath.toString, snapshot)
    }
    val unsupported = Protocol(3, 7).withReaderFeatures(Set("unknownReaderFeature"))
      .withWriterFeatures(Set("unknownReaderFeature"))
    intercept[DeltaUnsupportedTableFeatureException] {
      Protocol.protocolRead(spark, unsupported, snapshot.dataPath.toString, snapshot)
    }
  }

  test("CheckpointMetadata - extract from tags") {
    val metadata1 = CheckpointMetadata(version = 1, tags = null)
    assert(metadata1.numOfAddFiles.isEmpty)
    assert(metadata1.sidecarSizeInBytes.isEmpty)
    assert(metadata1.sidecarNumActions.isEmpty)
    assert(metadata1.sidecarFileSchema.isEmpty)

    import CheckpointMetadata.Tags
    val metadata2 = CheckpointMetadata(
      version = 1,
      tags = Map(Tags.NUM_OF_ADD_FILES.name -> "2", Tags.SIDECAR_SIZE_IN_BYTES.name -> "3"))
    assert(metadata2.numOfAddFiles.contains(2))
    assert(metadata2.sidecarSizeInBytes.contains(3))
    assert(metadata2.sidecarNumActions.isEmpty)
    assert(metadata2.sidecarFileSchema.isEmpty)

    val schemaForMetadata = StructType(Seq(StructField("a", dataType = BooleanType)))
    val metadata3 = CheckpointMetadata(
      version = 1,
      tags = Map(
        Tags.SIDECAR_NUM_ACTIONS.name -> "2",
        Tags.SIDECAR_FILE_SCHEMA.name -> schemaForMetadata.json)
    )
    assert(metadata3.numOfAddFiles.isEmpty)
    assert(metadata3.sidecarSizeInBytes.isEmpty)
    assert(metadata3.sidecarNumActions.contains(2))
    assert(metadata3.sidecarFileSchema.contains(schemaForMetadata))
  }
}
