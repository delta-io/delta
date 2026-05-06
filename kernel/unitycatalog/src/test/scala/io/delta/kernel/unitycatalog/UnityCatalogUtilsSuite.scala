/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.unitycatalog

import java.io.{File, PrintWriter}

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.DomainMetadata
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain
import io.delta.kernel.test.MockSnapshotUtils
import io.delta.kernel.transaction.DataLayoutSpec
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.{CloseableIterable, FileStatus}

import org.scalatest.funsuite.AnyFunSuite

class UnityCatalogUtilsSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with MockSnapshotUtils {

  private val testUcTableId = "testUcTableId"

  test("getPropertiesForCreate: throws when snapshot is not version 0") {
    val mockSnapshotV1 = getMockSnapshot(new Path("/fake/table/path"), latestVersion = 1)

    val exMsg = intercept[IllegalArgumentException] {
      UnityCatalogUtils.getPropertiesForCreate(defaultEngine, mockSnapshotV1)
    }.getMessage

    assert(exMsg.contains("Expected a snapshot at version 0, but got a snapshot at version 1"))
  }

  test("getPropertiesForCreate: handles all cases together") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)

      val testSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add(
          "address",
          new StructType()
            .add("city", StringType.STRING)
            .add("state", StringType.STRING))
        .add("data", StringType.STRING)

      val clusteringColumns = List(new Column("id"), new Column(Array("address", "city")))

      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val snapshot = ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          ucTableIdentifier)
        .withTableProperties(
          Map(
            "foo" -> "bar",
            "delta.enableRowTracking" -> "true",
            "delta.columnMapping.mode" -> "name").asJava)
        .withDataLayoutSpec(DataLayoutSpec.clustered(clusteringColumns.asJava))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
        .getPostCommitSnapshot
        .get()
        .asInstanceOf[SnapshotImpl]

      val snapshotTimestamp = snapshot.getTimestamp(engine)

      val actualProps = UnityCatalogUtils.getPropertiesForCreate(engine, snapshot).asScala

      val expectedProps = Map(
        // Case 0: Properties we expect to be injected by the UC-CatalogManaged-Client (and are
        //         stored in the metadata.configuration)
        "io.unitycatalog.tableId" -> testUcTableId,

        // Case 1: Table properties from metadata.configuration
        "foo" -> "bar",
        "delta.enableRowTracking" -> "true",
        "delta.columnMapping.mode" -> "name",

        // Case 2: Protocol-derived properties
        "delta.minReaderVersion" -> "3",
        "delta.minWriterVersion" -> "7",
        "delta.feature.catalogManaged" -> "supported",
        "delta.feature.rowTracking" -> "supported",
        "delta.feature.columnMapping" -> "supported",
        "delta.feature.inCommitTimestamp" -> "supported",

        // Case 3: UC metastore properties
        "delta.lastUpdateVersion" -> "0",
        "delta.lastCommitTimestamp" -> s"$snapshotTimestamp",

        // Case 4: Clustering properties - these should be the LOGICAL names not the PHYSICAL names
        "clusteringColumns" -> """[["id"],["address","city"]]""")

      val failures = expectedProps.collect {
        case (k, v) if !actualProps.contains(k) => s"$k: MISSING (expected: $v)"
        case (k, v) if actualProps(k) != v => s"$k: expected '$v', got '${actualProps(k)}'"
      }

      assert(failures.isEmpty, failures.mkString("Property mismatches:\n", "\n", ""))
    }
  }

  test("getPropertiesForCreate(CommitMetadata): properties match snapshot-based overload") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)

      val testSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add(
          "address",
          new StructType()
            .add("city", StringType.STRING)
            .add("state", StringType.STRING))
        .add("data", StringType.STRING)

      val clusteringColumns = List(new Column("id"), new Column(Array("address", "city")))

      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val snapshot = ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          ucTableIdentifier)
        .withTableProperties(
          Map(
            "foo" -> "bar",
            "delta.enableRowTracking" -> "true",
            "delta.columnMapping.mode" -> "name").asJava)
        .withDataLayoutSpec(DataLayoutSpec.clustered(clusteringColumns.asJava))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
        .getPostCommitSnapshot
        .get()
        .asInstanceOf[SnapshotImpl]

      // The CommitMetadata-based overload was called by finalizeTableInCatalog during commit.
      // Verify its output (captured in FinalizeCreateRecord) matches the snapshot-based overload.
      val snapshotProps = UnityCatalogUtils.getPropertiesForCreate(engine, snapshot).asScala
      val commitMetadataProps = ucClient.getLastFinalizeCreateRecord.get.properties.asScala

      // All properties from the snapshot-based overload should also be in the CommitMetadata-based
      val failures = snapshotProps.collect {
        case (k, v) if !commitMetadataProps.contains(k) =>
          s"$k: MISSING from CommitMetadata overload (expected: $v)"
        case (k, v) if commitMetadataProps(k) != v =>
          s"$k: snapshot='$v', commitMetadata='${commitMetadataProps(k)}'"
      }

      assert(failures.isEmpty, failures.mkString("Property mismatches:\n", "\n", ""))

      // Also verify clustering columns are present
      assert(commitMetadataProps("clusteringColumns") == """[["id"],["address","city"]]""")
    }
  }

  test("getPropertiesForCreate: clustered table with empty clustering columns") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)

      val ucTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl")
      val snapshot = ucCatalogManagedClient
        .buildCreateTableTransaction(
          testUcTableId,
          tablePath,
          testSchema,
          "test-engine",
          ucTableIdentifier)
        .withDataLayoutSpec(DataLayoutSpec.clustered(List.empty.asJava))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
        .getPostCommitSnapshot
        .get()
        .asInstanceOf[SnapshotImpl]

      val props = UnityCatalogUtils.getPropertiesForCreate(engine, snapshot).asScala
      assert(props("clusteringColumns") == "[]")
    }
  }

  //////////////////////////////////
  // parseDeltaFileContents tests
  //////////////////////////////////

  /** Helper to write JSON action lines to a temp file and return a FileStatus for it. */
  private def writeDeltaFile(dir: String, lines: Seq[String]): FileStatus = {
    val file = new File(dir, "test-delta-file.json")
    val writer = new PrintWriter(file)
    try {
      lines.foreach(line => writer.write(line + "\n"))
    } finally {
      writer.close()
    }
    FileStatus.of(file.getAbsolutePath, file.length(), file.lastModified())
  }

  test("parseDeltaFileContents: extracts protocol, metadata, commitInfo, and domainMetadata") {
    withTempDirAndEngine { case (tmpDir, engine) =>
      val lines = Seq(
        """{"commitInfo":{"inCommitTimestamp":1749830855993,"timestamp":1749830855992,""" +
          """"engineInfo":"test-engine","operation":"CREATE TABLE",""" +
          """"operationParameters":{},"isBlindAppend":true,"txnId":"txn-123",""" +
          """"operationMetrics":{}}}""",
        // scalastyle:off line.size.limit
        """{"metaData":{"id":"test-table-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"col1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true"},"createdTime":1749830855646}}""",
        // scalastyle:on line.size.limit
        """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,""" +
          """"readerFeatures":["catalogManaged"],""" +
          """"writerFeatures":["catalogManaged","inCommitTimestamp"]}}""",
        """{"domainMetadata":{"domain":"delta.clustering","configuration":"{}","removed":false}}""")

      val deltaFile = writeDeltaFile(tmpDir, lines)
      val result = UnityCatalogUtils.parseDeltaFileContents(engine, deltaFile)

      // Verify Protocol.
      assert(result.getProtocol.isPresent)
      val protocol = result.getProtocol.get()
      assert(protocol.getMinReaderVersion == 3)
      assert(protocol.getMinWriterVersion == 7)
      assert(protocol.getReaderFeatures.asScala == Set("catalogManaged"))
      assert(protocol.getWriterFeatures.asScala == Set("catalogManaged", "inCommitTimestamp"))

      // Verify Metadata.
      assert(result.getMetadata.isPresent)
      val metadata = result.getMetadata.get()
      assert(metadata.getId == "test-table-id")
      assert(metadata.getConfiguration.get("delta.enableInCommitTimestamps") == "true")
      assert(metadata.getSchema.fields().size() == 1)
      assert(metadata.getSchema.get("col1").getDataType == IntegerType.INTEGER)

      // Verify CommitInfo.
      assert(result.getCommitInfo.isPresent)
      val commitInfo = result.getCommitInfo.get()
      assert(commitInfo.getInCommitTimestamp.isPresent)
      assert(commitInfo.getInCommitTimestamp.get() == 1749830855993L)
      assert(commitInfo.getOperation.get() == "CREATE TABLE")
      assert(commitInfo.getTxnId.get() == "txn-123")

      // Verify DomainMetadata.
      assert(result.getDomainMetadatas.size() == 1)
      val dm = result.getDomainMetadatas.get(0)
      assert(dm.getDomain == "delta.clustering")
      assert(dm.getConfiguration == "{}")
      assert(!dm.isRemoved)
    }
  }

  test("parseDeltaFileContents: data-only file with commitInfo, no protocol or metadata") {
    withTempDirAndEngine { case (tmpDir, engine) =>
      val lines = Seq(
        """{"commitInfo":{"inCommitTimestamp":1749830871085,"timestamp":1749830871084,""" +
          """"engineInfo":"test-engine","operation":"WRITE",""" +
          """"operationParameters":{"mode":"Append"},"isBlindAppend":true,""" +
          """"txnId":"txn-456","operationMetrics":{}}}""",
        """{"add":{"path":"part-00000.parquet","partitionValues":{},"size":889,""" +
          """"modificationTime":1749830870833,"dataChange":true}}""")

      val deltaFile = writeDeltaFile(tmpDir, lines)
      val result = UnityCatalogUtils.parseDeltaFileContents(engine, deltaFile)

      assert(!result.getProtocol.isPresent)
      assert(!result.getMetadata.isPresent)
      assert(result.getCommitInfo.isPresent)
      val commitInfo = result.getCommitInfo.get()
      assert(commitInfo.getInCommitTimestamp.get() == 1749830871085L)
      assert(commitInfo.getOperation.get() == "WRITE")
      assert(result.getDomainMetadatas.isEmpty)
    }
  }

  test("parseDeltaFileContents: delta file with multiple domain metadata actions") {
    withTempDirAndEngine { case (tmpDir, engine) =>
      val lines = Seq(
        """{"commitInfo":{"timestamp":100,"engineInfo":"e","operation":"op",""" +
          """"operationParameters":{},"isBlindAppend":true,"txnId":"t",""" +
          """"operationMetrics":{}}}""",
        s"""{"domainMetadata":{"domain":"${ClusteringMetadataDomain.DOMAIN_NAME}",""" +
          """"configuration":"{\"columns\":[\"a\"]}","removed":false}}""",
        s"""{"domainMetadata":{"domain":"${RowTrackingMetadataDomain.DOMAIN_NAME}",""" +
          """"configuration":"{\"highWaterMark\":100}","removed":false}}""",
        """{"domainMetadata":{"domain":"myapp.customDomain","configuration":"custom",""" +
          """"removed":true}}""")

      val deltaFile = writeDeltaFile(tmpDir, lines)
      val result = UnityCatalogUtils.parseDeltaFileContents(engine, deltaFile)

      assert(!result.getProtocol.isPresent)
      assert(!result.getMetadata.isPresent)
      assert(result.getCommitInfo.isPresent)

      val domainMetadatas = result.getDomainMetadatas.asScala
      assert(domainMetadatas.size == 3)

      assert(domainMetadatas(0).getDomain == ClusteringMetadataDomain.DOMAIN_NAME)
      assert(domainMetadatas(0).getConfiguration == "{\"columns\":[\"a\"]}")
      assert(!domainMetadatas(0).isRemoved)

      assert(domainMetadatas(1).getDomain == RowTrackingMetadataDomain.DOMAIN_NAME)
      assert(domainMetadatas(1).getConfiguration == "{\"highWaterMark\":100}")
      assert(!domainMetadatas(1).isRemoved)

      assert(domainMetadatas(2).getDomain == "myapp.customDomain")
      assert(domainMetadatas(2).getConfiguration == "custom")
      assert(domainMetadatas(2).isRemoved)
    }
  }

  test("parseDeltaFileContents: file with only metadata, no protocol or commitInfo") {
    withTempDirAndEngine { case (tmpDir, engine) =>
      // scalastyle:off line.size.limit
      val lines = Seq(
        """{"metaData":{"id":"meta-only-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{}}}""")
      // scalastyle:on line.size.limit

      val deltaFile = writeDeltaFile(tmpDir, lines)
      val result = UnityCatalogUtils.parseDeltaFileContents(engine, deltaFile)

      assert(!result.getProtocol.isPresent)
      assert(result.getMetadata.isPresent)
      assert(result.getMetadata.get().getId == "meta-only-id")
      assert(!result.getCommitInfo.isPresent)
      assert(result.getDomainMetadatas.isEmpty)
    }
  }

  test("parseDeltaFileContents: file with only protocol, no metadata or commitInfo") {
    withTempDirAndEngine { case (tmpDir, engine) =>
      val lines = Seq(
        """{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}""")

      val deltaFile = writeDeltaFile(tmpDir, lines)
      val result = UnityCatalogUtils.parseDeltaFileContents(engine, deltaFile)

      assert(result.getProtocol.isPresent)
      assert(result.getProtocol.get().getMinReaderVersion == 1)
      assert(result.getProtocol.get().getMinWriterVersion == 2)
      assert(!result.getMetadata.isPresent)
      assert(!result.getCommitInfo.isPresent)
      assert(result.getDomainMetadatas.isEmpty)
    }
  }

  test("parseDeltaFileContents: empty delta file returns all fields absent") {
    withTempDirAndEngine { case (tmpDir, engine) =>
      val deltaFile = writeDeltaFile(tmpDir, Seq.empty)
      val result = UnityCatalogUtils.parseDeltaFileContents(engine, deltaFile)

      assert(!result.getProtocol.isPresent)
      assert(!result.getMetadata.isPresent)
      assert(!result.getCommitInfo.isPresent)
      assert(result.getDomainMetadatas.isEmpty)
    }
  }
}
