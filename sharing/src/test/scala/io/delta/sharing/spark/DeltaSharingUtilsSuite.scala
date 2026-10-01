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

package io.delta.sharing.spark

import scala.reflect.ClassTag

import org.apache.spark.sql.delta.{
  AppendOnlyTableFeature,
  DeletionVectorsTableFeature,
  GeoSpatialTableFeature
}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.util.FileNames
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{DeltaTableFiles, DeltaTableMetadata, Table, TemporaryCredentials}
import io.delta.sharing.spark.DeltaSharingUtils._
import io.delta.sharing.spark.model.{DeltaSharingMetadata, DeltaSharingProtocol}
import org.apache.hadoop.fs.Path

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.delta.sharing.TableRefreshResult
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.BlockId

class DeltaSharingUtilsSuite extends SparkFunSuite with SharedSparkContext {

  type RefresherFunction = Option[String] => TableRefreshResult
  class SimpleTestDeltaSharingClient extends DeltaSharingClient {
    def getStatsStr(): String = {
      """{
        |  "numRecords": 20,
        |  "minValues": { "col-a": 0 },
        |  "maxValues": { "col-a": 19 },
        |  "nullCount": { "col-a": 0 }
        |}""".stripMargin
        .replace("\n", "")
        .replace(" ", "")
        .replace("\"", "\\\"")
    }

    def getAddFileStr(): String = {
      val stats = getStatsStr()
      s"""{
         |  "file": {
         |    "id": "add_file_id1",
         |    "expirationTimestamp": 1721350999999,
         |    "deltaSingleAction": {
         |      "add": {
         |        "path": "c000.snappy.parquet",
         |        "partitionValues": {
         |          "col-partition": "3"
         |        },
         |        "size": 1213,
         |        "modificationTime": 1721350059000,
         |        "dataChange": true,
         |        "stats": "$stats",
         |        "tags": {
         |          "INSERTION_TIME": "1721350059000000"
         |        }
         |      }
         |    }
         |  }
         |}""".stripMargin
    }

    def getDeletionVectorStr(): String = {
      val stats = getStatsStr()
      s"""{
         |  "file": {
         |    "id": "add_file_id2",
         |    "expirationTimestamp": 1721350999999,
         |    "deletionVectorFileId": "dv_file_id",
         |    "deltaSingleAction": {
         |      "add": {
         |        "path": "c001.snappy.parquet",
         |        "partitionValues": {
         |          "col-partition": "3"
         |        },
         |        "size": 1213,
         |        "modificationTime": 1721350059000,
         |        "dataChange": true,
         |        "stats": "$stats",
         |        "tags": {
         |          "INSERTION_TIME": "1721350059000000"
         |        },
         |        "deletionVector": {
         |          "storageType": "p",
         |          "pathOrInlineDv": "fakeurl",
         |          "offset": 1,
         |          "sizeInBytes": 34,
         |          "cardinality": 1
         |        }
         |      }
         |    }
         |  }
         |}""".stripMargin
    }

    def getCdcStr(): String = {
      s"""{"file":{
         |  "id":"cdc_file_id",
         |  "expirationTimestamp":1721350999999,
         |  "deltaSingleAction":{
         |    "cdc":{
         |      "path":"_change_data/cdc.c000.snappy.parquet",
         |      "partitionValues":{},
         |      "size":1213,
         |      "modificationTime":1721350059000,
         |      "dataChange":false
         |    }
         |  }
         |}}""".stripMargin
    }
    override def listAllTables(): Seq[Table] = Seq.empty

    override def getTableVersion(table: Table, startingTimestamp: Option[String] = None): Long = 0

    override def getMetadata(
      table: Table,
      versionAsOf: Option[Long] = None,
      timestampAsOf: Option[String] = None
    ): DeltaTableMetadata =
      throw new UnsupportedOperationException

    override def getFiles(
      table: Table,
      predicates: Seq[String],
      limit: Option[Long],
      versionAsOf: Option[Long],
      timestampAsOf: Option[String],
      jsonPredicateHints: Option[String],
      refreshToken: Option[String],
      fileIdHash: Option[String]
    ): DeltaTableFiles = {
      val file = getAddFileStr()
      val dv = getDeletionVectorStr()
      DeltaTableFiles(
        version = 0L,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
        lines = Seq(file, dv)
      )
    }

    override def getFiles(
      table: Table,
      startingVersion: Long,
      endingVersion: Option[Long],
      fileIdHash: Option[String],
      includeHistoricalProtocol: Boolean = false
    ): DeltaTableFiles = {
      val file = getAddFileStr()
      val dv = getDeletionVectorStr()
      DeltaTableFiles(
        version = 0L,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
        lines = Seq(file, dv)
      )
    }

    override def getCDFFiles(
      table: Table,
      cdfOptions: Map[String, String],
      includeHistoricalMetadata: Boolean,
      fileIdHash: Option[String],
      includeHistoricalProtocol: Boolean = false): DeltaTableFiles = {
      val file = getAddFileStr()
      val dv = getDeletionVectorStr()
      val cdc = getCdcStr()
      DeltaTableFiles(
        version = 0L,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
        lines = Seq(file, dv, cdc)
      )
    }

    override def generateTemporaryTableCredential(
        table: Table,
        location: Option[String]): TemporaryCredentials = {
      throw new UnsupportedOperationException("generateTemporaryTableCredential is not implemented")
    }
  }


  test("override single block in blockmanager works") {
    val blockId = BlockId(s"${DeltaSharingUtils.DELTA_SHARING_BLOCK_ID_PREFIX}_1")
    overrideSingleBlock[Int](blockId, 1)
    assert(SparkEnv.get.blockManager.getSingle[Int](blockId).get == 1)
    SparkEnv.get.blockManager.releaseLock(blockId)
    overrideSingleBlock[String](blockId, "2")
    assert(SparkEnv.get.blockManager.getSingle[String](blockId).get == "2")
    SparkEnv.get.blockManager.releaseLock(blockId)
  }

  def getSeqFromBlockManager[T: ClassTag](blockId: BlockId): Seq[T] = {
    val iterator = SparkEnv.get.blockManager
      .get[T](blockId)
      .map(
        _.data.asInstanceOf[Iterator[T]]
      )
      .get
    val seqBuilder = Seq.newBuilder[T]
    while (iterator.hasNext) {
      seqBuilder += iterator.next()
    }
    seqBuilder.result()
  }

  test("override iterator block in blockmanager works") {
    val blockId = BlockId(s"${DeltaSharingUtils.DELTA_SHARING_BLOCK_ID_PREFIX}_1")
    overrideIteratorBlock[Int](blockId, values = Seq(1, 2).toIterator)
    assert(getSeqFromBlockManager[Int](blockId) == Seq(1, 2))
    overrideIteratorBlock[String](blockId, values = Seq("3", "4").toIterator)
    assert(getSeqFromBlockManager[String](blockId) == Seq("3", "4"))
  }

  test("getRefresherForGetFiles with deletion vector") {
    val client = new SimpleTestDeltaSharingClient()
    val table = Table(name = "table", schema = "schema", share = "share")
    val func: RefresherFunction = getRefresherForGetFiles(
      client,
      table,
      Seq.empty,
      None,
      None,
      None,
      None,
      useRefreshToken = true
    )
    val idToUrls = func(None).idToUrl
    assert(idToUrls.size == 3)
    assert(idToUrls.contains("add_file_id1"))
    assert(idToUrls.get("add_file_id1") == Some("c000.snappy.parquet"))
    assert(idToUrls.contains("add_file_id2"))
    assert(idToUrls.get("add_file_id2") == Some("c001.snappy.parquet"))
    assert(idToUrls.contains("dv_file_id"))
    assert(idToUrls.get("dv_file_id") == Some("fakeurl"))
  }

  test("getRefresherForGetFilesWithStartingVersion with deletion vector") {
    val client = new SimpleTestDeltaSharingClient()
    val table = Table(name = "table", schema = "schema", share = "share")
    val func: RefresherFunction = getRefresherForGetFilesWithStartingVersion(
      client,
      table,
      0L,
      None
    )
    val idToUrls = func(None).idToUrl
    assert(idToUrls.size == 3)
    assert(idToUrls.contains("add_file_id1"))
    assert(idToUrls.get("add_file_id1") == Some("c000.snappy.parquet"))
    assert(idToUrls.contains("add_file_id2"))
    assert(idToUrls.get("add_file_id2") == Some("c001.snappy.parquet"))
    assert(idToUrls.contains("dv_file_id"))
    assert(idToUrls.get("dv_file_id") == Some("fakeurl"))
  }

  test("getRefresherForGetCDFFiles with deletion vector") {
    val client = new SimpleTestDeltaSharingClient()
    val table = Table(name = "table", schema = "schema", share = "share")
    val func: RefresherFunction = getRefresherForGetCDFFiles(
      client,
      table,
      Map[String, String]("startingVersion" -> "0")
    )
    val idToUrls = func(None).idToUrl
    assert(idToUrls.size == 4)
    assert(idToUrls.contains("add_file_id1"))
    assert(idToUrls.get("add_file_id1") == Some("c000.snappy.parquet"))
    assert(idToUrls.contains("add_file_id2"))
    assert(idToUrls.get("add_file_id2") == Some("c001.snappy.parquet"))
    assert(idToUrls.contains("dv_file_id"))
    assert(idToUrls.get("dv_file_id") == Some("fakeurl"))
    assert(idToUrls.contains("cdc_file_id"))
    assert(idToUrls.get("cdc_file_id") == Some("_change_data/cdc.c000.snappy.parquet"))
  }

  test("GeoSpatial stable feature is advertised in both reader-features lists") {
    assert(STREAMING_SUPPORTED_READER_FEATURES.contains(GeoSpatialTableFeature.name))
    assert(SUPPORTED_READER_FEATURES.contains(GeoSpatialTableFeature.name))
    assert(GeoSpatialTableFeature.name == "geospatial")
  }

  test("readerFeatures header string contains the geospatial feature name") {
    val streamingHeader = STREAMING_SUPPORTED_READER_FEATURES.mkString(",")
    val batchHeader = SUPPORTED_READER_FEATURES.mkString(",")
    assert(streamingHeader.split(",").contains("geospatial"),
      s"streaming readerFeatures header missing 'geospatial': $streamingHeader")
    assert(batchHeader.split(",").contains("geospatial"),
      s"batch readerFeatures header missing 'geospatial': $batchHeader")
  }

  test("getRefresherForGetFiles respects useRefreshToken parameter") {
    // Test client that tracks the refresh token parameter
    class RefreshTokenTrackingClient extends SimpleTestDeltaSharingClient {
      var lastRefreshToken: Option[String] = null

      override def getFiles(
        table: Table,
        predicates: Seq[String],
        limit: Option[Long],
        versionAsOf: Option[Long],
        timestampAsOf: Option[String],
        jsonPredicateHints: Option[String],
        refreshToken: Option[String],
        fileIdHash: Option[String]
      ): DeltaTableFiles = {
        lastRefreshToken = refreshToken
        super.getFiles(table, predicates, limit, versionAsOf, timestampAsOf,
          jsonPredicateHints, refreshToken, fileIdHash)
      }
    }

    val client = new RefreshTokenTrackingClient()
    val table = Table(name = "table", schema = "schema", share = "share")
    val testRefreshToken = Some("test-refresh-token")

    // Test with useRefreshToken = true - should use the provided refresh token
    val funcWithRefreshToken: RefresherFunction = getRefresherForGetFiles(
      client,
      table,
      Seq.empty,
      None,
      Some(0L),
      None,
      None,
      useRefreshToken = true
    )
    funcWithRefreshToken(testRefreshToken)
    assert(client.lastRefreshToken == testRefreshToken,
      "When useRefreshToken=true, the refresh token should be passed through")

    // Test with useRefreshToken = false - should ignore the provided refresh token
    val funcWithoutRefreshToken: RefresherFunction = getRefresherForGetFiles(
      client,
      table,
      Seq.empty,
      None,
      Some(0L),
      None,
      None,
      useRefreshToken = false
    )
    funcWithoutRefreshToken(testRefreshToken)
    assert(client.lastRefreshToken == None,
      "When useRefreshToken=false, the refresh token should be ignored and None should be used")
  }

  // Reads back the json lines of a locally-constructed delta log file (as stored in the block
  // manager by DeltaSharingLogFileSystem) for the given version.
  private def readLocalDeltaLogVersion(customTablePath: String, version: Long): Seq[String] = {
    val deltaLogPath = s"${DeltaSharingLogFileSystem.encode(customTablePath).toString}/_delta_log"
    val jsonFilePath = FileNames.unsafeDeltaFile(new Path(deltaLogPath), version).toString
    getSeqFromBlockManager[String](
      DeltaSharingLogFileSystem.getDeltaSharingLogBlockId(jsonFilePath)
    ).flatMap(_.split("\n")).filter(_.nonEmpty)
  }

  test("constructLocalDeltaLogAcrossVersions writes historical protocols to their own versions") {
    val customTablePath = "constructHistoricalProtocol/table"
    // A version range [1, 3] where the protocol is upgraded at v2 by enabling a reader/writer table
    // feature (deletionVectors): the head protocol is stamped with the starting version (1), and
    // the upgrade at v2 is streamed as its own versioned protocol, mirroring how historical
    // metadata is streamed. Both protocols carry reader/writer feature lists (table-features
    // protocol, minReaderVersion 3 / minWriterVersion 7) so the test exercises a realistic
    // feature-list change, not just a version-number bump.
    val headProtocol = Protocol(minReaderVersion = 1, minWriterVersion = 2)
      .merge(Protocol.forTableFeature(AppendOnlyTableFeature))
    val upgradedProtocol = headProtocol
      .merge(Protocol.forTableFeature(DeletionVectorsTableFeature))
    val metadata = Metadata(schemaString = new StructType().add("c1", "long").json)

    val lines = Seq(
      DeltaSharingProtocol(deltaProtocol = headProtocol, version = 1L).json,
      DeltaSharingMetadata(deltaMetadata = metadata, version = 1L).json,
      DeltaSharingProtocol(deltaProtocol = upgradedProtocol, version = 2L).json
    )

    DeltaSharingLogFileSystem.constructLocalDeltaLogAcrossVersions(
      lines = lines,
      customTablePath = customTablePath,
      startingVersionOpt = Some(1L),
      endingVersionOpt = Some(3L)
    )

    // The head protocol (and metadata) seed the starting version's json file.
    val v1Lines = readLocalDeltaLogVersion(customTablePath, 1L)
    assert(v1Lines.contains(headProtocol.json),
      s"v1 log should contain the head protocol, got: $v1Lines")
    assert(v1Lines.contains(metadata.json),
      s"v1 log should contain the head metadata, got: $v1Lines")
    assert(!v1Lines.contains(upgradedProtocol.json),
      s"v1 log should not contain the v2 protocol upgrade, got: $v1Lines")

    // The protocol upgrade is written to v2's own json file (not left on the stale head protocol).
    val v2Lines = readLocalDeltaLogVersion(customTablePath, 2L)
    assert(v2Lines.contains(upgradedProtocol.json),
      s"v2 log should contain the protocol upgrade, got: $v2Lines")
    assert(!v2Lines.contains(headProtocol.json),
      s"v2 log should not repeat the head protocol, got: $v2Lines")

    // A version with no protocol change carries no protocol action at all.
    val v3Lines = readLocalDeltaLogVersion(customTablePath, 3L)
    assert(!v3Lines.contains(headProtocol.json) && !v3Lines.contains(upgradedProtocol.json),
      s"v3 log should carry no protocol action, got: $v3Lines")
  }

  test("constructLocalDeltaLogAcrossVersions keeps a single head protocol when unversioned") {
    val customTablePath = "constructUnversionedProtocol/table"
    // A server that doesn't emit historical protocols (or the flag/opt-in is off) returns a single
    // protocol with no version. It must still seed the starting version's json file unchanged.
    val headProtocol = Protocol(minReaderVersion = 1, minWriterVersion = 2)
    val metadata = Metadata(schemaString = new StructType().add("c1", "long").json)

    val lines = Seq(
      DeltaSharingProtocol(deltaProtocol = headProtocol).json,
      DeltaSharingMetadata(deltaMetadata = metadata, version = 1L).json
    )

    DeltaSharingLogFileSystem.constructLocalDeltaLogAcrossVersions(
      lines = lines,
      customTablePath = customTablePath,
      startingVersionOpt = Some(1L),
      endingVersionOpt = Some(2L)
    )

    val v1Lines = readLocalDeltaLogVersion(customTablePath, 1L)
    assert(v1Lines.contains(headProtocol.json),
      s"v1 log should contain the unversioned head protocol, got: $v1Lines")
  }
}
