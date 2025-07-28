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

import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.model.{DeltaTableFiles, DeltaTableMetadata, Table}
import io.delta.sharing.spark.DeltaSharingUtils._

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.delta.sharing.TableRefreshResult
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
      refreshToken: Option[String]
    ): DeltaTableFiles = {
      val file = getAddFileStr()
      val dv = getDeletionVectorStr()
      DeltaTableFiles(
        version = 0L,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
        lines = Seq(file, dv)
      )
    }

    override def getFiles(table: Table, startingVersion: Long, endingVersion: Option[Long])
    : DeltaTableFiles = {
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
      includeHistoricalMetadata: Boolean): DeltaTableFiles = {
      val file = getAddFileStr()
      val dv = getDeletionVectorStr()
      val cdc = getCdcStr()
      DeltaTableFiles(
        version = 0L,
        respondedFormat = DeltaSharingRestClient.RESPONSE_FORMAT_DELTA,
        lines = Seq(file, dv, cdc)
      )
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
}
