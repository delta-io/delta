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

package org.apache.spark.sql.delta.managedcommit

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ConditionalCheckFailedException, CreateTableRequest, CreateTableResult, DescribeTableResult, GetItemRequest, GetItemResult, PutItemRequest, PutItemResult, ResourceInUseException, ResourceNotFoundException, TableDescription, UpdateItemRequest, UpdateItemResult}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.util.FileNames
import io.delta.dynamodbcommitstore.DynamoDBCommitOwnerClient
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

/**
 * An in-memory implementation of DynamoDB client for testing. Only the methods used by
 * `DynamoDBCommitOwnerClient` are implemented.
 */
class InMemoryDynamoDBClient extends AbstractAmazonDynamoDB {
  /**
   * The db has multiple tables (outer map). Each table has multiple entries (inner map).
   */
  val db: mutable.Map[String, mutable.Map[String, PerEntryData]] = mutable.Map.empty
  case class PerEntryData(
      lock: ReentrantReadWriteLock,
      data: mutable.Map[String, AttributeValue])

  private def getTableData(tableName: String): mutable.Map[String, PerEntryData] = {
    db.getOrElse(tableName, throw new ResourceNotFoundException("table does not exist"))
  }

  override def createTable(createTableRequest: CreateTableRequest): CreateTableResult = {
    val tableName = createTableRequest.getTableName
    if (db.contains(tableName)) {
      throw new ResourceInUseException("Table already exists")
    }
    db.getOrElseUpdate(tableName, mutable.Map.empty)
    new CreateTableResult().withTableDescription(
      new TableDescription().withTableName(tableName));
  }

  override def describeTable(tableName: String): DescribeTableResult = {
    if (!db.contains(tableName)) {
      throw new ResourceNotFoundException("table does not exist")
    }
    val tableDesc =
      new TableDescription().withTableName(tableName).withTableStatus("ACTIVE")
    new DescribeTableResult().withTable(tableDesc)
  }

  override def getItem(getItemRequest: GetItemRequest): GetItemResult = {
    val table = getTableData(getItemRequest.getTableName)
    val tableId = getItemRequest.getKey.values().iterator().next();
    val entry = table.getOrElse(tableId.getS,
      throw new ResourceNotFoundException("table does not exist"))
    val lock = entry.lock.readLock()
    try {
      lock.lock()
      val result = new GetItemResult()
      getItemRequest.getAttributesToGet.forEach(attr => {
        entry.data.get(attr).foreach(result.addItemEntry(attr, _))
      })
      result
    } finally {
      lock.unlock()
    }
  }

  override def putItem(putItemRequest: PutItemRequest): PutItemResult = {
    val table = getTableData(putItemRequest.getTableName)
    val item = putItemRequest.getItem
    val tableId = item.get("tableId").getS
    if (table.contains(tableId)) {
      throw new ResourceInUseException("table already exists")
    }
    val entry = PerEntryData(new ReentrantReadWriteLock(), item.asScala)
    // This is not really safe, but tableId is a UUID, so it should be fine.
    table.put(tableId, entry)
    new PutItemResult()
  }

  override def updateItem(request: UpdateItemRequest): UpdateItemResult = {
    val table = getTableData(request.getTableName)
    val tableId = request.getKey.values().iterator().next();
    val entry = table.getOrElse(tableId.getS,
      throw new ResourceNotFoundException("table does not exist"))
    val lock = entry.lock.writeLock()
    try {
      lock.lock()
      request.getExpected.forEach((attr, expectedVal) => {
        val actualVal = entry.data.getOrElse(attr,
          throw new ConditionalCheckFailedException("Expected attr not found"))
        if (actualVal != expectedVal.getValue) {
          throw new ConditionalCheckFailedException("Value does not match")
        }
      })
      request.getAttributeUpdates.forEach((attr, update) => {
        if (attr != "commits") {
          entry.data.put(attr, update.getValue)
        } else {
          val commits = update.getValue.getL.asScala
          if (update.getAction == "ADD") {
            val existingCommits =
              entry.data.get("commits").map(_.getL.asScala).getOrElse(List())
            entry.data.put(
              "commits", new AttributeValue().withL((existingCommits ++ commits).asJava))
          } else if (update.getAction == "PUT") {
            entry.data.put("commits", update.getValue)
          } else {
            throw new IllegalArgumentException("Unsupported action")
          }
        }
      })
      new UpdateItemResult()
    } finally {
      lock.unlock()
    }
  }
}

case class TestDynamoDBCommitOwnerBuilder(batchSize: Long) extends CommitOwnerBuilder {
    override def getName: String = "test-dynamodb"
    override def build(spark: SparkSession, config: Map[String, String]): CommitOwnerClient = {
        new DynamoDBCommitOwnerClient(
          "testTable",
          "test-endpoint",
          new InMemoryDynamoDBClient(),
          batchSize)
    }
}

abstract class DynamoDBCommitOwnerClientSuite(batchSize: Long)
  extends CommitOwnerClientImplSuiteBase {

  override protected def createTableCommitOwnerClient(
      deltaLog: DeltaLog)
      : TableCommitOwnerClient = {
    val cs = TestDynamoDBCommitOwnerBuilder(batchSize = batchSize).build(spark, Map.empty)
    val tableConf = cs.registerTable(
      deltaLog.logPath,
      currentVersion = -1L,
      Metadata(),
      Protocol(1, 1))
    TableCommitOwnerClient(cs, deltaLog, tableConf)
  }

  override protected def registerBackfillOp(
      tableCommitOwnerClient: TableCommitOwnerClient,
      deltaLog: DeltaLog,
      version: Long): Unit = {
    tableCommitOwnerClient.backfillToVersion(version)
  }

  override protected def validateBackfillStrategy(
      tableCommitOwnerClient: TableCommitOwnerClient,
      logPath: Path,
      version: Long): Unit = {
    val lastExpectedBackfilledVersion = (version - (version % batchSize)).toInt
    val unbackfilledCommitVersionsAll = tableCommitOwnerClient
      .getCommits().getCommits.map(_.getVersion)
    val expectedVersions = lastExpectedBackfilledVersion + 1 to version.toInt

    assert(unbackfilledCommitVersionsAll == expectedVersions)
    (0 to lastExpectedBackfilledVersion).foreach { v =>
      assertBackfilled(v, logPath, Some(v))
    }
  }

  protected def validateGetCommitsResult(
      result: GetCommitsResponse,
      startVersion: Option[Long],
      endVersion: Option[Long],
      maxVersion: Long): Unit = {
    val commitVersions = result.getCommits.map(_.getVersion)
    val lastExpectedBackfilledVersion = (maxVersion - (maxVersion % batchSize)).toInt
    val expectedVersions = lastExpectedBackfilledVersion + 1 to maxVersion.toInt
    assert(commitVersions == expectedVersions)
    assert(result.getLatestTableVersion == maxVersion)
  }

  for (skipPathCheck <- Seq(true, false))
  test(s"skipPathCheck should work correctly [skipPathCheck = $skipPathCheck]") {
    withTempTableDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.toString)
      val logPath = log.logPath
      writeCommitZero(logPath)
      val dynamoDB = new InMemoryDynamoDBClient();
      val commitOwner = new DynamoDBCommitOwnerClient(
        "testTable",
        "test-endpoint",
        dynamoDB,
        batchSize,
        1, // readCapacityUnits
        1, // writeCapacityUnits
        skipPathCheck)
      val tableConf = commitOwner.registerTable(
        logPath,
        -1L,
        Metadata(),
        Protocol(1, 1))
      val wrongTablePath = new Path(logPath.getParent, "wrongTable")
      val wrongLogPath = new Path(wrongTablePath, logPath.getName)
      val fs = wrongLogPath.getFileSystem(log.newDeltaHadoopConf())
      fs.mkdirs(wrongTablePath)
      fs.mkdirs(FileNames.commitDirPath(wrongLogPath))
      val wrongTablePathTableCommitOwner = new TableCommitOwnerClient(
        commitOwner, wrongLogPath, tableConf, log.newDeltaHadoopConf(), log.store)
      if (skipPathCheck) {
        // This should succeed because we are skipping the path check.
        val resp = commit(1L, 1L, wrongTablePathTableCommitOwner)
        assert(resp.getVersion == 1L)
      } else {
        val e = intercept[CommitFailedException] {
          commit(1L, 1L, wrongTablePathTableCommitOwner)
        }
        assert(e.getMessage.contains("while the table is registered at"))
      }
    }
  }
}

class DynamoDBCommitOwnerClient5BackfillSuite extends DynamoDBCommitOwnerClientSuite(5)
