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

package org.apache.spark.sql.delta.coordinatedcommits

import java.util.Optional
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.amazonaws.services.dynamodbv2.{AbstractAmazonDynamoDB, AmazonDynamoDB, AmazonDynamoDBClient}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ConditionalCheckFailedException, CreateTableRequest, CreateTableResult, DescribeTableResult, GetItemRequest, GetItemResult, PutItemRequest, PutItemResult, ResourceInUseException, ResourceNotFoundException, TableDescription, UpdateItemRequest, UpdateItemResult}
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import io.delta.dynamodbcommitcoordinator.{DynamoDBCommitCoordinatorClient, DynamoDBCommitCoordinatorClientBuilder}
import io.delta.storage.commit.{CommitCoordinatorClient, CommitFailedException => JCommitFailedException, GetCommitsResponse => JGetCommitsResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession

/**
 * An in-memory implementation of DynamoDB client for testing. Only the methods used by
 * `DynamoDBCommitCoordinatorClient` are implemented.
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

case class TestDynamoDBCommitCoordinatorBuilder(batchSize: Long) extends CommitCoordinatorBuilder {
    override def getName: String = "test-dynamodb"
    override def build(
        spark: SparkSession, config: Map[String, String]): CommitCoordinatorClient = {
        new DynamoDBCommitCoordinatorClient(
          "testTable",
          "test-endpoint",
          new InMemoryDynamoDBClient(),
          batchSize)
    }
}

abstract class DynamoDBCommitCoordinatorClientSuite(batchSize: Long)
  extends CommitCoordinatorClientImplSuiteBase {

  override protected def createTableCommitCoordinatorClient(
      deltaLog: DeltaLog): TableCommitCoordinatorClient = {
    val cs = TestDynamoDBCommitCoordinatorBuilder(batchSize = batchSize).build(spark, Map.empty)
    val tableConf = cs.registerTable(
      deltaLog.logPath, Optional.empty(), -1L, Metadata(), Protocol(1, 1))
    TableCommitCoordinatorClient(cs, deltaLog, tableConf.asScala.toMap)
  }

  override protected def registerBackfillOp(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      deltaLog: DeltaLog,
      version: Long): Unit = {
    tableCommitCoordinatorClient.backfillToVersion(version)
  }

  override protected def validateBackfillStrategy(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      logPath: Path,
      version: Long): Unit = {
    val lastExpectedBackfilledVersion = (version - (version % batchSize)).toInt
    val unbackfilledCommitVersionsAll = tableCommitCoordinatorClient
      .getCommits().getCommits.asScala.map(_.getVersion)
    val expectedVersions = lastExpectedBackfilledVersion + 1 to version.toInt

    assert(unbackfilledCommitVersionsAll == expectedVersions)
    (0 to lastExpectedBackfilledVersion).foreach { v =>
      assertBackfilled(v, logPath, Some(v))
    }
  }

  protected def validateGetCommitsResult(
      result: JGetCommitsResponse,
      startVersion: Option[Long],
      endVersion: Option[Long],
      maxVersion: Long): Unit = {
    val commitVersions = result.getCommits.asScala.map(_.getVersion)
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
      val commitCoordinator = new DynamoDBCommitCoordinatorClient(
        "testTable",
        "test-endpoint",
        dynamoDB,
        batchSize,
        1, // readCapacityUnits
        1, // writeCapacityUnits
        skipPathCheck)
      val tableConf = commitCoordinator.registerTable(
        logPath, Optional.empty(), -1L, Metadata(), Protocol(1, 1))
      val wrongTablePath = new Path(logPath.getParent, "wrongTable")
      val wrongLogPath = new Path(wrongTablePath, logPath.getName)
      val fs = wrongLogPath.getFileSystem(log.newDeltaHadoopConf())
      fs.mkdirs(wrongTablePath)
      fs.mkdirs(FileNames.commitDirPath(wrongLogPath))
      val wrongTablePathTableCommitCoordinator = new TableCommitCoordinatorClient(
        commitCoordinator,
        wrongLogPath,
        tableConf.asScala.toMap,
        log.newDeltaHadoopConf(),
        log.store
      )
      if (skipPathCheck) {
        // This should succeed because we are skipping the path check.
        val resp = commit(1L, 1L, wrongTablePathTableCommitCoordinator)
        assert(resp.getVersion == 1L)
      } else {
        val e = intercept[JCommitFailedException] {
          commit(1L, 1L, wrongTablePathTableCommitCoordinator)
        }
        assert(e.getMessage.contains("while the table is registered at"))
      }
    }
  }

  test("builder should read dynamic configs from sparkSession") {
    class TestDynamoDBCommitCoordinatorBuilder extends DynamoDBCommitCoordinatorClientBuilder {
      override def getName: String = "dynamodb-test"
      override def createAmazonDDBClient(
          endpoint: String,
          credentialProviderName: String,
          hadoopConf: Configuration): AmazonDynamoDB = {
        assert(endpoint == "endpoint-1224")
        assert(credentialProviderName == "creds-1225")
        new InMemoryDynamoDBClient()
      }

      override def getDynamoDBCommitCoordinatorClient(
          coordinatedCommitsTableName: String,
          dynamoDBEndpoint: String,
          ddbClient: AmazonDynamoDB,
          backfillBatchSize: Long,
          readCapacityUnits: Int,
          writeCapacityUnits: Int,
          skipPathCheck: Boolean): DynamoDBCommitCoordinatorClient = {
        assert(coordinatedCommitsTableName == "tableName-1223")
        assert(dynamoDBEndpoint == "endpoint-1224")
        assert(backfillBatchSize == 1)
        assert(readCapacityUnits == 1226)
        assert(writeCapacityUnits == 1227)
        assert(skipPathCheck)
        new DynamoDBCommitCoordinatorClient(
          coordinatedCommitsTableName,
          dynamoDBEndpoint,
          ddbClient,
          backfillBatchSize,
          readCapacityUnits,
          writeCapacityUnits,
          skipPathCheck)
      }
    }
    val commitCoordinatorConf = JsonUtils.toJson(Map(
      "dynamoDBTableName" -> "tableName-1223",
      "dynamoDBEndpoint" -> "endpoint-1224"
    ))
    withSQLConf(
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
          "dynamodb-test",
        DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey ->
          commitCoordinatorConf,
        DeltaSQLConf.COORDINATED_COMMITS_DDB_AWS_CREDENTIALS_PROVIDER_NAME.key -> "creds-1225",
        DeltaSQLConf.COORDINATED_COMMITS_DDB_SKIP_PATH_CHECK.key -> "true",
        DeltaSQLConf.COORDINATED_COMMITS_DDB_READ_CAPACITY_UNITS.key -> "1226",
        DeltaSQLConf.COORDINATED_COMMITS_DDB_WRITE_CAPACITY_UNITS.key -> "1227") {
      // clear default builders
      CommitCoordinatorProvider.clearNonDefaultBuilders()
      CommitCoordinatorProvider.registerBuilder(new TestDynamoDBCommitCoordinatorBuilder())
      withTempTableDir { tempDir =>
        val tablePath = tempDir.getAbsolutePath
        spark.range(1).write.format("delta").mode("overwrite").save(tablePath)
        val log = DeltaLog.forTable(spark, tempDir.toString)
        val tableCommitCoordinatorClient = log.snapshot.tableCommitCoordinatorClientOpt.get
        assert(tableCommitCoordinatorClient
          .commitCoordinatorClient.isInstanceOf[DynamoDBCommitCoordinatorClient])
        assert(tableCommitCoordinatorClient.tableConf.contains("tableId"))
      }
    }
  }
}

class DynamoDBCommitCoordinatorClient5BackfillSuite extends DynamoDBCommitCoordinatorClientSuite(5)
