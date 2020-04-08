/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.storage

import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  ComparisonOperator,
  Condition,
  ConditionalCheckFailedException,
  ExpectedAttributeValue,
  PutItemRequest,
  QueryRequest
}
import java.util.NoSuchElementException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import org.apache.spark.sql.delta.storage


/*
  DynamoDB requirements:

  Single dynamodb table is required. Default table name is 'delta_log',
  it may be changed by setting spark property.

  Required key schema:
   - parentPath: String, HASH
   - filename: String, RANGE

  Table may be created with following aws cli command:

    aws --region us-east-1 dynamodb create-table \
      --table-name delta_log \
      --attribute-definitions AttributeName=parentPath,AttributeType=S \
                              AttributeName=filename,AttributeType=S \
      --key-schema AttributeName=parentPath,KeyType=HASH \
                   AttributeName=filename,KeyType=RANGE \
      --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

  Following spark properties are recognized:
  - spark.delta.DynamoDBLogStore.tableName - table name (defaults to 'delta_log')
  - spark.delta.DynamoDBLogStore.endpoint - endpoint (defaults to 'Amazon AWS')
  - spark.delta.DynamoDBLogStore.region - AWS region (defaults to 'us-east-1')
  - spark.delta.DynamoDBLogStore.fakeAuth - use for dynamodb-local (defaults to 'false')

*/
class DynamoDBLogStore(
  sparkConf: SparkConf,
  hadoopConf: Configuration) extends BaseExternalLogStore(sparkConf, hadoopConf) {

  import DynamoDBLogStore._

  private val tableName = sparkConf.get(s"${confPrefix}tableName", "delta_log")

  private val client: AmazonDynamoDBClient = DynamoDBLogStore.getClient(sparkConf)

  override protected def cleanCache(p: LogEntryMetadata => Boolean): Unit = {}

  override protected def writeCache(
    fs: FileSystem,
    logEntry: LogEntryMetadata,
    overwrite: Boolean): Unit = {
    try {
      logInfo(s"putItem $logEntry, overwrite: $overwrite")
      client.putItem(logEntry.asPutItemRequest(tableName, overwrite))
    } catch {
      case e: ConditionalCheckFailedException =>
        logError(e.toString)
        throw new java.nio.file.FileAlreadyExistsException(logEntry.path.toString)
      case e: Throwable =>
        logError(e.toString)
        throw new java.nio.file.FileSystemException(logEntry.path.toString)
    }
  }

  override protected def listFromCache(
    fs: FileSystem, resolvedPath: Path): Iterator[LogEntryMetadata] = {
    val filename = resolvedPath.getName
    val parentPath = resolvedPath.getParent
    logInfo(s"query parentPath = $parentPath AND filename >= $filename")

    val result = client
      .query(
        new QueryRequest(tableName)
          .withConsistentRead(true)
          .withKeyConditions(
            Map(
              "filename" -> new Condition()
                .withComparisonOperator(ComparisonOperator.GE)
                .withAttributeValueList(new AttributeValue(filename)),
              "parentPath" -> new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(parentPath.toString))
            ).asJava
          )
      )
      .getItems
      .asScala

    result.iterator.map(item => {
      logInfo(s"query result item: ${item.toString}")
      val parentPath = item.get("parentPath").getS
      val filename = item.get("filename").getS
      val tempPath = Option(item.get("tempPath").getS).map(new Path(_))
      val length = item.get("length").getN.toLong
      val modificationTime = item.get("modificationTime").getN.toLong
      storage.LogEntryMetadata(
        path = new Path(s"$parentPath/$filename"),
        tempPath = tempPath,
        length = length,
        modificationTime = modificationTime
      )
    })
  }
}

object DynamoDBLogStore {
  private val confPrefix = "spark.delta.DynamoDBLogStore."

  implicit def logEntryToWrapper(entry: LogEntryMetadata): LogEntryWrapper = LogEntryWrapper(entry)

  def getClient(sparkConf: SparkConf): AmazonDynamoDBClient = {

    var client = new AmazonDynamoDBClient()
    if (sparkConf.get(s"${confPrefix}fakeAuth", "false") == "true") {
      val auth = new BasicAWSCredentials("fakeMyKeyId", "fakeSecretAccessKey")
      client = new AmazonDynamoDBClient(auth)
    }

    val regionName = sparkConf.get(s"${confPrefix}region", "us-east-1")
    if (regionName != "") {
      client.setRegion(Region.getRegion(Regions.fromName(regionName)))
    }

    scala.util.control.Exception.ignoring(classOf[NoSuchElementException]) {
      client.setEndpoint(sparkConf.get(s"${confPrefix}host"))
    }

    client
  }
}

case class LogEntryWrapper(entry: LogEntryMetadata) {
  def asPutItemRequest(tableName: String, overwrite: Boolean): PutItemRequest = {
    val pr = new PutItemRequest(
      tableName,
      Map(
        "parentPath" -> new AttributeValue(entry.path.getParent.toString),
        "filename" -> new AttributeValue(entry.path.getName),
        "tempPath" -> (
          entry.tempPath
            .map(path => new AttributeValue(path.toString))
            .getOrElse(new AttributeValue().withN("0"))
          ),
        "length" -> new AttributeValue().withN(entry.length.toString),
        "modificationTime" -> new AttributeValue().withN(System.currentTimeMillis().toString),
        "isComplete" -> new AttributeValue().withS(entry.isComplete.toString)
      ).asJava
    )
    if (!overwrite) {
      pr.withExpected(Map("filename" -> new ExpectedAttributeValue(false)).asJava)
    } else pr
  }
}

