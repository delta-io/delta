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

package io.delta.storage

import java.util.NoSuchElementException

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf

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
    - spark.delta.DynamoDBLogStore.credentials.provider - name of class implementing
  `com.amazonaws.auth.AWSCredentialsProvider` (defaults to 'DefaultAWSCredentialsProviderChain')

  Implementation Notes:
  - key schema
      - parentPath == LogEntryMetadata.path.getParent.toString
      - filename == LogEntryMetadata.path.getName
 */
class DynamoDBLogStore(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends BaseExternalLogStore(sparkConf, hadoopConf) {

  import DynamoDBLogStore._

  private val tableName = sparkConf.get(s"${confPrefix}tableName", "delta_log")

  private val client: AmazonDynamoDBClient = DynamoDBLogStore.getClient(sparkConf)

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
      fs: FileSystem,
      resolvedPath: Path): Iterator[LogEntryMetadata] = {
    val filename = resolvedPath.getName
    val parentPathStr = resolvedPath.getParent.toString
    logInfo(s"query parentPath = $parentPathStr AND filename >= $filename")

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
                .withAttributeValueList(new AttributeValue(parentPathStr))
            ).asJava
          )
      )
      .getItems
      .asScala

    result.iterator.map { item =>
      logInfo(s"query result item: ${item.toString}")
      parseItem(item)
    }
  }

  override protected def lookupInCache(
      fs: FileSystem,
      resolvedPath: Path): Option[LogEntryMetadata] = {
    val itemKey = Map(
        "parentPath" -> new AttributeValue(resolvedPath.getParent.toString),
        "filename" -> new AttributeValue(resolvedPath.getName)
      ).asJava

    val result = client.getItem(tableName, itemKey)

    if (result == null || result.getItem == null || result.getItem.isEmpty) {
      logDebug(s"lookupInCache: ${resolvedPath.toString} not found")
      None
    } else {
      logDebug(s"lookupInCache: ${resolvedPath.toString} found")
      Some(parseItem(result.getItem))
    }
  }

  // TODO: optimize this to use <= filename instead of <= modificationTime?
  override protected def deleteFromCacheAllOlderThan(
      fs: FileSystem,
      parentPath: Path,
      expirationTime: Long): Iterator[Path] = {
    val parentPathStr = parentPath.toString
    logInfo(s"delete query: parentPath = $parentPathStr AND modificationTime <= $expirationTime")

    val result = client
      .query(
        new QueryRequest(tableName)
          .withConsistentRead(true)
          // TODO modification time isn't a key?
          .withKeyConditions(
            Map(
              "parentPath" -> new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(parentPathStr)),
              // TODO: complete=true
              "modificationTime" -> new Condition()
                .withComparisonOperator(ComparisonOperator.LE)
                .withAttributeValueList(new AttributeValue().withN(expirationTime.toString))
            ).asJava
          )
      )
      .getItems
      .asScala

    result.foreach { item =>
      val itemKey = Map(
        "parentPath" -> item.get("parentPath"),
        "filename" -> item.get("filename")
      ).asJava

      client.deleteItem(tableName, itemKey)
    }

    result.iterator.map { item => new Path(item.get("tempPath").getS) }
  }
}

object DynamoDBLogStore {
  private val confPrefix = "spark.delta.DynamoDBLogStore."

  implicit def logEntryToWrapper(entry: LogEntryMetadata): LogEntryWrapper = LogEntryWrapper(entry)

  def parseItem(item: java.util.Map[String, AttributeValue] ): LogEntryMetadata = {
    val parentPath = item.get("parentPath").getS
    val filename = item.get("filename").getS
    val tempPath = new Path(item.get("tempPath").getS)
    val length = item.get("length").getN.toLong
    val modificationTime = item.get("modificationTime").getN.toLong
    val isComplete = item.get("isComplete").getS == "true"

    LogEntryMetadata(
      path = new Path(s"$parentPath/$filename"),
      tempPath = tempPath,
      length = length,
      isComplete = isComplete,
      modificationTime = modificationTime
    )
  }

  def getClient(sparkConf: SparkConf): AmazonDynamoDBClient = {
    val credentialsProviderName = sparkConf.get(
      s"${confPrefix}credentials.provider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    // suggested org.apache.spark.util.Utils.classForName cannot be used as is private
    // scalastyle:off classforname
    val authClass = Class.forName(credentialsProviderName)
    // scalastyle:on classforname
    val auth = authClass.getConstructor().newInstance()
      .asInstanceOf[com.amazonaws.auth.AWSCredentialsProvider];
    // TODO builder pattern is the suggested way
    val client = new AmazonDynamoDBClient(auth)
    // TODO default value: Regions.US_EAST_1
    val regionName = sparkConf.get(s"${confPrefix}region", "us-east-1")
    if (regionName != "") {
      client.setRegion(Region.getRegion(Regions.fromName(regionName)))
    }

    scala.util.control.Exception.ignoring(classOf[NoSuchElementException]) { // TODO why this way?
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
        "tempPath" -> new AttributeValue(entry.tempPath.toString),
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

