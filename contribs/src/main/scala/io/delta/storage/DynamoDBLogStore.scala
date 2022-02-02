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

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  ComparisonOperator,
  Condition,
  ConditionalCheckFailedException,
  ExpectedAttributeValue,
  GetItemRequest,
  PutItemRequest,
  QueryRequest
}
import java.util.NoSuchElementException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions

/*
  DynamoDB requirements:

  Single dynamodb table is required. Default table name is 'delta_log',
  it may be changed by setting spark property.

  Required key schema:
   - tablePath: String, HASH
   - filename: String, RANGE

  Table may be created with following aws cli command:

    aws --region us-east-1 dynamodb create-table \
      --table-name delta_log \
      --attribute-definitions AttributeName=tablePath,AttributeType=S \
                              AttributeName=filename,AttributeType=S \
      --key-schema AttributeName=tablePath,KeyType=HASH \
                   AttributeName=filename,KeyType=RANGE \
      --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

  Following spark properties are recognized:
  - spark.delta.DynamoDBLogStore.tableName - table name (defaults to 'delta_log')
  - spark.delta.DynamoDBLogStore.endpoint - endpoint (defaults to 'Amazon AWS')
  - spark.delta.DynamoDBLogStore.region - AWS region (defaults to 'us-east-1')
  - spark.delta.DynamoDBLogStore.credentials.provider - name of class implementing
    `com.amazonaws.auth.AWSCredentialsProvider` (defaults to 'DefaultAWSCredentialsProviderChain')
 */
class DynamoDBLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends BaseExternalLogStore(sparkConf, hadoopConf) {

  import DynamoDBLogStore._

  private val tableName =
    sparkConf.get(s"${confPrefix}tableName", "delta_log")

  private val client: AmazonDynamoDBClient =
    DynamoDBLogStore.getClient(sparkConf)

  override protected def putDbEntry(
      entry: LogEntry,
      overwrite: Boolean = false
  ): Unit = {
    try {
      logDebug(s"putItem $entry, overwrite: $overwrite")
      client.putItem(entry.asPutItemRequest(tableName, overwrite))
    } catch {
      case e: ConditionalCheckFailedException =>
        logDebug(e.toString)
        throw new java.nio.file.FileAlreadyExistsException(
          entry.absoluteJsonPath.toString
        )
    }
  }

  override protected def getDbEntry(
      absoluteJsonPath: Path
  ): Option[LogEntry] = {
    Option(
      client
        .getItem(
          new GetItemRequest(
            tableName,
            Map(
              "tablePath" -> new AttributeValue(absoluteJsonPath.getParent.toString),
              "fileName" -> new AttributeValue(absoluteJsonPath.getName)
            ).asJava
          )
          .withConsistentRead(true)
        )
        .getItem
    ).map(itemToDbEntry)
  }

  override protected def getLatestDbEntry(
      tablePath: Path
  ): Option[LogEntry] = {
    client
      .query(
        new QueryRequest(tableName)
          .withConsistentRead(true)
          .withScanIndexForward(false)
          .withLimit(1)
          .withKeyConditions(
            Map(
              "tablePath" -> new Condition()
                .withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue(tablePath.toString))
            ).asJava
          )
      )
      .getItems
      .iterator
      .asScala
      .map(itemToDbEntry)
      .find(_ => true)
  }

  def itemToDbEntry(
      item: java.util.Map[String, AttributeValue]
  ): LogEntry = LogEntry(
    tablePath = new Path(item.get("tablePath").getS),
    fileName = item.get("fileName").getS,
    tempPath = item.get("tempPath").getS,
    complete = item.get("complete").getS == "true",
    commitTime = Option(item.get("commitTime")).map(_.getN.toLong)
  )
}

object DynamoDBLogStore {
  val confPrefix = "spark.delta.DynamoDBLogStore."

  implicit def logEntryToWrapper(entry: LogEntry): LogEntryWrapper =
    LogEntryWrapper(entry)

  def getClient(sparkConf: SparkConf): AmazonDynamoDBClient = {

    val credentialsProviderName = sparkConf.get(
      s"${confPrefix}credentials.provider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    // suggested org.apache.spark.util.Utils.classForName cannot be used as is private
    // scalastyle:off classforname
    val authClass = Class.forName(credentialsProviderName)
    // scalastyle:on classforname
    val auth = authClass
      .getConstructor()
      .newInstance()
      .asInstanceOf[com.amazonaws.auth.AWSCredentialsProvider];
    var client = new AmazonDynamoDBClient(auth)
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

case class LogEntryWrapper(entry: LogEntry) {
  def asPutItemRequest(
      tableName: String,
      overwrite: Boolean
  ): PutItemRequest = {
    var attributes = scala.collection.mutable.Map(
      "tablePath" -> new AttributeValue(entry.tablePath.toString),
      "fileName" -> new AttributeValue(entry.fileName),
      "tempPath" -> new AttributeValue(entry.tempPath),
      "complete" -> new AttributeValue().withS(entry.complete.toString)
    )

    if (entry.complete) {
      attributes += ("commitTime" -> new AttributeValue().withN(
        entry.commitTime.get.toString
      ))
    }

    val pr = new PutItemRequest(tableName, attributes.asJava)
    if (!overwrite) {
      pr.withExpected(
        Map("fileName" -> new ExpectedAttributeValue(false)).asJava
      )
    } else pr
  }
}
