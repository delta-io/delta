/*
 * Copyright 2019 Databricks, Inc.
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
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  PutItemRequest,
  QueryRequest,
  Condition,
  ComparisonOperator,
  ExpectedAttributeValue,
  ConditionalCheckFailedException
}
import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkConf

import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions


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
  - spark.delta.DynamoDBLogStore.region - AWS region (defaults to 'us-east-1')

*/


class DynamoDBLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends BaseExternalLogStore(sparkConf, hadoopConf)
{
  import DynamoDBLogStore._

  private def logEntryToPutItemRequest(entry: LogEntry, overwrite: Boolean) = {
  }

  override def putLogEntry(
    logEntry: LogEntry,
    overwrite: Boolean): Unit =
  {
    val parentPath = logEntry.path.getParent()
    try {
      logInfo(s"putItem $logEntry, overwrite: $overwrite")
      client.putItem(logEntry.asPutItemRequest(tableName, overwrite))
    } catch {
      case e: ConditionalCheckFailedException =>
        logError(e.toString)
        throw new java.nio.file.FileAlreadyExistsException(logEntry.path.toString())
      case e: Throwable =>
        logError(e.toString)
        throw new java.nio.file.FileSystemException(logEntry.path.toString())
    }
  }

  override def listLogEntriesFrom(
    fs: FileSystem, parentPath: Path, from: Path): Iterator[LogEntry] =
  {
    val filename = from.getName()
    logInfo(s"query parentPath = $parentPath AND filename >= $filename")
    val result = client.query(
      new QueryRequest(tableName)
      .withConsistentRead(true)
      .withKeyConditions(
        Map(
          "filename" -> new Condition()
            .withComparisonOperator(ComparisonOperator.GE)
            .withAttributeValueList(new AttributeValue(filename)),
          "parentPath" -> new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(new AttributeValue(parentPath.toString()))
        ).asJava
      )
    ).getItems().asScala
    result.iterator.map( item => {
      logInfo(s"query result item: ${item.toString()}")
      val parentPath = item.get("parentPath").getS()
      val filename = item.get("filename").getS()
      val tempPath = Option(item.get("tempPath").getS()).map(new Path(_))
      val length = item.get("length").getN().toLong
      val modificationTime = item.get("modificationTime").getN().toLong
      val isComplete = Option(item.get("isComplete").getS()).map(_.toBoolean).getOrElse(false)
      LogEntry(
        path = new Path(s"$parentPath/$filename"),
        tempPath = tempPath,
        length = length,
        modificationTime = modificationTime,
        isComplete = isComplete
      )
    })
  }

  val tableNameConfKey = "spark.delta.DynamoDBLogStore.tableName"
  val tableName = sparkConf.get(tableNameConfKey, "delta_log")

  val regionConfKey = "spark.delta.DynamoDBLogStore.region"

  val client = {
    val client = new AmazonDynamoDBClient()
    val region = sparkConf.get(regionConfKey, "")
    if(region != "") {
      client.setRegion(Region.getRegion(Regions.fromName(region)))
    }
    client
  }

}

object DynamoDBLogStore {
  implicit def logEntryToWrapper(entry: LogEntry): LogEntryWrapper = LogEntryWrapper(entry)
}

case class LogEntryWrapper(entry: LogEntry) {
  def asPutItemRequest(tableName: String, overwrite: Boolean): PutItemRequest = {
    val pr = new PutItemRequest(
      tableName,
      Map(
        "parentPath" -> new AttributeValue(entry.path.getParent().toString()),
        "filename" -> new AttributeValue(entry.path.getName()),
        "tempPath" -> (
          entry.tempPath
          .map(path => new AttributeValue(path.toString))
          .getOrElse(new AttributeValue().withN("0"))
        ),
        "length" -> new AttributeValue().withN(entry.length.toString),
        "modificationTime" -> new AttributeValue().withN(System.currentTimeMillis().toString()),
        "isComplete" ->  new AttributeValue().withS(entry.isComplete.toString)
      ).asJava
    )
    if (!overwrite) {
      pr.withExpected(Map("filename" -> new ExpectedAttributeValue(false)).asJava)
    } else pr
  }
}
