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
package io.delta.kernel.defaults

import scala.collection.immutable.Seq

import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Protocol

/**
 * Integration test suite for Delta table features.
 */
class DeltaTableFeaturesSuite extends DeltaTableWriteSuiteBase {

  ///////////////////////////////////////////////////////////////////////////
  // Tests for deletionVector, v2Checkpoint table features
  ///////////////////////////////////////////////////////////////////////////
  Seq(
    // Test format: feature (readerWriter type), table property to enable the feature
    // For each feature, we test the following scenarios:
    // 1. able to write to an existing Delta table with the feature supported
    // 2. create a table with the feature supported and append data
    // 3. update an existing table with the feature supported
    ("deletionVectors", "delta.enableDeletionVectors", "true"),
    ("v2Checkpoint", "delta.checkpointPolicy", "v2")).foreach {
    case (feature, tblProp, propValue) =>
      test(s"able to write to an existing Delta table with $feature supported") {
        withTempDirAndEngine { (tablePath, engine) =>
          // Create a table with the feature supported
          spark.sql(s"CREATE TABLE delta.`$tablePath` (id INTEGER) USING delta " +
            s"TBLPROPERTIES ('$tblProp' = '$propValue')")

          checkReaderWriterFeaturesSupported(tablePath, feature)

          // Write data to the table using Kernel
          val testData = Seq(Map.empty[String, Literal] -> dataBatches1)
          appendData(
            engine,
            tablePath,
            isNewTable = false,
            testSchema,
            partCols = Seq.empty,
            testData)

          // Check the data using Kernel and Delta-Spark readers
          verifyWrittenContent(tablePath, testSchema, dataBatches1.flatMap(_.toTestRows))
        }
      }

      test(s"create a table with $feature supported") {
        withTempDirAndEngine { (tablePath, engine) =>
          val testData = Seq(Map.empty[String, Literal] -> dataBatches1)

          // create a table with the feature supported and append testData
          appendData(
            engine,
            tablePath,
            isNewTable = true,
            testSchema,
            partCols = Seq.empty,
            testData,
            tableProperties = Map(tblProp -> propValue))

          checkReaderWriterFeaturesSupported(tablePath, feature)

          // insert more data
          appendData(
            engine,
            tablePath,
            isNewTable = false,
            testSchema,
            partCols = Seq.empty,
            testData)

          // Check the data using Kernel and Delta-Spark readers
          verifyWrittenContent(
            tablePath,
            testSchema,
            dataBatches1.flatMap(_.toTestRows) ++ dataBatches1.flatMap(_.toTestRows))
        }
      }

      test(s"update an existing table with $feature support") {
        withTempDirAndEngine { (tablePath, engine) =>
          val testData = Seq(Map.empty[String, Literal] -> dataBatches1)

          // create a table without the table feature supported
          appendData(
            engine,
            tablePath,
            isNewTable = true,
            testSchema,
            partCols = Seq.empty,
            testData)

          checkNoReaderWriterFeaturesSupported(tablePath, feature)

          // insert more data and enable the feature
          appendData(
            engine,
            tablePath,
            isNewTable = false,
            testSchema,
            partCols = Seq.empty,
            testData,
            tableProperties = Map(tblProp -> propValue))

          checkReaderWriterFeaturesSupported(tablePath, feature)

          // Check the data using Kernel and Delta-Spark readers
          verifyWrittenContent(
            tablePath,
            testSchema,
            dataBatches1.flatMap(_.toTestRows) ++ dataBatches1.flatMap(_.toTestRows))
        }
      }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper methods
  ///////////////////////////////////////////////////////////////////////////
  def checkWriterFeaturesSupported(
      tblPath: String,
      expWriterOnlyFeatures: String*): Unit = {
    val protocol = getLatestProtocol(tblPath)
    val missingFeatures =
      expWriterOnlyFeatures.toSet -- protocol.writerFeatures.getOrElse(Set.empty)

    assert(
      missingFeatures.isEmpty,
      s"The following expected writer features are not supported: " +
        s"${missingFeatures.mkString(", ")}")
  }

  def checkNoWriterFeaturesSupported(tblPath: String, notExpWriterOnlyFeatures: String*): Unit = {
    val protocol = getLatestProtocol(tblPath)
    assert(protocol.writerFeatures.getOrElse(Set.empty)
      .intersect(notExpWriterOnlyFeatures.toSet).isEmpty)
  }

  def checkReaderWriterFeaturesSupported(
      tblPath: String,
      expectedReaderWriterFeatures: String*): Unit = {

    val protocol = getLatestProtocol(tblPath)

    val missingInWriterSet =
      expectedReaderWriterFeatures.toSet -- protocol.writerFeatures.getOrElse(Set.empty)
    assert(
      missingInWriterSet.isEmpty,
      s"The following expected readerWriter features are not supported in writerFeatures set: " +
        s"${missingInWriterSet.mkString(", ")}")

    val missingInReaderSet =
      expectedReaderWriterFeatures.toSet -- protocol.readerFeatures.getOrElse(Set.empty)
    assert(
      missingInReaderSet.isEmpty,
      s"The following expected readerWriter features are not supported in readerFeatures set: " +
        s"${missingInReaderSet.mkString(", ")}")
  }

  def checkNoReaderWriterFeaturesSupported(
      tblPath: String,
      notExpReaderWriterFeatures: String*): Unit = {
    val protocol = getLatestProtocol(tblPath)
    assert(protocol.readerFeatures.getOrElse(Set.empty)
      .intersect(notExpReaderWriterFeatures.toSet).isEmpty)
    assert(protocol.writerFeatures.getOrElse(Set.empty)
      .intersect(notExpReaderWriterFeatures.toSet).isEmpty)
  }

  def getLatestProtocol(tblPath: String): Protocol = {
    val deltaLog = DeltaLog.forTable(spark, tblPath)
    deltaLog.update()
    deltaLog.snapshot.protocol
  }
}
