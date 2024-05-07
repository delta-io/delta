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

package org.apache.spark.sql.delta.columnmapping

import org.apache.spark.sql.delta.ColumnMappingStreamingTestUtils
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.DeltaRuntimeException

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamTest

/**
 * Test suite for removing column mapping(CM) from a streaming table. Test different
 * scenarios with respect to table schema at different points of time. There are a few events we
 * are interested in:
 * Upgrade: enable column mapping
 * Downgrade: disable column mapping
 * Rename, Drop: rename, drop a column
 *
 * And we can decide when we start the streaming read with StartStreamRead.
 *
 * We test all the possible combinations of these events.
 *
 * Additionally, we test each scenario with schema tracking enabled which in general results in
 * a failure as schema tracking prohibits reading across an Upgrade.
 */
class RemoveColumnMappingStreamingReadSuite
  extends RemoveColumnMappingSuiteUtils
  with StreamTest
  with ColumnMappingStreamingTestUtils {

  // Here the physical/logical names don't change between start and the end
  // so it succeeds without schema tracking. Schema tracking prohibits reading across an upgrade.
  runScenario(StartStreamRead, Upgrade, Downgrade, SuccessAndFailSchemaTracking)
  // Here the physical names do change. We start with existing physical names but end without them.
  runScenario(Upgrade, StartStreamRead, Downgrade, FailNonAdditiveChange)
  // This is just reading from a normal table.
  runScenario(Upgrade, Downgrade, StartStreamRead, Success)

  // In all of this cases there is a non-additive change between the start of the stream and
  // the end.
  runScenario(StartStreamRead, Upgrade, Rename, Downgrade, FailNonAdditiveChange)
  runScenario(StartStreamRead, Upgrade, Drop, Downgrade, FailNonAdditiveChange)
  runScenario(StartStreamRead, Upgrade, Rename, Downgrade, Upgrade, FailNonAdditiveChange)
  runScenario(StartStreamRead, Upgrade, Drop, Downgrade, Upgrade, FailNonAdditiveChange)

  runScenario(Upgrade, StartStreamRead, Rename, Downgrade, FailNonAdditiveChange)
  runScenario(Upgrade, StartStreamRead, Drop, Downgrade, FailNonAdditiveChange)
  runScenario(Upgrade, StartStreamRead, Rename, Downgrade, Upgrade, FailNonAdditiveChange)
  runScenario(Upgrade, StartStreamRead, Drop, Downgrade, Upgrade, FailNonAdditiveChange)

  // In these cases  schema pinned at the start of the stream is different from the end schema on
  // the physical level.
  // Essentially, prohibit reading across the downgrade.
  runScenario(Upgrade, Rename, StartStreamRead, Downgrade, FailNonAdditiveChange)
  runScenario(Upgrade, Rename, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange)
  runScenario(Upgrade, Drop, StartStreamRead, Downgrade, FailNonAdditiveChange)

  // Here the schema at the end version has different physical names than at the start version.
  runScenario(Upgrade, Drop, StartStreamRead, Downgrade, Upgrade, FailNonAdditiveChange)

  // This is just reading from a table without column mapping.
  runScenario(Upgrade, Rename, Downgrade, StartStreamRead, Success)
  runScenario(Upgrade, Drop, Downgrade, StartStreamRead, Success)

  // Reading across the upgrade is fine without schema tracking.
  runScenario(Upgrade, Rename, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking)
  runScenario(Upgrade, Drop, Downgrade, StartStreamRead, Upgrade, SuccessAndFailSchemaTracking)

  private def runScenario(operations: Operation*): Unit = {
    // Run each scenario with and without schema tracking.
    for (shouldTrackSchema <- Seq(true, false)) {
      withTempPath { tempPath =>
        val metadataLocation = tempPath.getCanonicalPath
        val testName = generateTestName(operations, shouldTrackSchema)
        val schemaTrackingLocation = if (shouldTrackSchema) {
          Some(metadataLocation)
        } else {
          None
        }
        test(testName) {
          createTable()
          // Run all actions before the stream starts
          val streamStartIndex = operations.indexWhere(_ == StartStreamRead)
          operations.take(streamStartIndex).foreach(_.runOperation())
          // Run the rest as stream actions
          val remainingActions = operations.takeRight(operations.size - streamStartIndex)
          testStream(testTableStreamDf(schemaTrackingLocation))(
            remainingActions.flatMap {
              // Add an explicit StartStream so we can pass the checkpoint location.
              case StartStreamRead => Seq(StartStream(checkpointLocation = metadataLocation))
              // Fail scenarios when schema tracking is enabled.
              case FailNonAdditiveChange | SuccessAndFailSchemaTracking if shouldTrackSchema =>
                FailSchemaEvolution.toStreamActions
              case op: StreamActionLike => op.toStreamActions
              case op => Seq(
                Execute { _ =>
                  op.runOperation()
                })
            }: _*
          )
        }
      }
    }
  }

  private def generateTestName(operations: Seq[Operation], shouldTrackSchema: Boolean) = {
    val testNameSuffix = if (shouldTrackSchema) {
      " with schema tracking"
    } else {
      ""
    }
    val testName = operations.map(_.toString).mkString(", ") + testNameSuffix
    testName
  }

  private abstract class Operation {
    def runOperation(): Unit = {}
  }

  private case object StartStreamRead extends Operation

  private case object Upgrade extends Operation {
    override def runOperation(): Unit = {
      enableColumnMapping()
    }
  }

  private case object Downgrade extends Operation {
    override def runOperation(): Unit = {
      unsetColumnMappingProperty(useUnset = false)
      insertMoreRows()
    }
  }

  private case object Rename extends Operation {
    override def runOperation(): Unit = {
      renameColumn()
    }
  }

  private case object Drop extends Operation {
    override def runOperation(): Unit = {
      dropColumn()
    }
  }

  private case object FailNonAdditiveChange extends Operation with StreamActionLike {
    override def toStreamActions: Seq[StreamAction] = Seq(
      ProcessAllAvailableIgnoreError,
      ExpectInStreamSchemaChangeFailure
    )
  }

  private case object FailSchemaEvolution extends Operation with StreamActionLike {
    override def toStreamActions: Seq[StreamAction] = Seq(
      ProcessAllAvailableIgnoreError,
      ExpectMetadataEvolutionException
    )
  }


  private trait CheckAnswerStreamActionLike extends Operation with StreamActionLike {
    override def toStreamActions: Seq[StreamAction] = Seq(
      ProcessAllAvailable(),
      // The end state should be the original consecutive rows and then -1s.
      CheckAnswer(
        (0 until totalRows)
          .map( i => Row((0 until currentNumCols)
            .map(colInd => i + colInd): _*)) ++
          (totalRows until totalRows * 2).map(_ => Row(List.fill(currentNumCols)(-1): _*))
          : _*
      )
    )
  }

  // Expected to succeed and check the rows in the sink.
  private case object Success extends CheckAnswerStreamActionLike
  // Expected to fail with schema tracking enabled. Schema tracking in general puts more limitations
  // on which operations are permitted during a streaming read.
  private case object SuccessAndFailSchemaTracking extends CheckAnswerStreamActionLike

  protected val ExpectMetadataEvolutionException =
    ExpectFailure[DeltaRuntimeException](e =>
      assert(
        e.asInstanceOf[DeltaRuntimeException].getErrorClass ==
          "DELTA_STREAMING_METADATA_EVOLUTION" &&
          e.getStackTrace.exists(
            _.toString.contains("updateMetadataTrackingLogAndFailTheStreamIfNeeded"))
      )
    )

  trait StreamActionLike {
    def toStreamActions: Seq[StreamAction]
  }

  private def testTableStreamDf(schemaTrackingLocation: Option[String]) = {
    var streamReader = spark.readStream.format("delta")
    schemaTrackingLocation.foreach { loc =>
      streamReader = streamReader
        .option(DeltaOptions.SCHEMA_TRACKING_LOCATION, loc)
    }
    streamReader.table(testTableName)
  }

  private def createTable(columnMappingMode: String = "none"): Unit = {
    sql(s"""CREATE TABLE $testTableName
           |USING delta
           |TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = '$columnMappingMode',
           |  '${DeltaConfigs.CHANGE_DATA_FEED.key}' = 'true'
           |)
           |AS SELECT id as $firstColumn, id + 1 as $secondColumn, id + 2 as $thirdColumn
           |  FROM RANGE(0, $totalRows, 1, $numFiles)
           |""".stripMargin)
  }

  private def insertMoreRows(v: Int = -1): Unit = {
    val values = List.fill(currentNumCols)(v.toString).mkString(", ")
    sql(s"INSERT INTO $testTableName SELECT $values FROM $testTableName LIMIT $totalRows")
  }

  private def currentNumCols = deltaLog.update().schema.length
  override protected def isCdcTest: Boolean = false
}
