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

package org.apache.spark.sql.delta

import java.io.File

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

trait DeltaSourceDeletionVectorTests extends StreamTest
  with DeletionVectorsTestUtils {

  import testImplicits._

  test("allow to delete files before starting a streaming query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }
      sql(s"DELETE FROM delta.`$inputDir`")
      (5 until 10).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }
      deltaLog.checkpoint()
      assert(deltaLog.readLastCheckpointFile().nonEmpty, "this test requires a checkpoint")

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((5 until 10).map(_.toString): _*))
    }
  }

  test("allow to delete files before staring a streaming query without checkpoint") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }
      sql(s"DELETE FROM delta.`$inputDir`")
      (5 until 7).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }
      assert(deltaLog.readLastCheckpointFile().isEmpty, "this test requires no checkpoint")

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((5 until 7).map(_.toString): _*))
    }
  }

  /**
   * If deletion vectors are expected here, return true if they are present. If none are expected,
   * return true if none are present.
   */
  protected def deletionVectorsPresentIfExpected(
      inputDir: String,
      expectDVs: Boolean): Boolean = {
    val deltaLog = DeltaLog.forTable(spark, inputDir)
    val filesWithDVs = getFilesWithDeletionVectors(deltaLog)
    logWarning(s"Expecting DVs=$expectDVs - found ${filesWithDVs.size}")
    if (expectDVs) {
      filesWithDVs.nonEmpty
    } else {
      filesWithDVs.isEmpty
    }
  }

  private def ignoreOperationsTest(
      inputDir: String,
      sourceOptions: Seq[(String, String)],
      sqlCommand: String,
      commandShouldProduceDVs: Option[Boolean] = None)(expectations: StreamAction*): Unit = {
    (0 until 10 by 2).foreach { i =>
      Seq(i, i + 1).toDF().coalesce(1).write.format("delta").mode("append").save(inputDir)
    }

    val df = spark.readStream.format("delta").options(sourceOptions.toMap).load(inputDir)
    val expectDVs = commandShouldProduceDVs.getOrElse(
      sqlCommand.toUpperCase().startsWith("DELETE"))

    val base = Seq(
      AssertOnQuery { q =>
        q.processAllAvailable()
        true
      },
      CheckAnswer((0 until 10): _*),
      AssertOnQuery { q =>
        sql(sqlCommand)
        deletionVectorsPresentIfExpected(inputDir, expectDVs)
      })

    testStream(df)((base ++ expectations): _*)
  }

  private def ignoreOperationsTestWithManualClock(
      inputDir: String,
      sourceOptions: Seq[(String, String)],
      sqlCommand1: String,
      sqlCommand2: String,
      command1ShouldProduceDVs: Option[Boolean] = None,
      command2ShouldProduceDVs: Option[Boolean] = None,
      expectations: List[StreamAction]): Unit = {

    (0 until 15 by 3).foreach { i =>
      Seq(i, i + 1, i + 2).toDF().coalesce(1).write.format("delta").mode("append").save(inputDir)
    }
    val log = DeltaLog.forTable(spark, inputDir)
    val commitVersionBeforeDML = log.update().version
    val df = spark.readStream.format("delta").options(sourceOptions.toMap).load(inputDir)
    def expectDVsInCommand(shouldProduceDVs: Option[Boolean], command: String): Boolean = {
      shouldProduceDVs.getOrElse(command.toUpperCase().startsWith("DELETE"))
    }
    val expectDVsInCommand1 = expectDVsInCommand(command1ShouldProduceDVs, sqlCommand1)
    val expectDVsInCommand2 = expectDVsInCommand(command2ShouldProduceDVs, sqlCommand2)

    // If it's expected to fail we must be sure not to actually process it in here,
    // or it'll fail too early instead of being caught by ExpectFailure.
    val shouldFailAfterCommands = expectations.exists(_.isInstanceOf[ExpectFailure[_]])

    val baseActions: Seq[StreamAction] = Seq(
      StartStream(
        Trigger.ProcessingTime("10 seconds"),
        new StreamManualClock(System.currentTimeMillis())),
      AdvanceManualClock(10L * 1000L),
      CheckAnswer((0 until 15): _*),
      AssertOnQuery { q =>
        // Make sure we only processed a single batch since the initial data load.
        q.commitLog.getLatestBatchId().get == 0
      },
      AssertOnQuery { q =>
        sql(sqlCommand1)
        deletionVectorsPresentIfExpected(inputDir, expectDVsInCommand1)
      },
      AssertOnQuery { q =>
        sql(sqlCommand2)
        deletionVectorsPresentIfExpected(inputDir, expectDVsInCommand2)
      },
      AdvanceManualClock(20L * 1000L)) ++
      (if (shouldFailAfterCommands) {
         Seq.empty[StreamAction]
       } else {
         Seq(
           // This makes it move to the next batch.
           AssertOnQuery(waitUntilBatchProcessed(1, _)),
           AssertOnQuery { q =>
             eventually("Next batch was never processed") {
               // Make sure we only processed a single batch since the initial data load.
               assert(q.commitLog.getLatestBatchId().get === 1)
             }
             true
           })
       })

    testStream(df)((baseActions ++ expectations): _*)
  }

  protected def waitUntilBatchProcessed(batchId: Int, currentStream: StreamExecution): Boolean = {
    eventually("Next batch was never processed") {
      if (!currentStream.exception.isDefined) {
        assert(currentStream.commitLog.getLatestBatchId().get >= batchId)
      }
    }
    if (currentStream.exception.isDefined) {
      throw currentStream.exception.get
    }
    true
  }

  protected def eventually[T](message: String)(func: => T): T = {
    try {
      Eventually.eventually(Timeout(streamingTimeout)) {
        func
      }
    } catch {
      case NonFatal(e) =>
        fail(message, e)
    }
  }

  testQuietly(s"deleting files fails query if ignoreDeletes = false") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        sourceOptions = Nil,
        sqlCommand = s"DELETE FROM delta.`$inputDir`",
        // Whole table deletes do not produce DVs.
        commandShouldProduceDVs = Some(false))(ExpectFailure[DeltaUnsupportedOperationException] {
        e =>
          for (msg <- Seq("Detected deleted data", "not supported", "ignoreDeletes", "true")) {
            assert(e.getMessage.contains(msg))
          }
      })
    }
  }

  Seq("ignoreFileDeletion", DeltaOptions.IGNORE_DELETES_OPTION).foreach { ignoreDeletes =>
    testQuietly(
      s"allow to delete files after staring a streaming query when $ignoreDeletes is true") {
      withTempDir { inputDir =>
        ignoreOperationsTest(
          inputDir.getAbsolutePath,
          sourceOptions = Seq(ignoreDeletes -> "true"),
          sqlCommand = s"DELETE FROM delta.`$inputDir`",
          // Whole table deletes do not produce DVs.
          commandShouldProduceDVs = Some(false))(
          AssertOnQuery { q =>
            Seq(10).toDF().write.format("delta").mode("append").save(inputDir.getAbsolutePath)
            q.processAllAvailable()
            true
          },
          CheckAnswer((0 to 10): _*))
      }
    }
  }

  case class SourceChangeVariant(
      label: String,
      query: File => String,
      answerWithIgnoreChanges: Seq[Int])

  val sourceChangeVariants: Seq[SourceChangeVariant] = Seq(
    // A partial file delete is treated like an update by the Source.
    SourceChangeVariant(
      label = "DELETE",
      query = inputDir => s"DELETE FROM delta.`$inputDir` WHERE value = 3",
      // 2 occurs in the same file as 3, so it gets duplicated during processing.
      answerWithIgnoreChanges = (0 to 10) :+ 2))

  for (variant <- sourceChangeVariants)
  testQuietly(
    "updating the source table causes failure when ignoreChanges = false" +
      s" - using ${variant.label}") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        sourceOptions = Nil,
        sqlCommand = variant.query(inputDir))(
        ExpectFailure[DeltaUnsupportedOperationException] { e =>
          for (msg <- Seq("data update", "not supported", "skipChangeCommits", "true")) {
            assert(e.getMessage.contains(msg))
          }
        })
    }
  }

  for (variant <- sourceChangeVariants)
  testQuietly(
    "allow to update the source table when ignoreChanges = true" +
      s" - using ${variant.label}") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        sourceOptions = Seq(DeltaOptions.IGNORE_CHANGES_OPTION -> "true"),
        sqlCommand = variant.query(inputDir))(
        AssertOnQuery { q =>
          Seq(10).toDF().write.format("delta").mode("append").save(inputDir.getAbsolutePath)
          q.processAllAvailable()
          true
        },
        CheckAnswer(variant.answerWithIgnoreChanges: _*))
    }
  }

  testQuietly("deleting files when ignoreChanges = true doesn't fail the query") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        sourceOptions = Seq(DeltaOptions.IGNORE_CHANGES_OPTION -> "true"),
        sqlCommand = s"DELETE FROM delta.`$inputDir`",
        // Whole table deletes do not produce DVs.
        commandShouldProduceDVs = Some(false))(
        AssertOnQuery { q =>
          Seq(10).toDF().write.format("delta").mode("append").save(inputDir.getAbsolutePath)
          q.processAllAvailable()
          true
        },
        CheckAnswer((0 to 10): _*))
    }
  }

  for (variant <- sourceChangeVariants)
  testQuietly("updating source table when ignoreDeletes = true fails the query" +
      s" - using ${variant.label}") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        sourceOptions = Seq(DeltaOptions.IGNORE_DELETES_OPTION -> "true"),
        sqlCommand = variant.query(inputDir))(
        ExpectFailure[DeltaUnsupportedOperationException] { e =>
          for (msg <- Seq("data update", "not supported", "skipChangeCommits", "true")) {
            assert(e.getMessage.contains(msg))
          }
        })
    }
  }

  private val allSourceOptions = Seq(
    Nil,
    List(DeltaOptions.IGNORE_DELETES_OPTION),
    List(DeltaOptions.IGNORE_CHANGES_OPTION),
    List(DeltaOptions.SKIP_CHANGE_COMMITS_OPTION))
    .map { options =>
      options.map(key => key -> "true")
    }

  for (sourceOption <- allSourceOptions)
  testQuietly(
    "subsequent DML commands are processed correctly in a batch - DELETE->DELETE" +
      s" - $sourceOption") {
    val expectations: List[StreamAction] =
      sourceOption.map(_._1) match {
        case List(DeltaOptions.IGNORE_DELETES_OPTION) | Nil =>
          // These two do not allow updates.
          ExpectFailure[DeltaUnsupportedOperationException] { e =>
            for (msg <- Seq("data update", "not supported", "skipChangeCommits", "true")) {
              assert(e.getMessage.contains(msg))
            }
          } :: Nil
        case List(DeltaOptions.IGNORE_CHANGES_OPTION) =>
          // The 4 and 5 are in the same file as 3, so the first DELETE is going to duplicate them.
          // 5 is still in the same file as 4 after the first DELETE, so the second DELETE is going
          // to duplicate it again.
          CheckAnswer((0 until 15) ++ Seq(4, 5, 5): _*) :: Nil
        case List(DeltaOptions.SKIP_CHANGE_COMMITS_OPTION) =>
          // This will completely ignore the DELETEs.
          CheckAnswer((0 until 15): _*) :: Nil
      }

    withTempDir { inputDir =>
      ignoreOperationsTestWithManualClock(
        inputDir.getAbsolutePath,
        sourceOptions = sourceOption,
        sqlCommand1 = s"DELETE FROM delta.`$inputDir` WHERE value == 3",
        sqlCommand2 = s"DELETE FROM delta.`$inputDir` WHERE value == 4",
        expectations = expectations)
    }
  }

  for (sourceOption <- allSourceOptions)
  // TODO(larsk-db): Reinstate once flakiness is fixed: testQuietly(
  ignore(
    "subsequent DML commands are processed correctly in a batch - INSERT->UPDATE" +
      s" - $sourceOption"
  ) {
    val expectations: List[StreamAction] = sourceOption.map(_._1) match {
      case List(DeltaOptions.IGNORE_DELETES_OPTION) | Nil =>
        // These two do not allow updates.
        ExpectFailure[DeltaUnsupportedOperationException] { e =>
          for (msg <- Seq("data update", "not supported", "skipChangeCommits", "true")) {
            assert(e.getMessage.contains(msg))
          }
        } :: Nil
      case List(DeltaOptions.IGNORE_CHANGES_OPTION) =>
        // 15 and 16 are in the same file, so 16 will get duplicated by the DELETE.
        CheckAnswer((0 to 16) ++ Seq(16): _*) :: Nil
      case List(DeltaOptions.SKIP_CHANGE_COMMITS_OPTION) =>
        // This will completely ignore the DELETE.
        CheckAnswer((0 to 16): _*) :: Nil
    }

    withTempDir { inputDir =>
      ignoreOperationsTestWithManualClock(
        inputDir.getAbsolutePath,
        sourceOptions = sourceOption,
        sqlCommand1 =
          s"INSERT INTO delta.`$inputDir` SELECT /*+ COALESCE(1) */ * FROM VALUES 15, 16",
        sqlCommand2 = s"DELETE FROM delta.`$inputDir` WHERE value == 15",
        expectations = expectations)
    }
  }
}

class DeltaSourceDeletionVectorsSuite extends DeltaSourceSuiteBase
  with DeltaSQLCommandTest
  with DeltaSourceDeletionVectorTests {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsInNewTables(spark.conf)
  }
}
