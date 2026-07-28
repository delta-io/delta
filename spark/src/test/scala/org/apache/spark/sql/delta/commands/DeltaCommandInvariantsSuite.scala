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

package org.apache.spark.sql.delta.commands

import scala.util.{Failure, Success, Try}

import com.databricks.spark.util.Log4jUsageLogger

import org.apache.spark.{SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.DeltaOperations.EmptyCommit
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

class DeltaCommandInvariantsSuite extends SparkFunSuite with DeltaSQLCommandTest {

  for {
    shouldSucceed <- BOOLEAN_DOMAIN
    shouldThrow <- BOOLEAN_DOMAIN
  } test("command invariant check - " +
    s"shouldSucceed=$shouldSucceed, shouldThrow=$shouldThrow") {
    withTempDir { dir =>
      val path = dir.toString
      spark.range(10).write.format("delta").save(path)
      val deltaLog = DeltaLog.forTable(spark, path)
      val opType =
        "delta.assertions.unreliable.commandInvariantViolated"
      val events = Log4jUsageLogger.track {
        val result = Try {
          withSQLConf(
            DeltaSQLConf.COMMAND_INVARIANT_CHECKS_THROW.key -> shouldThrow.toString) {
            // Create an anonymous class, since checkCommandInvariant is protected here.
            new DeltaCommand {
              checkCommandInvariant(
                invariant = () => shouldSucceed,
                label = "shouldSucceed",
                op = EmptyCommit,
                deltaLog = deltaLog,
                parameters = Map("unused" -> 123),
                additionalInfo = Map("shouldSucceed" -> shouldSucceed.toString))
            }
          }
        }
        if (!shouldSucceed && shouldThrow) {
          result match {
            case Failure(e: SparkThrowable) =>
              checkErrorMatchPVals(
                e,
                "DELTA_COMMAND_INVARIANT_VIOLATION",
                parameters = Map(
                  "operation" -> "Empty Commit",
                  "uuid" -> ".*" // Doesn't matter
                )
              )
            case Failure(e) => throw e
            case Success(_) => fail("Expected Failure but got Success")
          }
        } else {
          assert(result.isSuccess)
        }
      }
      val violationEvents =
        events.filter(_.tags.get("opType").contains(opType))
      if (shouldSucceed) {
        assert(violationEvents.isEmpty)
      } else {
        assert(violationEvents.size === 1)
        val violationEvent = violationEvents.head
        val violationEventInfo = JsonUtils.fromJson[CommandInvariantCheckInfo](violationEvent.blob)
        assert(violationEventInfo === CommandInvariantCheckInfo(
          exceptionThrown = shouldThrow,
          id = violationEventInfo.id, // Don't check this, it's random.
          invariantExpression = "shouldSucceed",
          invariantParameters = Map("unused" -> 123),
          operation = "Empty Commit",
          operationParameters = Map.empty,
          additionalInfo = Map("shouldSucceed" -> shouldSucceed.toString)))
      }
    }
  }
}
