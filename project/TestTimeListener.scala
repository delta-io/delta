/*
 * Copyright (2024-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import sbt.testing._
import java.io.{FileWriter, IOException}
import java.nio.file.{Files, Paths}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import java.lang.management.ManagementFactory
import scala.util.Using

/**
 * This class implements a custom Test Report Listener that logs detailed test execution
 * information, including test suite name, test case name, test duration, and test result.
 *
 * Each JVM writes its test results to a uniquely named CSV file based on the JVM's process ID,
 * ensuring no conflicts between concurrent JVMs.
 */
object TestTimeListener extends TestReportListener {

  /** testSuite -> Seq[(testName, duration, result)] */
  private val testResults = new ConcurrentHashMap[String, Seq[(String, Long, String)]]()

  /** Generate a unique file name per JVM using process ID and timestamp */
  private val jvmId: String = ManagementFactory.getRuntimeMXBean().getName.split("@")(0)
  private val individualTestCsvPath = s"test_results_jvm_${jvmId}.csv"

  /** Lock to ensure only one thread writes to a file at a time within this JVM */
  private val writeLock = new ReentrantLock()

  initialize()

  /** Clears the CSV file at the start of the test run. */
  private def initialize(): Unit = {
    clearCsvFile(individualTestCsvPath)
  }

  /////////////////
  // Public APIs //
  /////////////////

  /** Called when individual tests end. Records test suite, name, duration, and result. */
  override def testEvent(event: TestEvent): Unit = {
    event.detail.foreach { detail =>
      val suiteName = detail.fullyQualifiedName()
      val testName = detail.selector() match {
        case s: TestSelector => s.testName()
        case _ => "Unknown Test"
      }
      val testDuration = detail.duration()
      val testStatus = detail.status().toString

      println(s"Test suite: $suiteName, Test: $testName, Duration: $testDuration ms, Result: $testStatus")

      testResults.merge(suiteName, Seq((testName, testDuration, testStatus)), _ ++ _)
    }
  }

  /**
   * Called when a test suite ends. Writes the results to the JVM-specific CSV file and clears the
   * suite's data from the concurrent hash map.
   */
  override def endGroup(suiteName: String, result: TestResult): Unit = {
    println(s"Test suite $suiteName ended with result: $result")

    if (testResults.containsKey(suiteName)) {
      writeTestResultsToCsv(suiteName)
      testResults.remove(suiteName)
    } else {
      println(s"No test result data for test suite: $suiteName")
    }
  }

  override def startGroup(suiteName: String): Unit = {
    println(s"Test suite started: $suiteName")
  }

  override def endGroup(suiteName: String, t: Throwable): Unit = {
    println(s"Test suite $suiteName ended with error: ${t.getMessage}")
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  /** Escapes a CSV field by enclosing it in quotes if necessary and doubling internal quotes. */
  private def escapeCsvField(field: String): String = {
    if (field.contains(",") || field.contains("\"") || field.contains("\n")) {
      "\"" + field.replace("\"", "\"\"") + "\""
    } else {
      field
    }
  }

  /** Clears the given CSV file before writing. Ensures no data from previous runs is appended. */
  private def clearCsvFile(path: String): Unit = {
    try {
      Files.deleteIfExists(Paths.get(path))  // The file will be created automatically by FileWriter
    } catch {
      case e: IOException => println(s"Failed to clear CSV file: ${e.getMessage}")
    }
  }

  /**
   * Writes the test results for a specific suite to the JVM-specific CSV file. Expects that
   * [[testResults]] contains [[suiteName]].
   */
  private def writeTestResultsToCsv(suiteName: String): Unit = {
    writeLock.lock()
    try {
      Using.resource(new FileWriter(individualTestCsvPath, true)) { writer =>
        testResults.get(suiteName).foreach { case (testName, duration, result) =>
          val escapedSuiteName = escapeCsvField(suiteName)
          val escapedTestName = escapeCsvField(testName)
          val escapedResult = escapeCsvField(result)

          writer.write(s"$escapedSuiteName,$escapedTestName,$duration,$escapedResult\n")
        }
      }
    } catch {
      case e: IOException => println(s"Failed to write test results to file: ${e.getMessage}")
    } finally {
      writeLock.unlock()
    }
  }
}
