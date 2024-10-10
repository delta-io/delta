/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import com.etsy.sbt.checkstyle.CheckstylePlugin.autoImport.*
import com.lightbend.sbt.JavaFormatterPlugin.autoImport.javafmtCheckAll
import org.scalastyle.sbt.ScalastylePlugin.autoImport.*
import sbt.*
import sbt.Keys.*

object Checkstyle {

  /*
   *****************************
   * Scala checkstyle settings *
   *****************************
   */

  ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

  private lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
  private lazy val testScalastyle = taskKey[Unit]("testScalastyle")

  lazy val scalaStyleSettings = Seq(
    compileScalastyle := (Compile / scalastyle).toTask("").value,

    Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,

    testScalastyle := (Test / scalastyle).toTask("").value,

    Test / test := ((Test / test) dependsOn testScalastyle).value
  )

  /*
   ****************************
   * Java checkstyle settings *
   ****************************
   */

  private lazy val compileJavastyle = taskKey[Unit]("compileJavastyle")
  private lazy val testJavastyle = taskKey[Unit]("testJavastyle")

  def javaCheckstyleSettings(checkstyleFile: String): Def.SettingsDefinition = {
    // Can be run explicitly via: build/sbt $module/checkstyle
    // Will automatically be run during compilation (e.g. build/sbt compile)
    // and during tests (e.g. build/sbt test)
    Seq(
      checkstyleConfigLocation := CheckstyleConfigLocation.File(checkstyleFile),
      // if we keep the Error severity, `build/sbt` will throw an error and immediately stop at
      // the `checkstyle` phase (if error) -> never execute the `check-report` phase of
      // `checkstyle-report.xml` and `checkstyle-test-report.xml`. We need to ignore and throw
      // error if exists when checking *report.xml.
      checkstyleSeverityLevel := CheckstyleSeverityLevel.Ignore,

      compileJavastyle := {
        (Compile / checkstyle).value
        javaCheckstyle(streams.value.log, checkstyleOutputFile.value)
      },
      (Compile / compile) := ((Compile / compile) dependsOn compileJavastyle).value,

      testJavastyle := {
        (Test / checkstyle).value
        javaCheckstyle(streams.value.log, (Compile / target).value / "checkstyle-test-report.xml")
      },
      (Test / test) := ((Test / test) dependsOn (Test / testJavastyle)).value
    )
  }

  private def javaCheckstyle(log: Logger, reportFile: File): Unit = {
    val report = scala.xml.XML.loadFile(reportFile)

    val errors = (report \\ "file").flatMap { fileNode =>
      val file = fileNode.attribute("name").get.head.text
      (fileNode \ "error").map { error =>
        val line = error.attribute("line").get.head.text
        val message = error.attribute("message").get.head.text
        (file, line, message)
      }
    }

    if (errors.nonEmpty) {
      var errorMsg = "Found checkstyle errors"
      errors.foreach { case (file, line, message) =>
        val lineError = s"File: $file, Line: $line, Message: $message"
        log.error(lineError)
        errorMsg += ("\n" + lineError)
      }
      sys.error(errorMsg + "\n")
    }
  }

  // Enforce java code style
  lazy val javafmtCheckSettings = Seq(
    (Compile / compile) := ((Compile / compile) dependsOn (Compile / javafmtCheckAll)).value
  )
}
