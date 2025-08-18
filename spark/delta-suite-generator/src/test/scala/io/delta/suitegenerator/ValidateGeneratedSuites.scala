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

package io.delta.suitegenerator

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.scalatest.funsuite.AnyFunSuite

class ValidateGeneratedSuites extends AnyFunSuite {
  test("Generated suites are not manually modified") {
    // This test must be executed from the repository root for this relative path to work
    val outputDir = Paths.get(ModularSuiteGenerator.OUTPUT_PATH)
    val suitesValidator = new SuitesValidator(outputDir)
    ModularSuiteGenerator.generateSuites(suitesValidator)
  }
}

/**
 * Instead of writing to the files, validates that the files match the expected content.
 */
class SuitesValidator(override val outputDir: Path) extends SuitesWriter(outputDir) {
  override def writeFile(file: Path, content: String): Unit = {
    assert(Files.exists(file), s"File $file does not exist. Please run the generator to create it.")
    val fileContent = new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
    assert(fileContent == content,
      s"File $file does not match the expected content. Please run the generator to update it.")
    allFiles += file
  }
}
