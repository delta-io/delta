/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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
package io.delta.standalone.internal

import java.io.File
import java.net.JarURLConnection
import java.nio.file.Files
import java.util.{Collections, UUID}
import java.util.jar.JarFile

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

import io.delta.standalone.{DeltaLog, Operation, Snapshot}

import io.delta.standalone.internal.DeltaLogImpl

class ShadedJarSuite extends FunSuite {
  test("audit files in delta-standalone jar") {
    val allowedFilePrefixes = Seq(
      "META-INF/MANIFEST.MF",
      "META-INF/LICENSE",
      "META-INF/NOTICE",
      "io/delta/standalone/",
      "shadedelta/"
    )
    // scalastyle:off classforname
    val classUrl = Class.forName("io.delta.standalone.DeltaLog").getResource("DeltaLog.class")
    // scalastyle:on classforname
    assert(classUrl != null, "Cannot find delta-standalone jar")
    val connection = classUrl.openConnection().asInstanceOf[JarURLConnection]
    val url = connection.getJarFileURL()
    val jarFile = new JarFile(new File(url.toURI))
    var numOfAllowedFiles = 0
    var foundParquetUtils = false
    try {
      jarFile.entries().asScala.filter(!_.isDirectory).map(_.toString).foreach(e => {
        val allowed = allowedFilePrefixes.exists(e.startsWith)
        if (allowed) numOfAllowedFiles += 1
        assert(allowed, s"$e is not expected to appear in delta-standalone jar")
        if (e.startsWith("io/delta/standalone/util/ParquetSchemaConverter")) {
          foundParquetUtils = true
        }
      })
      assert(
        numOfAllowedFiles > 20,
        "Found no enough files. The test might be broken as we picked up a wrong jar file to check")
      assert(foundParquetUtils, "cannot find ParquetSchemaConverter in the jar")
    } finally {
      jarFile.close()
    }
  }

  test("basic read and write to verify the final delta-standalone jar is working") {
    val dir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    try {
      val tablePath = new File("../golden-tables/src/test/resources/golden/data-reader-primitives")
        .getCanonicalFile
      FileUtils.copyDirectory(tablePath, dir)
      val log = DeltaLog.forTable(new Configuration(), dir.getCanonicalPath)
      log.asInstanceOf[DeltaLogImpl].checkpoint()
      log.startTransaction().commit(
        Collections.emptyList(),
        new Operation(Operation.Name.WRITE),
        "engineInfo")
      val iter = log.snapshot().open()
      try {
        assert(iter.asScala.size == 11)
      } finally {
        iter.close()
      }
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }
}
