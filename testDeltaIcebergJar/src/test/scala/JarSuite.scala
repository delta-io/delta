/*
 * Copyright (2023-present) The Delta Lake Project Authors.
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
import java.net.JarURLConnection
import java.util.jar.JarFile

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

class JarSuite extends AnyFunSuite {
  test("audit files in assembly jar") {
    // Step 1: load the jar (and make sure it exists)
    // scalastyle:off classforname
    val classUrl = Class.forName("org.apache.spark.sql.delta.icebergShaded.IcebergConverter").getResource("IcebergConverter.class")
    // scalastyle:on classforname
    assert(classUrl != null, "Could not find delta-iceberg jar")
    println(classUrl)
    val connection = classUrl.openConnection().asInstanceOf[JarURLConnection]

    val url = connection.getJarFileURL()
    val jarFile = new JarFile(new File(url.toURI))

    // Step 2: Verify the JAR has the classes we want it to ahve
    try {
      val set = jarFile.entries().asScala.filter(!_.isDirectory).map(_.toString).toSet
      println(set.toList.sorted.mkString("\n"))

    } finally {
      jarFile.close()
    }
  }
}
