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

  val allowedClassPrefixes = Seq(
    // e.g. shadedForDelta/org/apache/iceberg/BaseTable.class
    "shadedForDelta/",
    // e.g. scala/collection/compat/immutable/ArraySeq.class
    // e.g. scala/jdk/CollectionConverters.class
    "scala/",
    // e.g. org/apache/spark/sql/delta/icebergShaded/IcebergTransactionUtils.class
    "org/apache/spark/sql/delta/icebergShaded/",
    // We explicitly include all the /delta/commands/convert classes we want, to ensure we don't
    // accidentally pull in some from delta-spark package.
    "org/apache/spark/sql/delta/commands/convert/IcebergFileManifest",
    "org/apache/spark/sql/delta/commands/convert/IcebergSchemaUtils",
    "org/apache/spark/sql/delta/commands/convert/IcebergTable",
    // e.g. org/apache/iceberg/transforms/IcebergPartitionUtil.class
    "org/apache/iceberg/",
    "com/github/benmanes/caffeine/"
  )

  test("audit files in assembly jar") {
    // Step 1: load the jar (and make sure it exists)
    // scalastyle:off classforname
    val classUrl = Class.forName("org.apache.spark.sql.delta.icebergShaded.IcebergConverter").getResource("IcebergConverter.class")
    // scalastyle:on classforname
    assert(classUrl != null, "Could not find delta-iceberg jar")
    val connection = classUrl.openConnection().asInstanceOf[JarURLConnection]
    val url = connection.getJarFileURL
    val jarFile = new JarFile(new File(url.toURI))

    // Step 2: Verify the JAR has the classes we want it to have
    try {
      val jarClasses = jarFile
        .entries()
        .asScala
        .filter(!_.isDirectory)
        .map(_.toString)
        .filter(_.endsWith(".class")) // let's ignore any .properties or META-INF files for now
        .toSet

      val prohibitedJarClasses = jarClasses
        .filter { clazz => !allowedClassPrefixes.exists(prefix => clazz.startsWith(prefix)) }

      if (prohibitedJarClasses.nonEmpty) {
        throw new Exception(
            s"Prohibited jar class(es) found:\n- ${prohibitedJarClasses.mkString("\n- ")}"
          )
      }
    } finally {
      jarFile.close()
    }
  }
}
