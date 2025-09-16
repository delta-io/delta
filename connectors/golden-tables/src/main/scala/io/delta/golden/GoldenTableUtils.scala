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

package io.delta.golden

import java.io.File

object GoldenTableUtils {

  lazy val classLoader = GoldenTableUtils.getClass.getClassLoader()
  lazy val goldenResourceURL = classLoader.getResource("golden")

  def goldenTablePath(name: String): String = {
    classLoader.getResource(s"golden/$name").getPath
  }

  def goldenTableFile(name: String): File = {
    new File(classLoader.getResource(s"golden/$name").getFile)
  }

  def allTableNames(): Seq[String] = {
    val root = new File(goldenResourceURL.getFile)

    def loop(dir: File): Seq[File] = {
      val children = Option(dir.listFiles()).getOrElse(Array.empty[File])
      val subdirs = children.filter(_.isDirectory)
      val here = if (new File(dir, "_delta_log").isDirectory) Seq(dir) else Seq.empty[File]
      here ++ subdirs.flatMap(loop)
    }

    val rootPath = root.toPath
    loop(root).map(f => rootPath.relativize(f.toPath).toString).sorted
  }
}
