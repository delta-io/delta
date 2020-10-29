/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import io.delta.standalone.internal.storage.HDFSReadOnlyLogStore
import io.delta.standalone.internal.util.GoldenTableUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

// scalastyle:off funsuite
import org.scalatest.FunSuite

/**
 * Instead of using Spark in this project to WRITE data and log files for tests, we have
 * io.delta.golden.GoldenTables do it instead. During tests, we then refer by name to specific
 * golden tables that that class is responsible for generating ahead of time. This allows us to
 * focus on READING only so that we may fully decouple from Spark and not have it as a dependency.
 *
 * See io.delta.golden.GoldenTables for documentation on how to ensure that the needed files have
 * been generated.
 */
class ReadOnlyLogStoreSuite extends FunSuite {
  // scalastyle:on funsuite

  test("read") {
    withGoldenTable("log-store-read") { tablePath =>
      val readStore = new HDFSReadOnlyLogStore(new Configuration())

      val deltas = Seq(0, 1).map(i => new File(tablePath, i.toString)).map(_.getCanonicalPath)
      assert(readStore.read(deltas.head) == Seq("zero", "none"))
      assert(readStore.read(deltas(1)) == Seq("one"))
    }
  }

  test("listFrom") {
    withGoldenTable("log-store-listFrom") { tablePath =>
      val readStore = new HDFSReadOnlyLogStore(new Configuration())
      val deltas = Seq(0, 1, 2, 3, 4)
        .map(i => new File(tablePath, i.toString))
        .map(_.toURI)
        .map(new Path(_))

      assert(readStore.listFrom(deltas.head).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(1)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(1, 2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(2)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(2, 3).map(_.toString))
      assert(readStore.listFrom(deltas(3)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Seq(3).map(_.toString))
      assert(readStore.listFrom(deltas(4)).map(_.getPath.getName)
        .filterNot(_ == "_delta_log").toArray === Nil)
    }
  }
}
