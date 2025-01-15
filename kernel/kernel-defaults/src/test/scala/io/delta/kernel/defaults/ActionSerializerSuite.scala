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

package io.delta.kernel.defaults;

import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.internal.actions.Protocol
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections
import scala.collection.JavaConverters._

class ActionSerializerSuite extends AnyFunSuite {

  test("protocol to json") {
    {
      val protocol = new Protocol(1, 2, null, null)
      val json = JsonUtils.rowToJson(protocol.toRow)
      assert(json === """{"minReaderVersion":1,"minWriterVersion":2}""")
    }
    {
      val protocol = new Protocol(1, 2, Collections.emptySet(), Collections.emptySet())
      val json = JsonUtils.rowToJson(protocol.toRow)
      assert(json === """{"minReaderVersion":1,"minWriterVersion":2}""")
    }
    {
      val protocol = new Protocol(3, 7, Collections.emptySet(), Collections.emptySet())
      val json = JsonUtils.rowToJson(protocol.toRow)
      // scalastyle:off line.size.limit
      assert(json === """{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":[]}""")
      // scalastyle:on line.size.limit
    }
    {
      val protocol = new Protocol(3, 7, Set("columnMapping").asJava, Set("appendOnly").asJava)
      val json = JsonUtils.rowToJson(protocol.toRow)
      // scalastyle:off line.size.limit
      assert(json === """{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["appendOnly"]}""")
      // scalastyle:on line.size.limit
    }
  }

}
