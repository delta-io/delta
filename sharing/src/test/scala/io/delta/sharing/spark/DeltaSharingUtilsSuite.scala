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

package io.delta.sharing.spark

import scala.reflect.ClassTag

import org.apache.spark.{SharedSparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.storage.BlockId

class DeltaSharingUtilsSuite extends SparkFunSuite with SharedSparkContext {
  import DeltaSharingUtils._

  test("override single block in blockmanager works") {
    val blockId = BlockId(s"${DeltaSharingUtils.DELTA_SHARING_BLOCK_ID_PREFIX}_1")
    overrideSingleBlock[Int](blockId, 1)
    assert(SparkEnv.get.blockManager.getSingle[Int](blockId).get == 1)
    SparkEnv.get.blockManager.releaseLock(blockId)
    overrideSingleBlock[String](blockId, "2")
    assert(SparkEnv.get.blockManager.getSingle[String](blockId).get == "2")
    SparkEnv.get.blockManager.releaseLock(blockId)
  }

  def getSeqFromBlockManager[T: ClassTag](blockId: BlockId): Seq[T] = {
    val iterator = SparkEnv.get.blockManager
      .get[T](blockId)
      .map(
        _.data.asInstanceOf[Iterator[T]]
      )
      .get
    val seqBuilder = Seq.newBuilder[T]
    while (iterator.hasNext) {
      seqBuilder += iterator.next()
    }
    seqBuilder.result()
  }

  test("override iterator block in blockmanager works") {
    val blockId = BlockId(s"${DeltaSharingUtils.DELTA_SHARING_BLOCK_ID_PREFIX}_1")
    overrideIteratorBlock[Int](blockId, values = Seq(1, 2).toIterator)
    assert(getSeqFromBlockManager[Int](blockId) == Seq(1, 2))
    overrideIteratorBlock[String](blockId, values = Seq("3", "4").toIterator)
    assert(getSeqFromBlockManager[String](blockId) == Seq("3", "4"))
  }
}
