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

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.storage.CloseableIterator
import org.scalatest.FunSuite

import io.delta.standalone.VersionLog
import io.delta.standalone.actions.{Action => ActionJ, AddFile => AddFileJ}

import io.delta.standalone.internal.actions.{Action, AddFile}
import io.delta.standalone.internal.util.ConversionUtils

class VersionLogSuite extends FunSuite {
  private val defaultVersionNumber = 33
  private val listLength = 10
  private val stringList: java.util.List[String] = Collections.unmodifiableList(
    (0 until listLength).map { x =>
      AddFile(x.toString, Map.empty, 1, 1, dataChange = true).json
    }.asJava
  )
  private val actionList: java.util.List[ActionJ] = stringList
    .toArray
    .map(x => ConversionUtils.convertAction(Action.fromJson(x.toString)))
    .toList
    .asJava

  private val stringIterator = () => stringList.iterator

  private def stringCloseableIterator: CloseableIterator[String] =
    new CloseableIterator[String]() {
      val wrap: Iterator[String] = stringIterator().asScala

      override def next(): String = {
        wrap.next
      }

      override def close(): Unit = {}

      override def hasNext: Boolean = {
        wrap.hasNext
      }
    }

  private def actionCloseableIterator: CloseableIterator[ActionJ] =
    new CloseableIterator[ActionJ]() {
      val wrap: Iterator[String] = stringIterator().asScala

      override def next(): ActionJ = {
        ConversionUtils.convertAction(Action.fromJson(wrap.next))
      }

      override def close(): Unit = {}

      override def hasNext: Boolean = {
        wrap.hasNext
      }
    }

  /**
   * The method compares newVersionLog with default [[VersionLog]] property objects
   * @param newVersionLog the new VersionLog object generated in tests
   */
  private def checkVersionLog(
      newVersionLog: VersionLog,
      defaultActionIterator: CloseableIterator[ActionJ]
  ): Unit = {
    val newActionList = newVersionLog.getActions

    assert(newVersionLog.getVersion == defaultVersionNumber)
    assert(newActionList.size() == actionList.size())
    assert(
      newActionList
        .toArray()
        .zip(actionList.toArray())
        .count(x => x._1 == x._2) == newActionList.size()
    )

    val newActionIterator = newVersionLog.getActionsIterator

    (0 until listLength).foreach(_ => {
      assert(newActionIterator.hasNext && defaultActionIterator.hasNext)
      assert(
        newActionIterator.next().asInstanceOf[AddFileJ].getPath ==
          defaultActionIterator.next().asInstanceOf[AddFileJ].getPath
      )
    })
  }

  test("basic operation for VersionLog.java") {
    checkVersionLog(
      new VersionLog(defaultVersionNumber, actionList),
      actionCloseableIterator
    )
  }

  test("basic operation for MemoryOptimizedVersionLog.scala") {
    checkVersionLog(
      new MemoryOptimizedVersionLog(
        defaultVersionNumber,
        () => stringCloseableIterator
      ),
      actionCloseableIterator
    )
  }

  test("CloseableIterator should not be instantiated when supplier is not used") {
    var applyCounter: Int = 0
    val supplierWithCounter: () => CloseableIterator[String] =
      () => {
        applyCounter += 1
        stringCloseableIterator
      }
    val versionLogWithIterator = new MemoryOptimizedVersionLog(
      defaultVersionNumber,
      supplierWithCounter
    )

    assert(versionLogWithIterator.getVersion == defaultVersionNumber)

    // Calling counter increased only when a new CloseableIterator is instantiated.
    // i.e. MemoryOptimizedVersionLog.getActions() or MemoryOptimizedVersionLog.getActionsIterator()
    // is called. See supplierWithCounter for details.
    assert(applyCounter == 0)
    versionLogWithIterator.getActions
    assert(applyCounter == 1)
    versionLogWithIterator.getActionsIterator
    assert(applyCounter == 2)
  }
}
