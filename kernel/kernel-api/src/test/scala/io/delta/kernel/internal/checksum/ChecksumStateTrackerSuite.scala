/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.checksum

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.util.{Collections, Optional}

import io.delta.kernel.data.Row
import io.delta.kernel.internal.actions.AddFile
import io.delta.kernel.internal.replay.LogReplayUtils
import io.delta.kernel.internal.replay.LogReplayUtils.UniqueFileActionTuple
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[ChecksumUtils.StateTracker]]'s allFiles bookkeeping, exercised directly rather
 * than through a full table computation. The seen-identity cap in particular is only reachable with
 * ~1000 distinct file actions, which is impractical to drive via real commits.
 */
class ChecksumStateTrackerSuite extends AnyFunSuite {

  /** A minimal AddFile at the given path (unique path => unique file identity). */
  private def addFile(path: String): AddFile = {
    val row: Row = AddFile.createAddFileRow(
      null, // statistics
      path,
      stringStringMapValue(Collections.emptyMap[String, String]()),
      100L.asInstanceOf[JLong], // size
      20L.asInstanceOf[JLong], // modificationTime
      true.asInstanceOf[JBoolean], // dataChange
      Optional.empty(), // deletionVector
      Optional.empty(), // tags
      Optional.empty(), // baseRowId
      Optional.empty(), // defaultRowCommitVersion
      Optional.empty() // stats
    )
    new AddFile(row)
  }

  /** The (path URI, dvId) identity for a file at the given path. */
  private def identity(path: String): UniqueFileActionTuple =
    LogReplayUtils.getUniqueFileAction(addFile(path))

  /** A StateTracker collecting allFiles, live-file cap raised so only the seen cap can fire. */
  private def collectingTracker(): ChecksumUtils.StateTracker = {
    val state = new ChecksumUtils.StateTracker()
    state.collectAllFiles = true
    // Raise the live-file map cap so it does not fire first (we want to isolate the seen cap).
    state.allFilesThreshold = Long.MaxValue
    state
  }

  test("recordAddFile abandons allFiles once the seen set exceeds MAX_SEEN_IDENTITIES") {
    val state = collectingTracker()
    val cap = ChecksumUtils.MAX_SEEN_IDENTITIES

    // Record exactly `cap` distinct adds and assert that all are collected.
    (0 until cap).foreach(i => state.recordAddFile(addFile(s"f$i")))
    assert(state.collectAllFiles)
    assert(state.collectedAllFiles().isPresent)
    assert(state.collectedAllFiles().get().size() === cap)

    // Re-recording an already-seen identity is a no-op.
    state.recordAddFile(addFile("f0"))
    assert(state.collectAllFiles)
    assert(state.seenIdentities.size() === cap)

    // Passing threshold triggers abandoning allFiles.
    state.recordAddFile(addFile(s"f$cap"))
    assert(!state.collectAllFiles)
    assert(!state.collectedAllFiles().isPresent)
    assert(state.addFilesByIdentity.isEmpty)
    assert(state.seenIdentities.isEmpty)
  }

  test("removeAddFile also counts toward the seen cap and can trigger abandonment") {
    val state = collectingTracker()
    val cap = ChecksumUtils.MAX_SEEN_IDENTITIES

    // Record exactly `cap` distinct removes and assert that all are collected.
    (0 until cap).foreach(i => state.removeAddFile(identity(s"r$i")))
    assert(state.collectAllFiles)

    // Passing threshold triggers abandoning allFiles.
    state.removeAddFile(identity(s"r$cap"))
    assert(!state.collectAllFiles)
    assert(state.seenIdentities.isEmpty)
  }
}
