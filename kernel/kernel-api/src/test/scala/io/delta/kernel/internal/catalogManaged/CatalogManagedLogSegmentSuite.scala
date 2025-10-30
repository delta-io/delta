/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.internal.catalogManaged

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.TableManager
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.{ActionUtils, MockFileSystemClientUtils}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class CatalogManagedLogSegmentSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with ActionUtils {

  implicit class OptionOps[T](option: Option[T]) {
    def asJava: Optional[T] = option match {
      case Some(value) => Optional.of(value)
      case None => Optional.empty()
    }
  }

  private def testLogSegment(
      testName: String,
      versionToLoad: Long,
      checkpointVersionOpt: Option[Long],
      deltaVersions: Seq[Long],
      ratifiedCommitVersions: Seq[Long],
      crcVersions: Seq[Long] = Seq.empty,
      expectedDeltaAndCommitVersionsOpt: Option[Seq[Long]] = None,
      expectedExceptionClassOpt: Option[Class[_ <: Exception]] = None): Unit = {
    // TODO: test with ratified=inline

    test(testName + " - ratified=materialized") {
      val checkpointFile = checkpointVersionOpt.map(v => classicCheckpointFileStatus(v)).toSeq
      val deltaFiles = deltaFileStatuses(deltaVersions)
      val crcFiles = crcVersions.map(checksumFileStatus)
      val ratifiedCommitParsedLogDatas = parsedRatifiedStagedCommits(ratifiedCommitVersions)

      val engine = createMockFSListFromEngine(checkpointFile ++ deltaFiles ++ crcFiles)

      val testSchema = new StructType().add("c1", IntegerType.INTEGER);

      val builder = TableManager
        .loadSnapshot(dataPath.toString)
        .asInstanceOf[SnapshotBuilderImpl]
        .atVersion(versionToLoad)
        .withProtocolAndMetadata(new Protocol(1, 2), testMetadata(testSchema))
        .withLogData(ratifiedCommitParsedLogDatas.toList.asJava)

      if (expectedExceptionClassOpt.isDefined) {
        val exception = intercept[Throwable] {
          // Ensure we load the LogSegment to identify any gaps/issues
          builder.build(engine).getLogSegment
        }
        assert(expectedExceptionClassOpt.get.isInstance(exception))
      } else {
        val snapshot = builder.build(engine)
        val logSegment = snapshot.getLogSegment

        val actualDeltaAndCommitFileStatuses = logSegment.getDeltas.asScala

        // Check: we got the expected versions
        val actualDeltaAndCommitVersions =
          actualDeltaAndCommitFileStatuses.map(x => FileNames.deltaVersion(x.getPath))
        assert(actualDeltaAndCommitVersions sameElements expectedDeltaAndCommitVersionsOpt.get)

        // Check: ratified commits take priority over published deltas when versions overlap
        val expectedRatifiedVersions =
          ratifiedCommitVersions.toSet.intersect(actualDeltaAndCommitVersions.toSet)

        actualDeltaAndCommitFileStatuses.map(_.getPath).foreach { path =>
          val version = FileNames.deltaVersion(path)
          if (expectedRatifiedVersions.contains(version)) {
            assert(FileNames.isStagedDeltaFile(path))
          } else {
            assert(FileNames.isPublishedDeltaFile(path))
          }
        }

        // Check: maxPublishedDeltaVersion
        val expectedMaxPublishedDeltaVersion = deltaVersions
          .filter(_ <= versionToLoad).reduceOption(_ max _).asJava

        assert(logSegment.getMaxPublishedDeltaVersion === expectedMaxPublishedDeltaVersion)

        // Check: lastSeenChecksum
        val expectedLastSeenChecksumVersion = crcVersions
          .filter(v => v <= versionToLoad && checkpointVersionOpt.forall(v >= _))
          .lastOption

        expectedLastSeenChecksumVersion match {
          case Some(expectedVersion) =>
            val checksumPath = logSegment.getLastSeenChecksum.get.getPath
            val actualVersion = FileNames.checksumVersion(checksumPath)
            assert(actualVersion === expectedVersion)
          case None =>
            assert(!logSegment.getLastSeenChecksum.isPresent)
        }
      }
    }
  }

  /////////////////////////////////////////////////////
  // LogSegment construction tests -- positive cases //
  /////////////////////////////////////////////////////

  // _delta_log: [                          10.checkpoint+json, 11.json, 12.json]
  // catalog:    [8.uuid.json, 9.uuid.json                                      ]
  testLogSegment(
    testName = "Build RT with ratified commits that are before first checkpoint",
    versionToLoad = 12L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 8L to 9L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 12L))

  // _delta_log: [          10.checkpoint+json, 11.json, 12.json, 13.json]
  // catalog:    [9.uuid.json, 10.uuid.json, 11.uuid.json                ]
  testLogSegment(
    testName = "Build RT with ratified commits that overlap w first checkpoint + deltas",
    versionToLoad = 13L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 13L,
    ratifiedCommitVersions = 9L to 11L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 13L))

  // _delta_log: [10.checkpoint+json, 11.json, 12.json, 13.json, 14.json, 15.json]
  // catalog:    [                  11.uuid.json, 12.uuid.json, 13.uuid.json     ]
  testLogSegment(
    testName = "Build RT with ratified commits that are contained within first checkpoint + deltas",
    versionToLoad = 15L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 15L,
    ratifiedCommitVersions = 11L to 13L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 15L))

  // _delta_log: [             10.checkpoint+json, 11.json, 12.json                      ]
  // catalog:    [9.uuid.json, 10.uuid.json 11.uuid.json, 12.uuid.json, 13.uuid.json     ]
  testLogSegment(
    testName = "Build RT with ratified commits that supersets the first checkpoint + deltas",
    versionToLoad = 13L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 9L to 13L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 13L))

  // _delta_log: [10.checkpoint+json, 11.json, 12.json                                 ]
  // catalog:    [                             12.uuid.json, 13.uuid.json, 14.uuid.json]
  testLogSegment(
    testName = "Build RT with ratified commits that overlap with end of deltas",
    versionToLoad = 14L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 12L to 14L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 14L))

  // _delta_log: [10.checkpoint+json, 11.json, 12.json                           ]
  // catalog:    [                                     13.uuid.json, 14.uuid.json]
  testLogSegment(
    testName = "Build RT with ratified commits that are after (no gap) the deltas",
    versionToLoad = 14L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 13L to 14L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 14L))

  // versionToLoad:     V
  // _delta_log: [10.checkpoint+json, 11.json, 12.json                           ]
  // catalog:    [                                     13.uuid.json, 14.uuid.json]
  testLogSegment(
    testName = "Build RT with commit versions > versionToLoad - versionToLoad = checkpoint version",
    versionToLoad = 10L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 13L to 14L,
    expectedDeltaAndCommitVersionsOpt = Some(Nil))

  // versionToLoad:                              V
  // _delta_log: [10.checkpoint+json, 11.json, 12.json                           ]
  // catalog:    [                                     13.uuid.json, 14.uuid.json]
  testLogSegment(
    testName = "Build RT with commit versions > versionToLoad - versionToLoad = delta version",
    versionToLoad = 12L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 13L to 14L,
    expectedDeltaAndCommitVersionsOpt = Some(11L to 12L))

  // _delta_log: [0.json,                                      ]
  // catalog:    [        1.uuid.json, 2.uuid.json, 3.uuid.json]
  testLogSegment(
    testName = "Build RT with only deltas and ratified commits (no checkpoint)",
    versionToLoad = 3L,
    checkpointVersionOpt = None,
    deltaVersions = Seq(0L),
    ratifiedCommitVersions = 1L to 3L,
    expectedDeltaAndCommitVersionsOpt = Some(0L to 3L))

  // _delta_log: [10.checkpoint+json,             ]
  // catalog:    [                    11.uuid.json]
  testLogSegment(
    testName = "Build RT when checkpoint version is the last version from the filesystem",
    versionToLoad = 11L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = Seq(10L),
    ratifiedCommitVersions = Seq(11L),
    expectedDeltaAndCommitVersionsOpt = Some(Seq(11L)))

  // scalastyle:off line.size.limit
  // _delta_log: [10.checkpoint+json, 11.json+crc, 12.json, 13.crc,                     15.crc]
  // catalog:    [                                          13.uuid.json, 14.uuid.json, 15.uuid.json, 16.uuid.json]
  // scalastyle:on line.size.limit
  testLogSegment(
    testName = "Build LogSegment with CRC files for unpublished versions",
    versionToLoad = 16L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = Seq(10L, 11L, 12L),
    ratifiedCommitVersions = 13L to 16L,
    crcVersions = Seq(11L, 13L, 15L),
    expectedDeltaAndCommitVersionsOpt = Some(11L to 16L))

  // TODO: Support this case in a followup PR
  // _delta_log: [                                                  ]
  // catalog:    [0.uuid.json, 1.uuid.json, 2.uuid.json, 3.uuid.json]
  /*
  testLogSegment(
    testName = "Build RT with only ratified commits",
    versionToLoad = 3L,
    checkpointVersionOpt = None,
    deltaVersions = Seq(),
    ratifiedCommitVersions = 0L to 3L,
    expectedDeltaAndCommitVersionsOpt = Some(0L to 3L))
   */

  /////////////////////////////////////////////////////
  // LogSegment construction tests -- negative cases //
  /////////////////////////////////////////////////////

  // _delta_log: [10.checkpoint+json, 11.json, 12.json                                ]
  // catalog:    [                                          14.uuid.json, 15.uuid.json]
  testLogSegment(
    testName = "Build RT with ratified commits that are after (with gap) the deltas => ERROR",
    versionToLoad = 15L,
    checkpointVersionOpt = Some(10L),
    deltaVersions = 10L to 12L,
    ratifiedCommitVersions = 14L to 15L,
    expectedExceptionClassOpt = Some(classOf[io.delta.kernel.exceptions.InvalidTableException]))

}
