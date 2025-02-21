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
package io.delta.kernel.internal.actions

import scala.collection.JavaConverters._

import io.delta.kernel.internal.tablefeatures.TableFeatures

import org.scalatest.funsuite.AnyFunSuite

class ProtocolSuite extends AnyFunSuite {
  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for TableFeature related methods on Protocol                                          //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // Invalid protocol versions/features throw validation errors
  Seq(
    // Test format:
    // minReaderVersion, minWriterVersion, readerFeatures, writerFeatures, expectedErrorMsg
    (0, 1, Set(), Set(), "minReaderVersion should be at least 1"),
    (1, 0, Set(), Set(), "minWriterVersion should be at least 1"),
    (
      // writer version doesn't support writer features
      1,
      2,
      Set("columnMapping"),
      Set(),
      "Reader features are not supported for the reader version: 1"),
    (
      // writer version doesn't support writer features
      1,
      2,
      Set(),
      Set("columnMapping"),
      "Writer features are not supported for the writer version: 2"),
    // you can't have reader version with feature support, but not the writer version
    (3, 5, Set(), Set(), "writer version doesn't support writer features: 5"),
    // columnMapping feature is not supported for reader version 1
    (1, 5, Set(), Set(), "Reader version 1 does not support readerWriter feature columnMapping"),
    (
      // readerWriter feature columnMapping is missing from the readerFeatures set
      3,
      7,
      Set(),
      Set("columnMapping"),
      "ReaderWriter feature columnMapping is not present in readerFeatures"),
    // minReaderVersion doesn't support readerWriter feature columnMapping requirement
    (
      1,
      7,
      Set(),
      Set("columnMapping"),
      "Reader version 1 does not support readerWriter feature columnMapping")).foreach {
    case (
          readerVersion,
          writerVersion,
          readerFeatures: Set[String],
          writerFeatures: Set[String],
          expectedError) =>
      test(s"Invalid protocol versions " +
        s"($readerVersion, $writerVersion, $readerFeatures, $writerFeatures)") {
        val protocol =
          new Protocol(readerVersion, writerVersion, readerFeatures.asJava, writerFeatures.asJava)
        val e = intercept[IllegalArgumentException] {
          protocol.validate()
        }
        assert(e.getMessage === expectedError)
      }
  }

  // Tests for getImplicitlySupportedFeatures, getExplicitlySupportedFeatures and
  // getImplicitlyAndExplicitlySupportedFeatures
  Seq(
    // Test format:
    // (minReaderVersion, minWriterVersion, expected features)
    (1, 1, Set()),
    (1, 2, Set("appendOnly", "invariants")),
    (1, 3, Set("appendOnly", "invariants", "checkConstraints")),
    (
      1,
      4,
      Set("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns")),
    (
      2,
      5,
      Set(
        "appendOnly",
        "invariants",
        "checkConstraints",
        "changeDataFeed",
        "generatedColumns",
        "columnMapping")),
    (
      2,
      6,
      Set(
        "appendOnly",
        "invariants",
        "checkConstraints",
        "changeDataFeed",
        "generatedColumns",
        "columnMapping",
        "identityColumns"))).foreach {
    case (minReaderVersion, minWriterVersion, expectedFeatures) =>
      test(s"getImplicitlySupportedFeatures with minReaderVersion $minReaderVersion and " +
        s"minWriterVersion $minWriterVersion") {
        val protocol = new Protocol(minReaderVersion, minWriterVersion)
        assert(
          protocol.getImplicitlySupportedFeatures.asScala.map(_.featureName()) === expectedFeatures)

        assert(
          protocol.getImplicitlyAndExplicitlySupportedFeatures.asScala.map(_.featureName()) ===
            expectedFeatures)

        assert(
          protocol.getExplicitlySupportedFeatures.asScala.map(_.featureName()) === Set())
      }
  }

  Seq(
    // Test format: readerFeatures, writerFeatures, expected set
    (Set(), Set(), Set()),
    (Set(), Set("rowTracking"), Set("rowTracking")),
    (Set(), Set("checkConstraints", "rowTracking"), Set("checkConstraints", "rowTracking")),
    (
      Set("columnMapping"),
      Set("columnMapping", "domainMetadata"),
      Set("columnMapping", "domainMetadata"))).foreach {
    case (
          readerFeatures: Set[String],
          writerFeatures: Set[String],
          expectedFeatureSet: Set[String]) =>
      test(s"getExplicitlySupportedFeatures $readerFeatures $writerFeatures") {
        val protocol = new Protocol(3, 7, readerFeatures.asJava, writerFeatures.asJava)
        assert(
          protocol.getExplicitlySupportedFeatures.asScala.map(_.featureName()) ===
            expectedFeatureSet)

        assert(
          protocol.getImplicitlyAndExplicitlySupportedFeatures.asScala.map(_.featureName()) ===
            expectedFeatureSet)

        assert(protocol.getImplicitlySupportedFeatures.asScala.map(_.featureName()) === Set())
      }
  }

  // Tests for `normalized
  Seq(
    // Test format: input, expected output out of the `normalized`

    // If the protocol has no table features, then the normalized shouldn't change
    (1, 1, Set[String](), Set[String]()) -> (1, 1, Set[String](), Set[String]()),
    (1, 2, Set[String](), Set[String]()) -> (1, 2, Set[String](), Set[String]()),
    (2, 5, Set[String](), Set[String]()) -> (2, 5, Set[String](), Set[String]()),

    // If the protocol has table features, then the normalized may or
    // may not have the table features
    (3, 7, Set[String](), Set("appendOnly", "invariants")) ->
      (1, 2, Set[String](), Set[String]()),
    (3, 7, Set[String](), Set("appendOnly", "invariants", "checkConstraints")) ->
      (1, 3, Set[String](), Set[String]()),
    (
      3,
      7,
      Set[String](),
      Set("appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns")) ->
      (1, 4, Set[String](), Set[String]()),
    (
      3,
      7,
      Set("columnMapping"),
      Set(
        "appendOnly",
        "invariants",
        "checkConstraints",
        "changeDataFeed",
        "generatedColumns",
        "columnMapping")) ->
      (2, 5, Set[String](), Set[String]()),

    // reader version is downgraded
    // can't downgrade the writer version, because version 2 (appendOnly) also has support for
    // invariants which is not supported in the writer features in the input
    (1, 7, Set[String](), Set("appendOnly")) -> (1, 7, Set[String](), Set[String]("appendOnly")),
    (3, 7, Set("columnMapping"), Set("columnMapping")) ->
      (2, 7, Set[String](), Set("columnMapping")),
    (3, 7, Set("columnMapping"), Set("columnMapping", "domainMetadata")) ->
      (2, 7, Set[String](), Set("columnMapping", "domainMetadata"))).foreach {
    case (
          (readerVersion, writerVersion, readerFeatures, writerFeatures),
          (
            expReaderVersion,
            expWriterVersion,
            expReaderFeatures,
            expWriterFeatures)) =>
      test(s"normalized $readerVersion $writerVersion $readerFeatures $writerFeatures") {
        val protocol =
          new Protocol(readerVersion, writerVersion, readerFeatures.asJava, writerFeatures.asJava)
        val normalized = protocol.normalized()
        assert(normalized.getMinReaderVersion === expReaderVersion)
        assert(normalized.getMinWriterVersion === expWriterVersion)
        assert(normalized.getReaderFeatures.asScala === expReaderFeatures)
        assert(normalized.getWriterFeatures.asScala === expWriterFeatures)
      }
  }

  // Tests for `denormalized`
  Seq(
    // Test format: input, expected output out of the `denormalized`
    (1, 1, Set[String](), Set[String]()) -> (1, 7, Set[String](), Set[String]()),
    (1, 2, Set[String](), Set[String]()) -> (1, 7, Set[String](), Set("appendOnly", "invariants")),
    (2, 5, Set[String](), Set[String]()) -> (
      2,
      7,
      Set[String](),
      Set(
        "appendOnly",
        "invariants",
        "checkConstraints",
        "changeDataFeed",
        "generatedColumns",
        "columnMapping")),

    // invalid protocol versions (2, 3)
    (2, 3, Set[String](), Set[String]()) -> (
      1,
      7,
      Set[String](),
      Set("appendOnly", "invariants", "checkConstraints")),

    // shouldn't change the protocol already has the table feature set support
    (3, 7, Set[String](), Set("appendOnly", "invariants")) ->
      (3, 7, Set[String](), Set("appendOnly", "invariants")),
    (3, 7, Set[String](), Set("appendOnly", "invariants", "checkConstraints")) ->
      (3, 7, Set[String](), Set("appendOnly", "invariants", "checkConstraints"))).foreach {
    case (
          (readerVersion, writerVersion, readerFeatures, writerFeatures),
          (
            expReaderVersion,
            expWriterVersion,
            expReaderFeatures,
            expWriterFeatures)) =>
      test(s"denormalized $readerVersion $writerVersion $readerFeatures $writerFeatures") {
        val protocol =
          new Protocol(readerVersion, writerVersion, readerFeatures.asJava, writerFeatures.asJava)
        val denormalized = protocol.denormalized()
        assert(denormalized.getMinReaderVersion === expReaderVersion)
        assert(denormalized.getMinWriterVersion === expWriterVersion)
        assert(denormalized.getReaderFeatures.asScala === expReaderFeatures)
        assert(denormalized.getWriterFeatures.asScala === expWriterFeatures)
      }
  }

  // Tests for `withFeature` and `normalized`
  Seq(
    // can't downgrade the writer version, because version 2 (appendOnly) also has support for
    // invariants which is not supported in the writer features in the input
    Set("appendOnly") -> (1, 7, Set[String](), Set("appendOnly")),
    Set("invariants") -> (1, 7, Set[String](), Set[String]("invariants")),
    Set("appendOnly", "invariants") -> (1, 2, Set[String](), Set[String]()),
    Set("checkConstraints") -> (1, 7, Set[String](), Set("checkConstraints")),
    Set("changeDataFeed") -> (1, 7, Set[String](), Set("changeDataFeed")),
    Set("appendOnly", "invariants", "checkConstraints") -> (1, 3, Set[String](), Set[String]()),
    Set("generatedColumns") -> (1, 7, Set[String](), Set("generatedColumns")),
    Set("columnMapping") -> (2, 7, Set(), Set("columnMapping")),
    Set("identityColumns") -> (1, 7, Set[String](), Set[String]("identityColumns")),

    // expect the dependency features also to be supported
    Set("icebergCompatV2") ->
      (2, 7, Set[String](), Set[String]("icebergCompatV2", "columnMapping")),
    Set("rowTracking") -> (
      1,
      7,
      Set[String](),
      Set[String]("rowTracking", "domainMetadata"))).foreach {
    case (features, (expReaderVersion, expWriterVersion, expReaderFeatures, expWriterFeatures)) =>
      test(s"withFeature $features") {
        val protocol = new Protocol(3, 7)
        val updated = protocol
          .withFeatures(features.map(TableFeatures.getTableFeature).asJava)
          .normalized()
        assert(updated.getMinReaderVersion === expReaderVersion)
        assert(updated.getMinWriterVersion === expWriterVersion)
        assert(updated.getReaderFeatures.asScala === expReaderFeatures)
        assert(updated.getWriterFeatures.asScala === expWriterFeatures)
      }
  }

  test("withFeature - can't add a feature at the current version") {
    val protocol = new Protocol(1, 2)
    val e = intercept[UnsupportedOperationException] {
      protocol.withFeatures(Set(TableFeatures.getTableFeature("columnMapping")).asJava)
    }
    assert(e.getMessage === "TableFeature requires higher reader protocol version")
  }

  // Tests for `merge` (also tests denormalized and normalized)
  Seq(
    // Test format: (protocol1, protocol2) -> expected merged protocol
    (
      (1, 1, Set[String](), Set[String]()),
      (1, 2, Set[String](), Set[String]())) ->
      (1, 2, Set[String](), Set[String]()),
    ((1, 2, Set[String](), Set[String]()), (1, 3, Set[String](), Set[String]())) ->
      (1, 3, Set[String](), Set[String]()),
    ((1, 4, Set[String](), Set[String]()), (2, 5, Set[String](), Set[String]())) ->
      (2, 5, Set[String](), Set[String]()),
    ((1, 4, Set[String](), Set[String]()), (2, 6, Set[String](), Set[String]())) ->
      (2, 6, Set[String](), Set[String]()),
    ((1, 2, Set[String](), Set[String]()), (1, 7, Set[String](), Set("invariants"))) ->
      (1, 2, Set[String](), Set[String]()),
    ((1, 2, Set[String](), Set[String]()), (3, 7, Set("columnMapping"), Set("columnMapping"))) ->
      (2, 7, Set[String](), Set("columnMapping", "invariants", "appendOnly")),
    (
      (1, 2, Set[String](), Set[String]()),
      (3, 7, Set("columnMapping"), Set("columnMapping", "domainMetadata"))) ->
      (2, 7, Set[String](), Set("domainMetadata", "columnMapping", "invariants", "appendOnly")),
    (
      (2, 5, Set[String](), Set[String]()),
      (3, 7, Set("v2Checkpoint"), Set("v2Checkpoint", "domainMetadata"))) ->
      (
        3,
        7,
        Set("columnMapping", "v2Checkpoint"),
        Set(
          "domainMetadata",
          "columnMapping",
          "v2Checkpoint",
          "invariants",
          "appendOnly",
          "checkConstraints",
          "changeDataFeed",
          "generatedColumns"))).foreach({
    case (
          (
            (readerVersion1, writerVersion1, readerFeatures1, writerFeatures1),
            (readerVersion2, writerVersion2, readerFeatures2, writerFeatures2)),
          (expReaderVersion, expWriterVersion, expReaderFeatures, expWriterFeatures)) =>
      test(s"merge $readerVersion1 $writerVersion1 $readerFeatures1 $writerFeatures1 " +
        s"$readerVersion2 $writerVersion2 $readerFeatures2 $writerFeatures2") {
        val protocol1 =
          new Protocol(
            readerVersion1,
            writerVersion1,
            readerFeatures1.asJava,
            writerFeatures1.asJava)
        val protocol2 = new Protocol(
          readerVersion2,
          writerVersion2,
          readerFeatures2.asJava,
          writerFeatures2.asJava)
        val merged = protocol1.merge(protocol2)
        assert(merged.getMinReaderVersion === expReaderVersion)
        assert(merged.getMinWriterVersion === expWriterVersion)
        assert(merged.getReaderFeatures.asScala === expReaderFeatures)
        assert(merged.getWriterFeatures.asScala === expWriterFeatures)
      }
  })
}
