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
package io.delta.spark.internal.v2.utils;

import com.google.common.base.Throwables;
import io.delta.kernel.exceptions.UnsupportedTableFeatureException;
import java.util.function.Supplier;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Translates Kernel's {@link UnsupportedTableFeatureException} raised on the DSv2 read path into
 * Delta's {@link DeltaUnsupportedTableFeatureException}.
 *
 * <p>Protocol validation lives inside Kernel for DSv2, so an unsupported reader feature surfaces as
 * Kernel's {@link UnsupportedTableFeatureException} with no Delta error class. Delta and downstream
 * consumers key on Delta's {@link DeltaUnsupportedTableFeatureException} / {@code
 * DELTA_UNSUPPORTED_FEATURES_FOR_READ}, so callers translate at every Kernel boundary that can
 * raise it on read.
 */
public final class KernelUnsupportedFeatureTranslator {

  private KernelUnsupportedFeatureTranslator() {}

  /**
   * Runs {@code snapshotLoad} (a Kernel snapshot build), translating a Kernel {@link
   * UnsupportedTableFeatureException} thrown directly by it into Delta's {@link
   * DeltaUnsupportedTableFeatureException}. Use this to wrap {@code loadLatestSnapshot} / {@code
   * loadSnapshotAt} calls on the read path so the unsupported-feature failure carries the {@code
   * DELTA_UNSUPPORTED_FEATURES_FOR_READ} error class. Any other exception propagates unchanged.
   */
  public static <T> T translatingUnsupportedFeature(Supplier<T> snapshotLoad) {
    try {
      return snapshotLoad.get();
    } catch (UnsupportedTableFeatureException e) {
      throw toUnsupportedReaderFeatureException(e);
    }
  }

  /**
   * Translates a Kernel {@link UnsupportedTableFeatureException} into Delta's {@link
   * DeltaUnsupportedTableFeatureException} ({@code DELTA_UNSUPPORTED_FEATURES_FOR_READ}), carrying
   * the unsupported feature name(s) and table path, and setting the Kernel exception as the cause.
   *
   * @param kernelException the Kernel exception to translate
   * @return the equivalent Delta exception, to be thrown by the caller
   */
  public static DeltaUnsupportedTableFeatureException toUnsupportedReaderFeatureException(
      UnsupportedTableFeatureException kernelException) {
    DeltaUnsupportedTableFeatureException translated =
        DeltaErrors.unsupportedReaderTableFeaturesInTableException(
            kernelException.getTablePath(),
            CollectionConverters.asScala(kernelException.getUnsupportedFeatures()));
    // Preserve the Kernel exception (and its stack) as the cause.
    translated.initCause(kernelException);
    return translated;
  }

  /**
   * Walks the cause chain of {@code throwable} looking for a Kernel {@link
   * UnsupportedTableFeatureException}. Kernel's action iterator wraps the exception in a {@link
   * RuntimeException} when it surfaces from {@code commit.getActions()}, so the meaningful type is
   * not always the direct cause.
   *
   * @return the Kernel exception if present in the chain, otherwise null
   */
  public static UnsupportedTableFeatureException findUnsupportedTableFeatureCause(
      Throwable throwable) {
    return Throwables.getCausalChain(throwable).stream()
        .filter(cause -> cause instanceof UnsupportedTableFeatureException)
        .map(cause -> (UnsupportedTableFeatureException) cause)
        .findFirst()
        .orElse(null);
  }
}
