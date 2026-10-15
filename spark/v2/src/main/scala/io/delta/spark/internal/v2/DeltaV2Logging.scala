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
package io.delta.spark.internal.v2

import java.util.function.Supplier

import org.apache.spark.sql.delta.metering.DeltaLogging

/**
 * Delta V2 specialization of [[DeltaLogging]]. Frame-name suffixes passed here must be fixed
 * system-code literals; this trait supplies the shared `Delta` group and `v2.` prefix.
 */
private[v2] trait DeltaV2Logging extends DeltaLogging {
  private def deltaV2FrameName(nameSuffix: String): String = "v2." + nameSuffix

  protected final def recordFrameProfile[T](
      nameSuffix: String)(thunk: => T): T = {
    super.recordFrameProfile("Delta", deltaV2FrameName(nameSuffix))(thunk)
  }

  protected final def recordFrameProfileValue[T](
      nameSuffix: String,
      body: Supplier[T]): T =
    super.recordFrameProfileValue("Delta", deltaV2FrameName(nameSuffix), body)

  protected final def recordFrameProfileAction(
      nameSuffix: String,
      body: Runnable): Unit =
    super.recordFrameProfileAction("Delta", deltaV2FrameName(nameSuffix), body)
}

/** Java inheritance bridge for Delta V2 classes that do not already have a superclass. */
private[v2] abstract class DeltaV2JavaLogging extends DeltaV2Logging
