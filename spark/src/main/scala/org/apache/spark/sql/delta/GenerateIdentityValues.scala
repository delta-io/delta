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

package org.apache.spark.sql.delta

import com.databricks.spark.util.MetricDefinitions
import com.databricks.spark.util.TagDefinitions.TAG_OP_TYPE
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.delta.cic.IdentitySequenceClient
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.util.{Clock, SystemClock}

/**
 * Returns the next generated IDENTITY column value based on the underlying
 * [[PartitionIdentityValueGenerator]].
 */
case class GenerateIdentityValues(generator: PartitionIdentityValueGenerator)
  extends LeafExpression with Nondeterministic {

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    generator.initialize(partitionIndex)
  }

  override protected def evalInternal(input: InternalRow): Long = generator.next()

  override def nullable: Boolean = false

  /**
   * Returns Java source code that can be compiled to evaluate this expression.
   * The default behavior is to call the eval method of the expression. Concrete expression
   * implementations should override this to do actual code generation.
   *
   * @param ctx a [[CodegenContext]]
   * @param ev  an [[ExprCode]] with unique terms.
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val generatorTerm = ctx.addReferenceObj("generator", generator,
      classOf[PartitionIdentityValueGenerator].getName)

    ctx.addPartitionInitializationStatement(s"$generatorTerm.initialize(partitionIndex);")
    ev.copy(code = code"""
        final ${CodeGenerator.javaType(dataType)} ${ev.value} = $generatorTerm.next();
        """, isNull = FalseLiteral)
  }

  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = LongType
}

object GenerateIdentityValues {
  def apply(start: Long, step: Long, highWaterMarkOpt: Option[Long]): GenerateIdentityValues = {
    new GenerateIdentityValues(PartitionIdentityValueGenerator(start, step, highWaterMarkOpt))
  }
}

/**
 * The reserve-more config a CIC service-backed [[PartitionIdentityValueGenerator]] uses to reserve
 * from the driver when its range is exhausted (via
 * [[org.apache.spark.sql.delta.cic.IdentitySequenceClient]]). The executor does not
 * choose the size: it reports `(lastCount, elapsedMs)` and the driver's controller sizes the grant.
 * The sizing knobs are read on the driver, so they are neither read here nor carried on requests.
 *
 * @param sequenceId the CIC service sequence this column reserves from.
 * @param tableId the service table scope; the service keys counters by `(tableId, sequenceId)`.
 */
case class CicReserveConfig(
    sequenceId: String,
    tableId: String)

/**
 * Generator of IDENTITY value for one partition.
 *
 * @param start The configured start value for the identity column.
 * @param highWaterMarkOpt The optional high watermark for the identity value generation. If this is
 *                      None, that means that no identity values has been generated in the past and
 *                      we should start the identity value generation from the `start`.
 * @param step IDENTITY value increment.
 * @param reservedEndOpt The inclusive upper bound (lower bound for a decreasing step) of this
 *                      generator's current reserved range. Set with `cicReserveConfig` on
 *                      the CIC service backend: [[next]] emits `value += step` from the contiguous
 *                      range and, when it would step past `reservedEndOpt`, reserves a fresh range
 *                      from the driver and continues. None (the metadata / legacy HWM path)
 *                      means no reserve-more: the generator stripes by `numPartitions * step`.
 * @param cicReserveConfig When set (CIC service backend), the config the generator uses
 *                to refill itself from the driver once its current range is exhausted. Always set
 *                together with `reservedEndOpt`; None preserves the legacy partition-striped
 *                generator.
 */
case class PartitionIdentityValueGenerator(
    start: Long,
    step: Long,
    highWaterMarkOpt: Option[Long],
    reservedEndOpt: Option[Long] = None,
    cicReserveConfig: Option[CicReserveConfig] = None) {

  require(step != 0)
  // The value generation logic requires high water mark to follow the start and step configuration.
  highWaterMarkOpt.foreach(highWaterMark => require((highWaterMark - start) % step == 0))

  private lazy val base = highWaterMarkOpt.map(Math.addExact(_, step)).getOrElse(start)
  private var partitionIndex: Int = -1
  private var nextValue: Long = -1L
  private var increment: Long = -1L

  // The active reserved upper bound. Starts at `reservedEndOpt` and is advanced to each reserved
  // range's end on the CIC service backend (see [[next]]).
  private var currentReservedEndOpt: Option[Long] = reservedEndOpt
  // Orders Long values along the step direction (ascending for step > 0, descending for step < 0),
  // so the reserved-range checks compare by direction without branching on the step sign.
  private lazy val stepOrdering: Ordering[Long] =
    if (step > 0L) Ordering.Long else Ordering.Long.reverse
  // Reserve sizing feedback: last granted size + when it landed, so the next reserve reports
  // (lastCount, elapsedMs). Both 0 until the first reserve completes -> first reports (0, 0).
  private var lastReserveGranted: Long = 0L
  private var lastReserveTimestampMs: Long = 0L
  // Clock for the reserve-gap timing. Transient + lazy because Clock is not Serializable and this
  // generator ships to executors, so it is rebuilt per JVM rather than carried.
  @transient protected lazy val clock: Clock = new SystemClock()

  def initialize(partitionIndex: Int): Unit = {
    if (this.partitionIndex < 0) {
      this.partitionIndex = partitionIndex
      this.currentReservedEndOpt = reservedEndOpt
      // CIC service-backed (reserve-more) generator: this task holds ONE contiguous reserved range
      // and emits `value += step`, reserving a fresh, service-guaranteed-disjoint range from the
      // driver whenever the range is exhausted. It starts EMPTY (no shared up-front range -- that
      // would make every partition emit the same values), so the first `next()` reserves this
      // task's private first range. No partition striping.
      if (cicReserveConfig.isDefined) {
        this.increment = step
        // currentReservedEndOpt starts None; the first next() sees the empty bound and reserves
        // this task's initial range before emitting.
        this.currentReservedEndOpt = None
      } else {
        this.nextValue = try {
          Math.addExact(base, Math.multiplyExact(partitionIndex, step))
        } catch {
          case e: ArithmeticException =>
            IdentityOverflowLogger.logOverflow()
            throw e
        }
        // Each value is incremented by numPartitions * step from the previous value.
        this.increment = try {
          // Total number of partitions. In local execution case where TaskContext is not set, the
          // task is executed as a single partition.
          val numPartitions = Option(TaskContext.get()).map(_.numPartitions()).getOrElse(1)
          Math.multiplyExact(numPartitions, step)
        } catch {
          case e: ArithmeticException =>
            IdentityOverflowLogger.logOverflow()
            throw e
        }
      }
    } else if (this.partitionIndex != partitionIndex) {
      throw SparkException.internalError("Same PartitionIdentityValueGenerator object " +
        s"initialized with two different partitionIndex [oldValue: ${this.partitionIndex}, " +
        s"newValue: $partitionIndex]")

    }
  }

  private def assertInitialized(): Unit = if (partitionIndex == -1) {
    throw SparkException.internalError("PartitionIdentityValueGenerator is not initialized.")
  }

  // Generate the next IDENTITY value.
  def next(): Long = {
    try {
      assertInitialized()
      // CIC service backend: reserve this task's first range on the first call, then emit from the
      // current contiguous range, reserving a fresh disjoint range whenever it is exhausted.
      cicReserveConfig.foreach { cfg =>
        // An empty bound means no range reserved yet (first call), so reserve the initial range;
        // afterwards reserve again only once the current range is exhausted.
        if (currentReservedEndOpt.isEmpty || wouldExceedReservedRange(nextValue)) {
          reserveNextRange(cfg)
        }
      }
      val ret = nextValue
      nextValue = Math.addExact(nextValue, increment)
      ret
    } catch {
      case e: ArithmeticException =>
        IdentityOverflowLogger.logOverflow()
        throw e
    }
  }
  /**
   * True when `value` falls outside the current reserved range, honoring the sign of `step`.
   * No active bound (the metadata / legacy HWM path) means no check.
   */
  private def wouldExceedReservedRange(value: Long): Boolean =
    currentReservedEndOpt.exists { reservedEnd =>
      stepOrdering.gt(value, reservedEnd)
    }

  /**
   * Reserve a fresh contiguous range from the driver and continue from it. The range is
   * service-guaranteed non-overlapping and private to this task. Blocks on the RPC; a failure
   * aborts the task, so no out-of-range value is committed (a retry may leak the prior range as a
   * permitted gap).
   */
  private def reserveNextRange(cfg: CicReserveConfig): Unit = {
    val taskContext = TaskContext.get()
    val stageId = if (taskContext != null) taskContext.stageId() else -1
    val taskAttemptId = if (taskContext != null) taskContext.taskAttemptId() else -1L
    // Report the previous reserve (lastCount, elapsedMs). Measuring the gap from range-in-hand
    // (stamped after the RPC below), not the last reserve's start, keeps this RPC's latency and any
    // park-wait out of the gap, so a slow backend never looks like a fast drain.
    val lastCount = lastReserveGranted
    val elapsedMs =
      if (lastReserveTimestampMs <= 0L) 0L else clock.getTimeMillis() - lastReserveTimestampMs
    val range = IdentitySequenceClient.requestIds(
      cfg.sequenceId, cfg.tableId, lastCount, elapsedMs, step,
      stageId, partitionIndex, taskAttemptId)
    // Defend against a backend returning a range with a drifted step: emitting
    // `rangeStart + i*step` from a range whose step disagrees with this column's step would
    // produce colliding values.
    if (range.step != step) {
      throw ConcurrentIdentityColumnErrors.metadataMismatch(range.step, cfg.sequenceId, step)
    }
    // A reply must contain at least one value: an empty range would make no progress and spin. The
    // generator already tolerates a shorter-than-requested range (the buffered/partial reserve in a
    // later PR may return one); this simple backend always returns exactly `count`.
    val empty =
      stepOrdering.lt(range.rangeEnd, range.rangeStart)
    if (empty) {
      throw ConcurrentIdentityColumnErrors.emptyReserveRange(cfg.sequenceId, cfg.tableId)
    }
    val granted = range.numValues
    lastReserveGranted = granted
    lastReserveTimestampMs = clock.getTimeMillis()
    currentReservedEndOpt = Some(range.rangeEnd)
    increment = range.step
    nextValue = range.rangeStart
  }
}

object IdentityOverflowLogger extends DeltaLogging {
  def logOverflow(): Unit = {
    recordEvent(
      MetricDefinitions.EVENT_TAHOE,
      Map(TAG_OP_TYPE -> "delta.identityColumn.overflow")
    )
  }
}
