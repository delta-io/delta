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
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Nondeterministic}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.types.{DataType, LongType}

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
 * Generator of IDENTITY value for one partition.
 *
 * @param start The configured start value for the identity column.
 * @param highWaterMarkOpt The optional high watermark for the identity value generation. If this is
 *                      None, that means that no identity values has been generated in the past and
 *                      we should start the identity value generation from the `start`.
 * @param step IDENTITY value increment.
 */
case class PartitionIdentityValueGenerator(
    start: Long,
    step: Long,
    highWaterMarkOpt: Option[Long]) {

  require(step != 0)
  // The value generation logic requires high water mark to follow the start and step configuration.
  highWaterMarkOpt.foreach(highWaterMark => require((highWaterMark - start) % step == 0))

  private lazy val base = highWaterMarkOpt.map(Math.addExact(_, step)).getOrElse(start)
  private var partitionIndex: Int = -1
  private var nextValue: Long = -1L
  private var increment: Long = -1L


  def initialize(partitionIndex: Int): Unit = {
    if (this.partitionIndex < 0) {
      this.partitionIndex = partitionIndex
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
      val ret = nextValue
      nextValue = Math.addExact(nextValue, increment)
      ret
    } catch {
      case e: ArithmeticException =>
        IdentityOverflowLogger.logOverflow()
        throw e
    }
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
