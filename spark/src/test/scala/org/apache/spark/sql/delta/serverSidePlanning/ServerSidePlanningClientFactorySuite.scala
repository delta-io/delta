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

package org.apache.spark.sql.delta.serverSidePlanning

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for ServerSidePlanningClientFactory auto-registration and lifecycle.
 * Tests the reflection-based auto-registration with double-checked locking,
 * manual factory registration, and factory lifecycle management.
 */
class ServerSidePlanningClientFactorySuite extends QueryTest with SharedSparkSession {

  override def afterEach(): Unit = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
    } finally {
      super.afterEach()
    }
  }

  // ========== Test Infrastructure ==========

  /**
   * Execute test block with clean factory state (setup + teardown).
   * Ensures factory is cleared before and after the test.
   */
  private def withCleanFactory[T](testFn: => T): T = {
    try {
      ServerSidePlanningClientFactory.clearFactory()
      testFn
    } finally {
      ServerSidePlanningClientFactory.clearFactory()
    }
  }

  /**
   * Assert that factory is registered and matches expected type.
   * Includes descriptive error messages.
   */
  private def assertFactoryType(
      expectedType: String,
      context: String = ""): Unit = {
    val prefix = if (context.nonEmpty) s"[$context] " else ""

    assert(ServerSidePlanningClientFactory.isFactoryRegistered(),
      s"${prefix}Factory should be registered")

    val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
    assert(factoryInfo.isDefined,
      s"${prefix}Factory info should be defined")
    assert(factoryInfo.get.contains(expectedType),
      s"${prefix}Expected factory type=$expectedType, got: ${factoryInfo.get}")
  }

  /**
   * Assert that factory is NOT registered.
   */
  private def assertNoFactory(context: String = ""): Unit = {
    val prefix = if (context.nonEmpty) s"[$context] " else ""

    assert(!ServerSidePlanningClientFactory.isFactoryRegistered(),
      s"${prefix}Factory should not be registered")
    assert(ServerSidePlanningClientFactory.getFactoryInfo().isEmpty,
      s"${prefix}Factory info should be empty")
  }

  /**
   * Assert that two factory instances are the same (reference equality).
   */
  private def assertSameInstance(
      factory1: ServerSidePlanningClientFactory,
      factory2: ServerSidePlanningClientFactory,
      context: String = ""): Unit = {
    val prefix = if (context.nonEmpty) s"[$context] " else ""
    assert(factory1 eq factory2,
      s"${prefix}Expected same factory instance")
  }

  // ========== Test Case Classes ==========

  /**
   * Test case for factory state verification scenarios.
   */
  private case class FactoryStateTestCase(
    description: String,
    setup: () => Unit,
    expectedRegistered: Boolean,
    expectedFactoryInfo: Option[String]
  )

  /**
   * Actions that can be performed on the factory.
   */
  private sealed trait FactoryAction
  private case object TriggerAutoRegistration extends FactoryAction
  private case object SetManualFactory extends FactoryAction
  private case object ClearFactory extends FactoryAction

  /**
   * Test case for factory registration scenarios.
   */
  private case class FactoryRegistrationTestCase(
    description: String,
    registrationSequence: Seq[FactoryAction],
    expectedFinalFactory: String,
    additionalVerification: Option[() => Unit] = None
  )

  /**
   * Test case for clearFactory behavior.
   */
  private case class ClearFactoryTestCase(
    description: String,
    initialSetup: () => Unit,
    verifyCleared: () => Unit
  )

  // ========== Helper Methods ==========

  /**
   * Execute a factory action.
   */
  private def executeFactoryAction(action: FactoryAction): Unit = action match {
    case TriggerAutoRegistration => ServerSidePlanningClientFactory.getFactory()
    case SetManualFactory =>
      ServerSidePlanningClientFactory.setFactory(new TestServerSidePlanningClientFactory())
    case ClearFactory => ServerSidePlanningClientFactory.clearFactory()
  }

  // ========== Test Data ==========

  private val stateTestCases = Seq(
    FactoryStateTestCase(
      "initially not registered",
      setup = () => { /* no-op */ },
      expectedRegistered = false,
      expectedFactoryInfo = None
    ),
    FactoryStateTestCase(
      "after manual registration",
      setup = () => ServerSidePlanningClientFactory.setFactory(
        new TestServerSidePlanningClientFactory()),
      expectedRegistered = true,
      expectedFactoryInfo = Some("TestServerSidePlanningClientFactory")
    ),
    FactoryStateTestCase(
      "after auto-registration",
      setup = () => ServerSidePlanningClientFactory.getFactory(),
      expectedRegistered = true,
      expectedFactoryInfo = Some("IcebergRESTCatalogPlanningClientFactory")
    ),
    FactoryStateTestCase(
      "after clearFactory",
      setup = () => {
        ServerSidePlanningClientFactory.getFactory()
        ServerSidePlanningClientFactory.clearFactory()
      },
      expectedRegistered = false,
      expectedFactoryInfo = None
    )
  )

  private val registrationTestCases = Seq(
    FactoryRegistrationTestCase(
      "auto-registration succeeds",
      registrationSequence = Seq(TriggerAutoRegistration),
      expectedFinalFactory = "IcebergRESTCatalogPlanningClientFactory"
    ),
    FactoryRegistrationTestCase(
      "manual factory overrides auto-registration",
      registrationSequence = Seq(SetManualFactory),
      expectedFinalFactory = "TestServerSidePlanningClientFactory",
      additionalVerification = Some(() => {
        // Verify auto-registration was NOT triggered
        val factory = ServerSidePlanningClientFactory.getFactory()
        assert(factory.isInstanceOf[TestServerSidePlanningClientFactory],
          "Should be test factory, not auto-registered")
      })
    ),
    FactoryRegistrationTestCase(
      "manual factory replaces auto-registered factory",
      registrationSequence = Seq(TriggerAutoRegistration, SetManualFactory),
      expectedFinalFactory = "TestServerSidePlanningClientFactory"
    )
  )

  private val clearFactoryTestCases = Seq(
    ClearFactoryTestCase(
      "clears auto-registered factory",
      initialSetup = () => ServerSidePlanningClientFactory.getFactory(),
      verifyCleared = () => {
        ServerSidePlanningClientFactory.clearFactory()
        assertNoFactory("after clearing auto-registered")

        // Verify can re-register
        val newFactory = ServerSidePlanningClientFactory.getFactory()
        assertFactoryType("IcebergRESTCatalogPlanningClientFactory", "after re-registration")
      }
    ),
    ClearFactoryTestCase(
      "clears manual factory",
      initialSetup = () => ServerSidePlanningClientFactory.setFactory(
        new TestServerSidePlanningClientFactory()),
      verifyCleared = () => {
        ServerSidePlanningClientFactory.clearFactory()
        assertNoFactory("after clearing manual factory")
      }
    ),
    ClearFactoryTestCase(
      "allows multiple registration cycles",
      initialSetup = () => {
        // First cycle
        ServerSidePlanningClientFactory.getFactory()
        ServerSidePlanningClientFactory.clearFactory()
        // Second cycle
        ServerSidePlanningClientFactory.getFactory()
        ServerSidePlanningClientFactory.clearFactory()
      },
      verifyCleared = () => {
        // Third cycle should work
        val factory = ServerSidePlanningClientFactory.getFactory()
        assertFactoryType("IcebergRESTCatalogPlanningClientFactory", "third cycle")
      }
    )
  )

  // ========== Tests ==========

  test("factory state transitions") {
    stateTestCases.foreach { testCase =>
      withCleanFactory {
        testCase.setup()

        assert(
          ServerSidePlanningClientFactory.isFactoryRegistered() == testCase.expectedRegistered,
          s"[${testCase.description}] Expected registered=${testCase.expectedRegistered}")

        val actualInfo = ServerSidePlanningClientFactory.getFactoryInfo()
        testCase.expectedFactoryInfo match {
          case Some(expected) =>
            assert(actualInfo.isDefined && actualInfo.get.contains(expected),
              s"[${testCase.description}] Expected factory info to contain '$expected', " +
              s"got: $actualInfo")
          case None =>
            assert(actualInfo.isEmpty,
              s"[${testCase.description}] Expected no factory info, got: $actualInfo")
        }
      }
    }
  }

  test("factory registration scenarios") {
    registrationTestCases.foreach { testCase =>
      withCleanFactory {
        // Execute registration sequence
        testCase.registrationSequence.foreach(executeFactoryAction)

        // Verify final state
        val factoryInfo = ServerSidePlanningClientFactory.getFactoryInfo()
        assert(factoryInfo.isDefined && factoryInfo.get.contains(testCase.expectedFinalFactory),
          s"[${testCase.description}] Expected factory=${testCase.expectedFinalFactory}, " +
          s"got: $factoryInfo")

        // Additional verification if specified
        testCase.additionalVerification.foreach(_.apply())
      }
    }
  }

  test("clearFactory resets complete state for multiple scenarios") {
    clearFactoryTestCases.foreach { testCase =>
      withCleanFactory {
        testCase.initialSetup()
        testCase.verifyCleared()
      }
    }
  }

  test("getFactory returns same instance across multiple calls") {
    withCleanFactory {
      val factory1 = ServerSidePlanningClientFactory.getFactory()
      val factory2 = ServerSidePlanningClientFactory.getFactory()
      val factory3 = ServerSidePlanningClientFactory.getFactory()

      assertSameInstance(factory1, factory2, "second call")
      assertSameInstance(factory2, factory3, "third call")
      assertFactoryType("IcebergRESTCatalogPlanningClientFactory", "cached instance")
    }
  }


  test("verify test isolation - factory state doesn't leak between tests") {
    withCleanFactory {
      // This test verifies that afterEach() cleanup works correctly
      // and tests start with clean state

      // Verify starting with clean state
      assertNoFactory("test should start with clean state")

      // Set a factory
      val testFactory = new TestServerSidePlanningClientFactory()
      ServerSidePlanningClientFactory.setFactory(testFactory)
      assertFactoryType("TestServerSidePlanningClientFactory", "within test")

      // afterEach() will clean this up automatically
      // The next test should start fresh
    }
  }
}
