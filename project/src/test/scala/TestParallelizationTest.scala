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

import sbt._

object TestParallelizationTest {
  import TestParallelization.MinShardGroupDurationStrategy._

  def main(args: Array[String]): Unit = {
    val names = Seq("slow", "medium", "unknown-a", "unknown-b", "tie-a", "tie-b")
    val durations = Map("slow" -> 30.0, "medium" -> 10.0, "tie-a" -> 2.0, "tie-b" -> 2.0)

    for ((shards, groups) <- Seq((1, 1), (1, 4), (8, 1), (8, 4))) {
      val (assignment, loads) = optimalAssignment(names, shards, groups, durations)
      val assignedNames = assignment.flatMap(_.flatMap(_.toSeq)).toSeq
      assert(assignedNames.sorted == names.sorted, "Every discovered suite must appear once")
      val expectedDuration = durations.values.sum + 2 * DEFAULT_TEST_SUITE_DURATION
      assert(math.abs(loads.map(_.sum).sum - expectedDuration) < 0.000001)
      for (shard <- 0 until shards; group <- 0 until groups) {
        val expectedLoad = assignment(shard)(group).toSeq.map { name =>
          durations.getOrElse(name, DEFAULT_TEST_SUITE_DURATION)
        }.sum
        assert(math.abs(loads(shard)(group) - expectedLoad) < 0.000001)
      }

      def plan(suites: Seq[String], estimates: Map[String, Double]) = {
        optimalAssignment(suites, shards, groups, estimates)._1.map(_.toVector).toVector
      }
      assert(
        plan(names, durations) == plan(names.reverse, durations.toSeq.reverse.toMap),
        "Discovery and timing table order must not change the assignment")
      assert(
        plan(names, durations) == plan(names, durations + ("another-project" -> 1000.0)),
        "Stale suites and other projects must not reserve any load")
      val (emptyAssignment, emptyLoads) = optimalAssignment(Nil, shards, groups, durations)
      assert(emptyAssignment.forall(_.forall(_.isEmpty)))
      assert(emptyLoads.forall(_.forall(_ == 0)))
    }

    val unknownNames = (0 until 32).map(i => s"unknown-$i")
    val (unknownAssignment, _) = optimalAssignment(unknownNames, 8, 4, Map.empty)
    assert(
      unknownAssignment.forall(_.forall(_.size == 1)),
      "Unknown suites must participate in balancing")

    for ((shards, groups) <- Seq((0, 1), (-1, 1), (1, 0), (1, -1))) {
      try {
        optimalAssignment(names, shards, groups, durations)
        sys.error("Invalid shard and group counts must be rejected")
      } catch {
        case _: IllegalArgumentException =>
      }
    }

    IO.withTemporaryDirectory { directory =>
      val fingerprint = new sbt.testing.SubclassFingerprint {
        override def superclassName(): String = "org.scalatest.Suite"
        override def isModule(): Boolean = false
        override def requireNoArgConstructor(): Boolean = true
      }
      val definitions = names.map { name =>
        new TestDefinition(
          name,
          fingerprint,
          false,
          Array[sbt.testing.Selector](new sbt.testing.SuiteSelector))
      }
      val forkOptions = ForkOptions().withRunJVMOptions(Vector("-Dtest.option=retained"))
      val originalGroups = Seq(
        Tests.Group("serial", definitions.take(3), Tests.SubProcess(forkOptions)),
        Tests.Group("other", definitions.drop(3), Tests.InProcess))
      for (shards <- Seq(1, 8)) {
        val assignedGroups = (0 until shards).flatMap { shard =>
          val groups = TestParallelization.shardGroups(originalGroups, shards, shard)
          groups.zip(originalGroups).foreach { case (group, original) =>
            assert(
              group.name == original.name && group.runPolicy == original.runPolicy,
              "Sharding must preserve the existing JVM groups and run policies")
            assert(
              group.tests == original.tests.filter(group.tests.contains),
              "Sharding must preserve test order within each group")
          }
          groups
        }
        val assignedTests = assignedGroups.flatMap(_.tests)
        assert(assignedTests.map(_.name).sorted == names.sorted)
        assert(assignedTests.forall(test => definitions.exists(_ eq test)))
      }
      for (shard <- Seq(-1, 8)) {
        try {
          TestParallelization.shardGroups(originalGroups, 8, shard)
          sys.error("An invalid shard ID must be rejected")
        } catch {
          case _: IllegalArgumentException =>
        }
      }
      val assigned = (0 until NUM_SHARDS).flatMap { shard =>
        val strategy = TestParallelization.MinShardGroupDurationStrategy(
          4,
          directory,
          shard,
          forkOptions,
          definitions)
        definitions.reverse.foreach(strategy.add)
        val testGroups = strategy.testGroups
        assert(testGroups.map(_.name) == (0 until 4).map(i => s"Test group $i"))
        testGroups.zipWithIndex.foreach { case (group, index) =>
          val options = forkOptions.withRunJVMOptions(forkOptions.runJVMOptions :+
            s"-Djava.io.tmpdir=$directory/target/tmp/$index")
          assert(group.runPolicy == Tests.SubProcess(options), "Keep the JVM options")
        }
        testGroups.flatMap(_.tests)
      }
      assert(assigned.map(_.name).sorted == names.sorted)
      assigned.foreach { test =>
        assert(definitions.exists(_ eq test), "Keep the original TestDefinition objects")
      }
      for (shard <- Seq(-1, NUM_SHARDS)) {
        try {
          TestParallelization.MinShardGroupDurationStrategy(
            4,
            directory,
            shard,
            forkOptions,
            definitions)
          sys.error("An invalid shard ID must be rejected")
        } catch {
          case _: IllegalArgumentException =>
        }
      }
    }
    println("TestParallelization: all checks passed")
  }
}
