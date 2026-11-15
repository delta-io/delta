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
    val names = Set("slow", "medium", "unknown-a", "unknown-b")
    val durations = Seq("slow" -> 30.0, "medium" -> 10.0)
    for (groupCount <- Seq(1, 4)) {
      val (assignment, loads) = highDurationOptimalAssignment(groupCount, names, durations)
      assert(assignment.flatMap(_.flatMap(_.toSeq)).sorted.toSeq == Seq("medium", "slow"))
      assert(math.abs(loads.map(_.sum).sum - 40.0) < 0.000001)
      val (withStale, staleLoads) = highDurationOptimalAssignment(
        groupCount, names, ("another-project" -> 1000.0) +: durations)
      assert(withStale.map(_.toVector).toVector == assignment.map(_.toVector).toVector,
        "Suites outside the current project must not reserve any load")
      assert(staleLoads.map(_.toVector).toVector == loads.map(_.toVector).toVector)
      val (emptyAssignment, emptyLoads) = highDurationOptimalAssignment(
        groupCount, Set.empty, durations)
      assert(emptyAssignment.forall(_.forall(_.isEmpty)))
      assert(emptyLoads.forall(_.forall(_ == 0)))
    }

    val fingerprint = new sbt.testing.SubclassFingerprint {
      override def superclassName(): String = "org.scalatest.Suite"
      override def isModule(): Boolean = false
      override def requireNoArgConstructor(): Boolean = true
    }
    val discoveredNames = TOP_N_HIGH_DURATION_TEST_SUITES.take(3).map(_._1) ++ names.toSeq.sorted
    val definitions = discoveredNames.map { name =>
      new TestDefinition(name, fingerprint, false,
        Array[sbt.testing.Selector](new sbt.testing.SuiteSelector))
    }
    val forkOptions = ForkOptions().withRunJVMOptions(Vector("-Dtest.option=retained"))
    val originalGroups = Seq(
      Tests.Group("serial", definitions.take(3), Tests.SubProcess(forkOptions)),
      Tests.Group("other", definitions.drop(3), Tests.InProcess))
    val collidingTests = Iterator.from(0).map(i => s"timed-$i").filter { name =>
      math.abs(scala.util.hashing.MurmurHash3.stringHash(name) % 8) == 0
    }.take(2).map { name =>
      new TestDefinition(name, fingerprint, false,
        Array[sbt.testing.Selector](new sbt.testing.SuiteSelector))
    }.toVector
    val timedGroups = Seq(Tests.Group("timed", collidingTests, Tests.SubProcess(forkOptions)))
    val timings = collidingTests.map(test => test.name -> 10.0)
    val balanced = (0 until 8).map { shard =>
      TestParallelization.shardGroups(timedGroups, 8, shard, timings).flatMap(_.tests)
    }
    assert(balanced.forall(_.size <= 1), "Timed suites must not stay on the same hash shard")
    assert(balanced.flatten.map(_.name).sorted == collidingTests.map(_.name).sorted)
    for (shards <- Seq(1, 8)) {
      val assignedGroups = (0 until shards).flatMap { shard =>
        val groups = TestParallelization.shardGroups(originalGroups, shards, shard)
        groups.zip(originalGroups).foreach { case (group, original) =>
          assert(group.name == original.name && group.runPolicy == original.runPolicy,
            "Sharding must preserve group names and run policies")
          assert(group.tests == original.tests.filter(group.tests.contains),
            "Sharding must preserve the test order in each group")
        }
        groups
      }
      val assignedTests = assignedGroups.flatMap(_.tests)
      assert(assignedTests.map(_.name).sorted == discoveredNames.sorted,
        "Every discovered suite must run once across all shards")
      assert(assignedTests.forall(test => definitions.exists(_ eq test)))
    }
    for ((shards, shard) <- Seq((0, 0), (8, -1), (8, 8))) {
      try {
        TestParallelization.shardGroups(originalGroups, shards, shard)
        sys.error("Invalid shard counts and IDs must be rejected")
      } catch {
        case _: IllegalArgumentException =>
      }
    }

    IO.withTemporaryDirectory { directory =>
      val assigned = (0 until NUM_SHARDS).flatMap { shard =>
        val strategy = TestParallelization.MinShardGroupDurationStrategy(
          4, directory, shard, forkOptions, discoveredNames.toSet)
        definitions.foreach(strategy.add)
        val groups = strategy.testGroups
        groups.foreach { group =>
          val index = group.name.stripPrefix("Test group ").toInt
          val options = forkOptions.withRunJVMOptions(forkOptions.runJVMOptions :+
            s"-Djava.io.tmpdir=$directory/target/tmp/$index")
          assert(group.runPolicy == Tests.SubProcess(options), "Keep the JVM options")
        }
        groups.flatMap(_.tests)
      }
      assert(assigned.map(_.name).sorted == discoveredNames.sorted)
      assert(assigned.forall(test => definitions.exists(_ eq test)),
        "Keep the original test definitions")
    }
    println("TestParallelization: all checks passed")
  }
}
