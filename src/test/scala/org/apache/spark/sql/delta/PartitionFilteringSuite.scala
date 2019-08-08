/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.test.SharedSparkSession

class PartitionFilteringSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val A_P1 = "part=1/a"
  val B_P1 = "part=1/b"
  val C_P2 = "part=2/c"
  val E_P3 = "part=3/e"
  val G_P4 = "part=4/g"
  private val addA_P1 = AddFile(A_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addB_P1 = AddFile(B_P1, Map("part" -> "1"), 1, 1, dataChange = true)
  private val addC_P2 = AddFile(C_P2, Map("part" -> "2"), 1, 1, dataChange = true)
  private val addE_P3 = AddFile(E_P3, Map("part" -> "3"), 1, 1, dataChange = true)
  private val addG_P4 = AddFile(G_P4, Map("part" -> "4"), 1, 1, dataChange = true)

  private val rmA_P1 = addA_P1.remove
  private val rmB_P1 = addB_P1.remove
  private val rmC_P2 = addC_P2.remove
  private val rmE_P3 = addE_P3.remove
  private val rmG_P4 = addG_P4.remove

  val addActions = addA_P1 :: addB_P1 :: addC_P2 :: addE_P3 :: addG_P4 :: Nil
  val removeActions = rmA_P1 :: rmB_P1 :: rmC_P2 :: rmE_P3 :: rmG_P4 :: Nil

  test("Filter AddFiles gt lt operators") {
    withSnapshot { snapshot =>
      val filtered = snapshot.filterFileActions(addActions,
      ('part > 1 and 'part < 4).expr :: Nil)
      assert(filtered == addC_P2 :: addE_P3 :: Nil)
    }
  }

  test("Filter AddFiles eq operator") {
    withSnapshot { snapshot =>
      val filtered = snapshot.filterFileActions(addActions,
        ('part === 1 or 'part === 10).expr :: Nil)
      assert(filtered == addA_P1 :: addB_P1 :: Nil)
    }
  }

  test("Filter RemoveFiles gt lt operators") {
    withSnapshot { snapshot =>
      val filtered = snapshot.filterFileActions(removeActions,
        ('part > 1 and 'part < 4).expr :: Nil)
      assert(filtered == rmC_P2 :: rmE_P3 :: Nil)
    }
  }

  test("Filter RemoveFiles eq operators") {
    withSnapshot { snapshot =>
      val filtered = snapshot.filterFileActions(removeActions,
        ('part === 1 or 'part === 10).expr :: Nil)
      assert(filtered == rmA_P1 :: rmB_P1 :: Nil)
    }
  }

  test("Filter AddFiles and RemoveFiles") {
    withSnapshot { snapshot =>
      val addX_P100 = AddFile("part=100/x", Map("part" -> "100"), 1, 1, dataChange = true)
      val filtered = snapshot.filterFileActions(removeActions ++ addActions :+ addX_P100,
        ('part < 2 or 'part >= 4).expr :: Nil)
      assert(filtered.toSet ==
        // 'part < 2
        (addA_P1 :: addB_P1 :: rmA_P1 :: rmB_P1 ::
        // 'part >= 4
        addG_P4 :: rmG_P4 :: addX_P100 :: Nil).toSet)
    }
  }

  test("Filter AddFiles and RemoveFiles 'in' operator") {
    withSnapshot { snapshot =>
      val filtered = snapshot.filterFileActions(removeActions ++ addActions,
        ('part isin(2, 4)).expr :: Nil)
      assert(filtered.toSet ==
        (addC_P2 :: rmC_P2 :: addG_P4 :: rmG_P4:: Nil).toSet)
    }
  }

  def withSnapshot(test: Snapshot => Unit): Unit = {
    withTempDir { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      test(log.snapshot)
    }
  }
}
