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
package example

import java.io.File

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.{DataFrame, SparkSession}

/*
To run this example you must follow these steps:
Requirements:
- Using Java 17
- Spark 4.0.0+
- delta-connect-server 4.0.0rc1+

(1) Download the latest Spark 4 release and start a local Spark connect server using this command:
sbin/start-connect-server.sh \
  --packages io.delta:delta-connect-server_2.13:{DELTA_VERSION},com.google.protobuf:protobuf-java:3.25.1 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes"="org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes"="org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
* Be sure to replace DELTA_VERSION with the version you are using

(2) Set the SPARK_REMOTE environment variable to point to your local Spark server
export SPARK_REMOTE="sc://localhost:15002"

(3) export _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"

(4) Run this file i.e. ./build/sbt connect/run

(5) Once finished QA-ing, stop the Spark Connect server.
sbin/stop-connect-server.sh
*/

object DeltaConnect {
  private val filePath = "/tmp/deltaConnect"

  private val tableName = "deltaConnectTable"

  private def cleanup(deltaSpark: SparkSession): Unit = {
    deltaSpark.sql(s"DROP TABLE IF EXISTS $tableName")
    deltaSpark.sql(s"DROP TABLE IF EXISTS delta.`${filePath}`")
    FileUtils.deleteDirectory(new File(filePath))
  }

  def main(args: Array[String]): Unit = {
    val deltaSpark = SparkSession
      .builder()
      .connect()
      .remote("sc://localhost:15002")
      .create()

    // Clear up old session
    cleanup(deltaSpark)

    // Using forPath
    try {
      DeltaTable.forPath(deltaSpark, filePath).toDF.show()
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains("DELTA_MISSING_DELTA_TABLE"))
    }

    // Using forName
    try {
      DeltaTable.forName(deltaSpark, tableName).toDF.show()
    } catch {
      case ex: Exception =>
        assert(ex.getMessage.contains("DELTA_MISSING_DELTA_TABLE"))
    }

    try {
      println("########### Write basic table and check that results match ##############")
      // By table name
      deltaSpark.range(5).write.format("delta").saveAsTable(tableName)
      DeltaTable.forName(deltaSpark, tableName).toDF.show()

      // By table path
      deltaSpark.range(6).write.format("delta").save(filePath)
      DeltaTable.forPath(deltaSpark, filePath).toDF.show()

      val deltaTable = DeltaTable.forPath(deltaSpark, filePath)
      println("########### Update to the table(add 100 to every even value) ##############")
      deltaTable.updateExpr(
        condition = "id % 2 == 0",
        set = Map("id" -> "id + 100")
      )

      deltaTable.toDF.show() // 100, 1, 102, 3, 104, 5

      println("######### Delete every even value ##############")
      deltaTable.delete(condition = "id % 2 == 0")

      deltaTable.toDF.show() // 1, 3, 5

      println("######## Read old data using time travel ############")
      val oldVersionDF = deltaSpark.read.format("delta").option("versionAsOf", 0).load(filePath)
      oldVersionDF.show() // 0, 1, 2, 3, 4, 5
    } finally {
      cleanup(deltaSpark)
      deltaSpark.stop()
    }
  }
}
