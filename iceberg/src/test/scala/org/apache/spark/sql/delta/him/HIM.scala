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

package org.apache.spark.sql.delta.him

import java.io.{BufferedReader, File, InputStreamReader, IOException}
import java.net.ServerSocket
import java.nio.file.Files
import java.sql.{Connection, DriverManager}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.HIMServer

/**
 * HIM (Hms In Memory) is an embedded Hive MetaStore for testing purpose.
 * Multiple HIM instances can be started in parallel on the same host.
 * see [[HIMExamples]] for how to use HIM in the code.
 */
class HIM {
  private var server: HIMServer = _
  private var whFolder: String = _
  private var dbName: String = _
  private var started = false
  private var port: Int = 0

  /**
   * Start a HIM instance
   */
  def start(): Unit = {
    if (started) return
    port = HIM.firstAvailablePort()
    val dbFolder = Files.createTempDirectory("him_metastore")
    Files.delete(dbFolder) // Derby needs an empty folder
    dbName = dbFolder.toString
    whFolder = Files.createTempDirectory("him_warehouse").toString

    initDatabase(dbName)

    val innerConf = new HiveConf()
    innerConf.set(ConfVars.HIVE_IN_TEST.varname, "false")
    innerConf.set(ConfVars.METASTOREWAREHOUSE.varname, whFolder)
    innerConf.set(ConfVars.METASTORECONNECTURLKEY.varname, s"jdbc:derby:$dbName;create=true")
    server = new HIMServer(innerConf, port)
    server.start()

    started = true
  }

  /**
   * Stop a HIM instance and cleanup its resources
   */
  def stop(): Unit = {
    if (!started) return
    server.stop()
    // Cleanup on exit
    FileUtils.deleteDirectory(new File(dbName))
    FileUtils.deleteDirectory(new File(whFolder))
    started = false
  }

  /**
   * Fetch the configuration used for clients to connect to the MetaStore
   * @return conf containing thrift uri and warehouse location
   */
  def conf(): Configuration = {
    if (!started) throw new IllegalStateException("Not started")
    val conf = new Configuration()
    conf.set(ConfVars.METASTOREWAREHOUSE.varname, whFolder)
    conf.set(ConfVars.METASTOREURIS.varname, s"thrift://localhost:$port")
    conf
  }

  /**
   * Initialize HMS schema in the Apache Derby instance.
   * @param dbFolder the folder to create the database, also the database name
   */
  private def initDatabase(dbFolder: String): Unit = {
    // scalastyle:off classforname
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getConstructor().newInstance()
    // scalastyle:on classforname
    val con = DriverManager.getConnection(s"jdbc:derby:$dbFolder;create=true")
    // May need to use another version when upgrading Hive dependencies
    executeScript(con, "hive-schema-3.1.0.derby.sql")
    con.close()
    try {
      DriverManager.getConnection(s"jdbc:derby:$dbFolder;shutdown=true")
    } catch {
      case _: Throwable => // Derby always throws error on closing, ignore
    }
  }

  /**
   * Execute sql scripts in the given file
   * @param con        database connection
   * @param scriptFile sql scripts
   */
  private def executeScript(con: Connection, scriptFile: String): Unit = {
    val scriptIs = Thread.currentThread().getContextClassLoader.getResourceAsStream(scriptFile)
    if (scriptIs == null) {
      throw new RuntimeException("Make sure derby init script is in the classpath")
    }
    val reader = new BufferedReader(new InputStreamReader(scriptIs))
    var line: String = reader.readLine
    val buffer: StringBuilder = new StringBuilder()
    val stmt = con.createStatement()
    while (line != null) {
      line match {
        case comment if comment.startsWith("--") =>
        case eos if eos.endsWith(";") =>
          if (buffer.nonEmpty) buffer.append("\n")
          buffer.append(eos)
          buffer.deleteCharAt(buffer.length - 1) // Remove semicolon
          stmt.addBatch(buffer.toString)
          buffer.clear
        case piece =>
          if (buffer.nonEmpty) buffer.append("\n")
          buffer.append(piece)
      }
      line = reader.readLine()
    }
    reader.close()
    stmt.executeBatch()
    stmt.close()
  }
}

object HIM {
  var start = 1024

  def firstAvailablePort(): Integer = this.synchronized {
    for (port <- start until 65536) {
      var ss: ServerSocket = null
      try {
        ss = new ServerSocket(port)
        ss.setReuseAddress(true)
        start = port + 1
        return port
      } catch {
        case e: IOException =>
      } finally {
        if (ss != null) {
          try ss.close()
          catch {
            case e: IOException =>
          }
        }
      }
    }
    throw new RuntimeException("No port is available")
  }
}
