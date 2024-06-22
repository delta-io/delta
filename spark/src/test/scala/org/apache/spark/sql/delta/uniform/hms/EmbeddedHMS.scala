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

package org.apache.spark.sql.delta.uniform.hms

import java.io.{BufferedReader, File, InputStreamReader, IOException}
import java.net.ServerSocket
import java.nio.file.Files
import java.sql.{Connection, DriverManager}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars


/**
 * EmbeddedHMS is an embedded Hive MetaStore for testing purposes.
 * Multiple EmbeddedHMS instances can be started in parallel on the same host
 * (see [[HMSTest]] for how to use it in the code).
 */
class EmbeddedHMS {
  private var server: HMSServer = _
  private var whFolder: String = _
  private var dbName: String = _
  private var started = false
  private var port: Int = 0

  /**
   * Start an EmbeddedHMS instance
   */
  def start(): Unit = {
    if (started) return
    port = EmbeddedHMS.firstAvailablePort()
    val dbFolder = Files.createTempDirectory("ehms_metastore")
    Files.delete(dbFolder) // Derby needs the folder to be non-existent
    dbName = dbFolder.toString
    whFolder = Files.createTempDirectory("ehms_warehouse").toString

    initDatabase(dbName)

    val innerConf = new HiveConf()
    innerConf.set(ConfVars.HIVE_IN_TEST.varname, "false")
    innerConf.set(ConfVars.METASTOREWAREHOUSE.varname, whFolder)
    innerConf.set(ConfVars.METASTORECONNECTURLKEY.varname, s"jdbc:derby:$dbName;create=true")
    server = new HMSServer(innerConf, port)
    server.start()

    started = true
  }

  /**
   * Stop the instance and cleanup its resources
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
   * Load SQL scripts into Apache Derby instance to initialize the metastore
   * schema. The script used here is copied from HMS official repo.
   * @param dbFolder the folder to create the database, also the database name
   */
  private def initDatabase(dbFolder: String): Unit = {
    // scalastyle:off classforname
    // Register the Derby JDBC Driver
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getConstructor().newInstance()
    // scalastyle:on classforname
    val con = DriverManager.getConnection(s"jdbc:derby:$dbFolder;create=true")
    // May need to use another version when upgrading Hive dependencies
    executeScript(con, "hms/hive-schema-3.1.0.derby.sql")
    con.close()
    // Shutdown the Derby instance properly, allowing it to clean up.
    try {
      DriverManager.getConnection(s"jdbc:derby:$dbFolder;shutdown=true")
    } catch {
      // From Derby doc:
      // "A successful shutdown always results in an SQLException to indicate
      // that Derby has shut down and that there is no other exception."
      // We thus ignore the exception here.
      case _: java.sql.SQLException =>
    }
  }

  /**
   * Execute sql scripts in the given resource file
   * @param con        database connection
   * @param scriptFile the name of the resource location of the sql script
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

object EmbeddedHMS {
  var start = 9083

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
