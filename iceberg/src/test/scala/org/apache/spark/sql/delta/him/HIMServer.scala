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

package org.apache.hadoop.hive.metastore

import java.net.InetSocketAddress

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol, TProtocolFactory}
import org.apache.thrift.server.{ServerContext, TServer, TServerEventHandler, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TTransport, TTransportFactory}

/**
 * Start a Thrift Server that accepts standard HMS thrift client.
 *
 * @param conf including database connection and warehouse location
 * @param port the port this thrift server listens
 */
class HIMServer(val conf: HiveConf, val port: Int) {

  private var tServer: TServer = _
  private var serverThread: MetastoreThread = _

  def start(): Unit = {
    val maxMessageSize = 100L * 1024 * 1024

    val protocolFactory: TProtocolFactory = new TBinaryProtocol.Factory
    val inputProtoFactory: TProtocolFactory = new TBinaryProtocol.Factory(
      true, true, maxMessageSize, maxMessageSize)
    val hmsHandler = new HMSHandler("default", conf)
    val handler = RetryingHMSHandler.getProxy(conf, hmsHandler, false)
    val transFactory = new TTransportFactory
    val processor = new TSetIpAddressProcessor(handler)
    val serverSocket = new TServerSocket(new InetSocketAddress(port))

    val args = new TThreadPoolServer.Args(serverSocket)
      .processor(processor)
      .transportFactory(transFactory)
      .protocolFactory(protocolFactory)
      .inputProtocolFactory(inputProtoFactory)
      .minWorkerThreads(5)
      .maxWorkerThreads(5);

    tServer = new TThreadPoolServer(args);

    val tServerEventHandler = new TServerEventHandler() {
      override def preServe(): Unit = {
      }

      override def createContext(tProtocol: TProtocol, tProtocol1: TProtocol): ServerContext = {
        null
      }

      override def deleteContext(
          serverContext: ServerContext, tProtocol: TProtocol, tProtocol1: TProtocol): Unit = {
        // If the IMetaStoreClient#close was called, HMSHandler#shutdown would have already
        // cleaned up thread local RawStore. Otherwise, do it now.
        HIMServer.cleanupRawStore()
      }

      override def processContext(
          serverContext: ServerContext, tTransport: TTransport, tTransport1: TTransport): Unit = {
      }
    }
    tServer.setServerEventHandler(tServerEventHandler)

    serverThread = new MetastoreThread
    serverThread.start()

    // Wait till the server is up
    while (!tServer.isServing) {
      Thread.sleep(100)
    }
  }

  def stop(): Unit = {
    HIMServer.cleanupRawStore()
    tServer.stop()
  }

  /**
   * The metastore thrift server will run in this thread
   */
  private class MetastoreThread extends Thread {
    super.setDaemon(true)
    super.setName("HIM Metastore Thread")

    override def run(): Unit = {
      tServer.serve()
    }
  }
}

object HIMServer {

  val localConfField = classOf[HMSHandler].getDeclaredField("threadLocalConf")
  localConfField.setAccessible(true)
  val localConf = localConfField.get().asInstanceOf[ThreadLocal[HiveConf]]

  private def cleanupRawStore(): Unit = {
    try {
      val rs = HMSHandler.getRawStore
      if (rs != null) {
        rs.shutdown()
      }
    } finally {
      HMSHandler.removeRawStore()
      localConf.remove()
    }
  }
}
