//package io.delta.sparkutil
//
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//import java.io.{ObjectInputStream, ObjectOutputStream}
//import java.io.IOException
//import scala.util.control.NonFatal
//
//import org.apache.hadoop.conf.Configuration
//
///**
//  * Hadoop configuration but serializable. Use `value` to access the Hadoop configuration.
//  *
//  * @param value Hadoop configuration
//  */
//class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
//
//  def tryOrIOException[T](block: => T): T = {
//    try {
//      block
//    } catch {
//      case e: IOException =>
//        throw e
//      case NonFatal(e) =>
//        throw new IOException(e)
//    }
//  }
//
//  private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
//    out.defaultWriteObject()
//    value.write(out)
//  }
//
//  private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
//    value = new Configuration(false)
//    value.readFields(in)
//  }
//}
