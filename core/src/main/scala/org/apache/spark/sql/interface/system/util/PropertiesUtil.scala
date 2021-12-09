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
package org.apache.spark.sql.interface.system.util

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

object PropertiesUtil {



  def getProperties(path: String): Properties = {
    val filePath: String = System.getProperty("user.dir")
    val prop = new Properties
    var inputStream: BufferedInputStream = null
    try {
      inputStream = new BufferedInputStream(new FileInputStream(filePath + path))
    } catch {
      case _: Exception => return null
    }
    prop.load(inputStream)
    prop
  }


  def getProperty(properties: Properties, path: String): String = {
    if (null == properties) {
      if ("file.split.number".equals(path)) {
        return "8"
      }
      if ("file.one.row.max.size".equals(path)) {
        return "134217728"
      }
      return null
    }
    properties.getProperty(path)
  }


  def chooseProperties(file: String, key: String): String = {
    val filePath: String = "/configuration/" + file + ".properties"
    val properties: Properties = getProperties(filePath)
    getProperty(properties, key)
  }
}
