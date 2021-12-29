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

package io.delta.storage.cephobjectstore

import org.apache.hadoop.fs.Path
import org.javaswift.joss.model.StoredObject

import java.io.IOException
import java.net.URI

/**
 * Utility methods for Ceph RGW code.
 */
object CephStoreUtil {

  /**
   * Check the completeness of the path
   *
   * @throws IOException IO problems
   */
  def checkRootPath(uri: URI): Unit = {
    val objectUri = uri
    if (objectUri == null) {
      throw new IOException("Path cannot be null, ceph://<your-container-name>/<your-object-path>")
    }
    if (!objectUri.getScheme.equals("ceph")) {
      throw new IOException("Scheme is wrong, ceph://<your-container-name>/<your-object-path>")
    }
    if (objectUri.getHost == null) throw new IOException("Container name cannot be null")
    val objectPath = new Path(objectUri)
    if (objectPath.toString().endsWith("_delta_log")) {
      val parentPath = objectPath.getParent
      if (getObjectPath(parentPath).equals("")) {
        throw new IOException("Object name cannot be null")
      }
    }
    else if (getObjectPath(objectPath).equals("")) {
      throw new IOException("Object name cannot be null")
    }
  }

  def getObjectPath(path: Path): String = {
    val objectUri = path.toUri
    val objectPath = objectUri.getPath.substring(1)
    objectPath
  }

  def getCompletePath(path: Path, storeObject: StoredObject): String = {
    val host = path.toUri.getHost
    val completePath = "ceph://" + host + "/" + storeObject.getName
    completePath
  }

  def getSuffixes(string: String): String = {
    val str = string + "/"
    str
  }


}
