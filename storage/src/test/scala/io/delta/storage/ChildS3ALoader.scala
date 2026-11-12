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

package io.delta.storage

import java.net.URLClassLoader

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.s3a.S3AFileSystem
import software.amazon.awssdk.services.s3.S3Client

/** Loads an independent S3A and SDK while sharing Hadoop's FileSystem API. */
class ChildS3ALoader(parent: ClassLoader)
  extends URLClassLoader(Array(
    classOf[S3AFileSystem].getProtectionDomain.getCodeSource.getLocation,
    classOf[S3Client].getProtectionDomain.getCodeSource.getLocation,
    classOf[FileSystem].getProtectionDomain.getCodeSource.getLocation,
    classOf[ChildLoadedS3AFileSystem].getProtectionDomain.getCodeSource.getLocation), parent) {

  override protected def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
    // Keep each loader's S3A classes and their statistics registry together.
    if (name.startsWith("org.apache.hadoop.fs.s3a.") ||
        name.startsWith("software.amazon.awssdk.") ||
        name.startsWith("org.apache.hadoop.fs.GlobalStorageStatistics") ||
        name == classOf[ChildLoadedS3AFileSystem].getName) {
      val clazz: Class[_] = Option(findLoadedClass(name)).getOrElse(findClass(name))
      if (resolve) resolveClass(clazz)
      clazz
    } else {
      super.loadClass(name, resolve)
    }
  }
}

class ChildLoadedS3AFileSystem extends S3AFileSystem
