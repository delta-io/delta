/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.hive

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

class HiveInputFormat extends org.apache.hadoop.hive.ql.io.HiveInputFormat {

  override def pushProjectionsAndFilters(
      jobConf: JobConf,
      inputFormatClass: Class[_],
      splitPath: Path,
      nonNative: Boolean): Unit = {
    if (inputFormatClass == classOf[DeltaInputFormat]) {
      super.pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, false)
    } else {
      super.pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, nonNative)
    }
  }
}
