package io.delta.hive

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

class HiveInputFormat extends org.apache.hadoop.hive.ql.io.HiveInputFormat {

  override def pushProjectionsAndFilters(jobConf: JobConf, inputFormatClass: Class[_], splitPath: Path, nonNative: Boolean) = {
    if (inputFormatClass == classOf[DeltaInputFormat]) {
      super.pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, false)
    } else {
      super.pushProjectionsAndFilters(jobConf, inputFormatClass, splitPath, nonNative)
    }
  }
}
