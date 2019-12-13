package io.delta.hive

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.Writable

/**
 * @param index the index of a partition column in the schema.
 * @param tpe the Hive type of a partition column.
 * @param value the string value of a partition column. The actual partition value should be
 *              parsed according to its type.
 */
case class PartitionColumnInfo(
    var index: Int,
    var tpe: String,
    var value: String) extends Writable {

  def this() {
    this(0, null, null)
  }

  override def write(out: DataOutput): Unit = {
    out.writeInt(index)
    out.writeUTF(tpe)
    out.writeUTF(value)
  }

  override def readFields(in: DataInput): Unit = {
    index = in.readInt()
    tpe = in.readUTF()
    value = in.readUTF()
  }
}
