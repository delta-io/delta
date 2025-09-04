package io.delta.spark.dsv2.utils;

import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;

public interface SerializableReaderFunction
    extends Function1<PartitionedFile, Iterator<InternalRow>>, Serializable {}
