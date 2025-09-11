package io.delta.spark.dsv2.utils;

import java.util.Objects;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

/** A utility class for converting between Java and Scala types. */
public class ScalaUtils {
  public static Map<String, String> toScalaMap(CaseInsensitiveStringMap javaMap) {
    return JavaConverters.mapAsScalaMapConverter(
            Objects.requireNonNull(javaMap, "options").asCaseSensitiveMap())
        .asScala()
        .toMap(scala.Predef$.MODULE$.conforms());
  }
}
