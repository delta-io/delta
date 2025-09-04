package io.delta.spark.dsv2.utils;

public class ScalaUtils {

  public static <K, V> scala.collection.immutable.Map<K, V> toScalaMap(
      java.util.Map<K, V> javaMap) {
    scala.collection.immutable.Map<K, V> result = new scala.collection.immutable.HashMap<>();
    for (java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
      result = result.updated(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
