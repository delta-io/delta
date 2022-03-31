# delta scala examples

This directory contains a set of spark & delta examples.

Execute `./build/sbt run` and choose which main class to run.

```
Multiple main classes detected. Select one to run:
 [1] example.Quickstart
 [2] example.QuickstartSQL
 [3] example.QuickstartSQLOnPaths
 [4] example.Streaming
 [5] example.Utilities
```

You can specify delta lake version and scala version with environment variables `DELTA_VERSION`, `SCALA_VERSION` or editing `build.sbt`.

If you are faced with `java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x******) cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module` when you use Java 9 or later, add jvm option in `build.sbt`.

```diff
lazy val root = (project in file("."))
  .settings(
    run / fork := true,
+   run / javaOptions ++= Seq(
+     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
+   ),
```
