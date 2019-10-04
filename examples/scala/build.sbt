name := "example"
organization := "com.example"
organizationName := "example"
scalaVersion := "2.11.12"
version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "hello-world",
    libraryDependencies += "io.delta" %% "delta-core" % "0.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3",
    resolvers += "Delta" at "https://dl.bintray.com/delta-io/delta/")
