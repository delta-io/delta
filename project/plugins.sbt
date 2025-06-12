/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.0"

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.0")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.3")

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.15")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.11")
//Upgrade sbt-scoverage to 2.0.3+ because 2.0.0 is not compatible to Scala 2.12.17:
//sbt.librarymanagement.ResolveException: Error downloading org.scoverage:scalac-scoverage-plugin_2.12.17:2.0.0

//It caused a conflict issue:
//[error] java.lang.RuntimeException: found version conflict(s) in library dependencies; some are suspected to be binary incompatible:
//[error] 
//[error] 	* org.scala-lang.modules:scala-xml_2.12:2.1.0 (early-semver) is selected over 1.0.6
//[error] 	    +- org.scoverage:scalac-scoverage-reporter_2.12:2.0.7 (depends on 2.1.0)
//[error] 	    +- org.scalariform:scalariform_2.12:0.2.0             (depends on 1.0.6)
//The following fix the conflict:
libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always % "test"

addSbtPlugin("net.aichler" % "sbt-jupiter-interface" % "0.9.1")

addSbtPlugin("software.purpledragon" % "sbt-checkstyle-plugin" % "4.0.1")
// By default, sbt-checkstyle-plugin uses checkstyle version 6.15, but we should set it to use the
// same version as Spark
dependencyOverrides += "com.puppycrawl.tools" % "checkstyle" % "9.3"

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.7")

addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.4")
