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


// resolvers += Resolver.url("artifactory", url("https://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

// resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

// resolvers += "Typesafe Repository" at "https://repo.typesafe.com/typesafe/releases/"

// resolvers += DefaultMavenRepository

// resolvers += Resolver.url("bintray-sbt-plugins", url("https://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")

// addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")

// addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

// addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.9")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")

// addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.3")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.4.3")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.9.2")

addSbtPlugin("com.simplytyped" % "sbt-antlr4" % "0.8.3")

// resolvers += Resolver.url("typesafe sbt-plugins",
//   url("https://dl.bintray.com/typesafe/sbt-plugins"))(Resolver.ivyStylePatterns)
