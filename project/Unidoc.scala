/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import sbt._
import sbt.Keys._
import sbtunidoc._
import sbtunidoc.BaseUnidocPlugin.autoImport._
import sbtunidoc.ScalaUnidocPlugin.autoImport._
import sbtunidoc.JavaUnidocPlugin.autoImport._

object Unidoc {

  /**
   * Patterns are strings to do simple substring matches on the full path of every source file.
   */
  case class SourceFilePattern(patterns: Seq[String], project: Option[Project] = None)

  object SourceFilePattern {
    def apply(patterns: String*): SourceFilePattern = SourceFilePattern(patterns.toSeq, None)
  }

  val unidocSourceFilePatterns = settingKey[Seq[SourceFilePattern]](
      "Patterns to match (simple substring match) against full source file paths. " +
        "Matched files will be selected for generating API docs.")

  implicit class PatternsHelper(patterns: Seq[SourceFilePattern]) {
    def scopeToProject(projectToAdd: Project): Seq[SourceFilePattern] = {
      patterns.map(_.copy(project = Some(projectToAdd)))
    }
  }

  implicit class UnidocHelper(val projectToUpdate: Project) {
    def configureUnidoc(
      docTitle: String = null,
      generatedJavaDoc: Boolean = true,
      generateScalaDoc: Boolean = false,
      classPathToSkip: String = null
    ): Project = {
      if (sys.env.contains("DISABLE_UNIDOC")) return projectToUpdate
      if (!generatedJavaDoc && !generateScalaDoc) return projectToUpdate

      var updatedProject: Project = projectToUpdate
      if (generateScalaDoc) {
        updatedProject = updatedProject.enablePlugins(ScalaUnidocPlugin)
      }
      updatedProject
        .enablePlugins(GenJavadocPlugin, JavaUnidocPlugin)
        // TODO: Allows maven publishing to use unidoc doc jar, but it currently throws errors.
        // .enablePlugins(PublishJavadocPlugin)
        .settings(
          libraryDependencies ++= Seq(
            // Ensure genJavaDoc plugin is of the right version that works with Scala 2.12
            compilerPlugin(
              "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
          ),

          generateUnidocSettings(docTitle, generateScalaDoc, classPathToSkip),

          // Ensure unidoc is run with tests.
          (Test / test) := ((Test / test) dependsOn (Compile / unidoc)).value,

          // hide package private types and methods in javadoc
          scalacOptions ++= Seq(
            "-P:genjavadoc:strictVisibility=true"
          ),
        )
    }

    private def generateUnidocSettings(
        customDocTitle: String,
        generateScalaDoc: Boolean,
        classPathToSkip : String): Def.SettingsDefinition = {

      val internalFilePattern = Seq("/internal/", "/execution/", "$")

      // Generate the full doc title
      def fullDocTitle(projectName: String, version: String, isScalaDoc: Boolean): String = {
        val namePart = Option(customDocTitle).getOrElse {
          projectName.split("-").map(_.capitalize).mkString(" ")
        }
        val versionPart = version.replaceAll("-SNAPSHOT", "")
        val langPart = if (isScalaDoc) "Scala API Docs" else "Java API Docs"
        s"$namePart $versionPart - $langPart"
      }

      // Remove source files that does not match the pattern
      def ignoreUndocumentedSources(
        allSourceFiles: Seq[Seq[java.io.File]],
        sourceFilePatternsToKeep: Seq[SourceFilePattern]
      ): Seq[Seq[java.io.File]] = {
        if (sourceFilePatternsToKeep.isEmpty) return Nil

        val projectSrcDirToFilePatternsToKeep = sourceFilePatternsToKeep.map {
          case SourceFilePattern(dirs, projOption) =>
            val projectPath = projOption.getOrElse(projectToUpdate).base.getCanonicalPath
            projectPath -> dirs
        }.toMap

        def shouldKeep(path: String): Boolean = {
          projectSrcDirToFilePatternsToKeep.foreach { case (projBaseDir, filePatterns) =>
            def isInProjectSrcDir =
              path.contains(s"$projBaseDir/src") || path.contains(s"$projBaseDir/target/java/")
            def matchesFilePattern = filePatterns.exists(path.contains(_))
            def matchesInternalFilePattern = internalFilePattern.exists(path.contains(_))
            if (isInProjectSrcDir && matchesFilePattern && !matchesInternalFilePattern) return true
          }
          false
        }
        allSourceFiles.map {_.filter(f => shouldKeep(f.getCanonicalPath))}
      }

      val javaUnidocSettings = Seq(
        // Configure Java unidoc
        JavaUnidoc / unidoc / javacOptions := Seq(
          "-public",
          "-windowtitle",
          fullDocTitle((projectToUpdate / name).value, version.value, isScalaDoc = false),
          "-noqualifier", "java.lang",
          "-tag", "implNote:a:Implementation Note:",
          "-tag", "apiNote:a:API Note:",
          "-Xdoclint:none"
        ),

        JavaUnidoc / unidoc / unidocAllSources := {
          ignoreUndocumentedSources(
            allSourceFiles = (JavaUnidoc / unidoc / unidocAllSources).value,
            sourceFilePatternsToKeep = unidocSourceFilePatterns.value)
        },

        // Settings for plain, old Java doc needed for successful doc generation during publishing.
        Compile / doc / javacOptions ++= Seq(
          "-public",
          "-noqualifier", "java.lang",
          "-tag", "implNote:a:Implementation Note:",
          "-tag", "apiNote:a:API Note:",
          "-Xdoclint:all")
      )

      val scalaUnidocSettings = if (generateScalaDoc) Seq(
        // Configure Scala unidoc
        ScalaUnidoc / unidoc / scalacOptions ++= Seq(
          "-doc-title",
          fullDocTitle((projectToUpdate / name).value, version.value, isScalaDoc = true),
        ),

        ScalaUnidoc / unidoc / unidocAllSources := {
          ignoreUndocumentedSources(
            allSourceFiles = (ScalaUnidoc / unidoc / unidocAllSources).value,
            sourceFilePatternsToKeep = unidocSourceFilePatterns.value
          )
        },

        ScalaUnidoc / unidoc / fullClasspath := {
          (ScalaUnidoc / unidoc / fullClasspath).value
            .filter(f =>
              classPathToSkip == null || !f.data.getCanonicalPath.contains(classPathToSkip))
        }
      ) else Nil

      javaUnidocSettings ++ scalaUnidocSettings
    }
  }
}

