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

  val sourceFilePatternsToDocument = settingKey[Seq[SourceFilePattern]](
      "Patterns to match (simple substring match) against full source file paths. " +
        "Matched files will be selected for generating API docs.")

  implicit class UnidocHelpers(val projectToUpdate: Project) {

    def configureUnidoc(
      docTitle: String = null,
      generateScalaDoc: Boolean = false
    ): Project = {

      var updatedProject: Project = projectToUpdate
      if (generateScalaDoc) {
        updatedProject = updatedProject.enablePlugins(ScalaUnidocPlugin)
      }
      updatedProject
        .enablePlugins(GenJavadocPlugin, PublishJavadocPlugin, JavaUnidocPlugin)
        .settings(
          libraryDependencies ++= Seq(
            // Ensure genJavaDoc plugin is of the right version
            compilerPlugin(
              "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
          ),

          generateUnidocSettings(docTitle, generateScalaDoc),

          // Ensure unidoc is run with tests.
          (Test / test) := ((Test / test) dependsOn (Compile / unidoc)).value
        )
    }

    private def generateUnidocSettings(
        customDocTitle: String,
        generateScalaDoc: Boolean): Def.SettingsDefinition = {

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
          fullDocTitle(projectToUpdate / name value, version.value, isScalaDoc = false),
          "-noqualifier", "java.lang",
          "-tag", "implNote:a:Implementation Note:",
          "-tag", "apiNote:a:API Note:",
          "-tag", "return:X",
          "-Xdoclint:none"
        ),

        JavaUnidoc / unidoc / unidocAllSources := {
          ignoreUndocumentedSources(
            allSourceFiles = (JavaUnidoc / unidoc / unidocAllSources).value,
            sourceFilePatternsToKeep = (Compile / unidoc / sourceFilePatternsToDocument).value)
        },
      )

      val scalaUnidocSettings = if (generateScalaDoc) Seq(
        // Configure Scala unidoc
        ScalaUnidoc / unidoc / scalacOptions ++= Seq(
          "-doc-title",
          fullDocTitle(projectToUpdate / name value, version.value, isScalaDoc = true),
        ),

        ScalaUnidoc / unidoc / unidocAllSources := {
          ignoreUndocumentedSources(
            allSourceFiles = (ScalaUnidoc / unidoc / unidocAllSources).value,
            sourceFilePatternsToKeep = (Compile / unidoc / sourceFilePatternsToDocument).value
          )
        },
      ) else Nil

      javaUnidocSettings ++ scalaUnidocSettings
    }
  }
}

