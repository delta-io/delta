import sbt._
import sbt.Keys._
import sbtunidoc._
import sbtunidoc.BaseUnidocPlugin.autoImport._
import sbtunidoc.ScalaUnidocPlugin.autoImport._
import sbtunidoc.JavaUnidocPlugin.autoImport._

object Unidoc {

  implicit class UnidocHelpers(project: Project) {
    def configureUnidoc(
      docTitle: String = "Delta Lake",
      projectSrcDirToFilePatternsToKeep: Map[String, Seq[String]] = Map.empty,
      generateScalaDoc: Boolean = false
    ): Project = {

      var updatedProject: Project = project
      if (generateScalaDoc) {
        updatedProject = updatedProject.enablePlugins(ScalaUnidocPlugin)
      }
      updatedProject
        .enablePlugins(GenJavadocPlugin, PublishJavadocPlugin, JavaUnidocPlugin)
        .settings (
          libraryDependencies ++= Seq(
            compilerPlugin(
              "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.18" cross CrossVersion.full)
          ),
          generateUnidocSettings(docTitle, projectSrcDirToFilePatternsToKeep),
        )
    }
  }

  private def generateUnidocSettings(
    docTitle: String = "Delta Lake",
    projectSrcDirToFilePatternsToKeep: Map[String, Seq[String]] = Map.empty,
    generateScalaDoc: Boolean = false
  ): Def.SettingsDefinition = {

    val internalFilePattern = Seq("/internal/", "/execution/", "$")

    // Explicitly remove source files that does not match the pattern
    def ignoreUndocumentedSources(packages: Seq[Seq[java.io.File]]): Seq[Seq[java.io.File]] = {
      if (projectSrcDirToFilePatternsToKeep.isEmpty) return Nil

      def shouldKeep(path: String): Boolean = {
        projectSrcDirToFilePatternsToKeep.foreach { case (projectSrcDir, filePatterns) =>
          def isInProjectSrcDir =
            path.contains(s"$projectSrcDir/src") || path.contains(s"$projectSrcDir/target/java/")
          def matchesFilePattern = filePatterns.exists(path.contains(_))
          def matchesInternalFilePattern = internalFilePattern.exists(path.contains(_))
          if (isInProjectSrcDir && matchesFilePattern && !matchesInternalFilePattern) return true
        }
        false
      }
      packages.map {_.filter(f => shouldKeep(f.getCanonicalPath)) }
    }

    val javaUnidocSettings = Seq(
      // Configure Java unidoc
      JavaUnidoc / unidoc / javacOptions := Seq(
        "-public",
        "-windowtitle",
        docTitle + " " + version.value.replaceAll("-SNAPSHOT", "") + " JavaDoc",
        "-noqualifier", "java.lang",
        "-tag", "implNote:a:Implementation Note:",
        "-tag", "apiNote:a:API Note:",
        "-tag", "return:X",
        // `doclint` is disabled on Circle CI. Need to enable it manually to test our javadoc.
        "-Xdoclint:none"
      ),

      JavaUnidoc / unidoc / unidocAllSources := {
        ignoreUndocumentedSources((JavaUnidoc / unidoc / unidocAllSources).value)
      },
    )

    val scalaUnidocSettings = if (generateScalaDoc) Seq(
      // Configure Scala unidoc
      ScalaUnidoc / unidoc / scalacOptions ++= Seq(
        "-doc-title",
        docTitle + " " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc",
      ),

      ScalaUnidoc / unidoc / unidocAllSources := {
        ignoreUndocumentedSources((ScalaUnidoc / unidoc / unidocAllSources).value)
      },
    ) else Nil

    javaUnidocSettings ++ scalaUnidocSettings
  }
}
