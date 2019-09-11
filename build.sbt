val Specs2Version = "4.7.0"
val FlinkVersion  = "1.9.0"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.sonatypeRepo("snapshots")

lazy val arrow = ProjectRef(uri("https://github.com/Neurodyne/zio-serdes.git#master"), "zio-serdes")

lazy val root = (project in file("."))
  .settings(
    organization := "CloverGroup",
    name := "kafka",
    version := "0.0.1",
    scalaVersion := "2.12.9",
    maxErrors := 3,
    libraryDependencies ++= Seq(
      "org.specs2"       %% "specs2-core"           % Specs2Version % "test",
      "org.apache.flink" %% "flink-streaming-scala" % FlinkVersion,
      "org.apache.flink" %% "flink-connector-kafka" % FlinkVersion
    )
  )
  .dependsOn(arrow)

// Refine scalac params from tpolecat
scalacOptions --= Seq(
  "-Xfatal-warnings"
)
addCompilerPlugin(scalafixSemanticdb)
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

// Aliases
addCommandAlias("com", "all compile test:compile it:compile")
addCommandAlias("fix", "all compile:scalafix test:scalafix")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
