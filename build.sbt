ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "hello-scala",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql"  % "3.5.0",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4"
    )
  )

javacOptions ++= Seq("-source", "11", "-target", "11")