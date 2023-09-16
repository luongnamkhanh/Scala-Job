ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-job-scala"
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.twitter4j" % "twitter4j-core" % "4.1.2",
  "org.twitter4j" % "twitter4j-stream" % "4.0.7"
)