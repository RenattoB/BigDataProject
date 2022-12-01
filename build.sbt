ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

ThisBuild / libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

ThisBuild / libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8"

ThisBuild / libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.18.0" % "provided"
ThisBuild / libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.18.0" % "provided"

ThisBuild / libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProject"
  )
