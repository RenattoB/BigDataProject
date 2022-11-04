ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"

ThisBuild / libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

ThisBuild / libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProject"
  )
