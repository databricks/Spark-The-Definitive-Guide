// Package Information

name := "example" // change to project name
organization := "com.databricks" // change to your org
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.8"

// Spark Information
val sparkVersion = "2.1.0"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at
  "https://dl.bintray.com/spark-packages/maven/"

resolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

resolvers += "MavenRepository" at
  "https://mvnrepository.com/"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // spark-modules
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  // "org.apache.spark" %% "spark-mllib" % sparkVersion,

  // spark packages
  "graphframes" % "graphframes" % "0.4.0-spark2.1-s_2.11",

  // testing
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)


//////////
///// Databricks Settings
//////////

// Your username to login to Databricks
val dbcUsername = sys.env("DATABRICKSUSERNAME")

// Your password (Can be set as an environment variable)
val dbcPassword = sys.env("DATABRICKSPASSWORD")
// Gotcha: Setting environment variables in IDE's may differ.
// IDE's usually don't pick up environment variables from .bash_profile or .bashrc

// The URL to the Databricks REST API
val dbcApiUrl = "https://your-sub-domain.cloud.databricks.com/api/1.2"

// Add any clusters that you would like to deploy your work to. e.g. "My Cluster"
val dbcClusters = Seq("my-cluster")
// Add "ALL_CLUSTERS" if you want to attach your work to all clusters

// An optional parameter to set the location to upload your libraries to in the workspace
// e.g. "/Shared/libraries"
// This location must be an existing path and all folders must exist.
// NOTE: Specifying this parameter is *strongly* recommended as many jars will be uploaded to your cluster.
// Putting them in one folder will make it easy for your to delete all the libraries at once.
// Default is "/"
val dbcLibraryPath = "/Shared/Libraries"

// Whether to restart the clusters everytime a new version is uploaded to Databricks.
val dbcRestartOnAttach = false // Default true

//////////
///// END Databricks Settings
//////////

mainClass in(Compile, packageBin) := Some("com.databricks.example.MainClass")

// Compiler settings. Use scalac -X for other options and their description.
// See Here for more info http://www.scala-lang.org/files/archive/nightly/docs/manual/html/scalac.html
scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

// ScalaTest settings.
// Ignore tests tagged as @Slow (they should be picked only by integration test)
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l",
  "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")

