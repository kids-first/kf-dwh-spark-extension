name := "kf-dwh-spark-extension"

version := "0.2"

scalaVersion := "2.12.10"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "3.0.2"

/* Runtime */
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % spark_version % Provided

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"


