name := "kf-dwh-spark-extension"

version := "0.1"

scalaVersion := "2.11.12"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

val spark_version = "2.4.5"

/* Runtime */
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % spark_version % Provided

/* Test */
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % spark_version % "test"


