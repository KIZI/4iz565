name := "scala-apriori-spark"

version := "0.1"

scalaVersion := "2.11.8"

fork := true

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"

dependencyOverrides ++= Seq(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "2.2",
  "com.google.guava" % "guava" % "11.0.2"
)