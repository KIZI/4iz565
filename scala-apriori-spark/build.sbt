name := "scala-apriori-spark"

version := "0.1"

scalaVersion := "2.11.8"

fork := true

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"

dependencyOverrides ++= Seq(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "2.2",
  "com.google.guava" % "guava" % "11.0.2"
)