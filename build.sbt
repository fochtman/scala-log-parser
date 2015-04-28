name := "scala-log-parser"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.parboiled" %% "parboiled" % "2.1.0",
  "org.mongodb" %% "casbah" % "2.8.1",
  "org.slf4j" % "slf4j-simple" % "1.6.4"
)
