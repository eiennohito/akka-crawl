name := """crawl-test"""

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.9.2",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.11",
  "com.typesafe.akka" %% "akka-slf4j" % "2.4.11",
  "com.optimaize.languagedetector" % "language-detector" % "0.5",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.github.crawler-commons" % "crawler-commons" % "0.6",
  "io.mola.galimatias" % "galimatias" % "0.2.0",
  "com.googlecode.juniversalchardet" % "juniversalchardet" % "1.0.3",
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

// Change this to another test framework if you prefer
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

