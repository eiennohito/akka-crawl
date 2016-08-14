name := """crawl-test"""

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.jsoup" % "jsoup" % "1.9.2",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.9-RC2",
  "com.optimaize.languagedetector" % "language-detector" % "0.5",
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

// Change this to another test framework if you prefer
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

