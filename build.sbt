organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.5"

crossScalaVersions := Seq("2.10.4", "2.11.5")

scalacOptions += "-target:jvm-1.7"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  //
  // Compile dependencies
  //
  "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.9",
  "org.apache.kafka"  %% "kafka" % "0.8.2.0",
  //
  // Test dependencies
  //
  "org.apache.curator" % "curator-test" % "2.7.1" % Test,
  "com.github.krasserm" %% "akka-persistence-testkit" % "0.3.4" % "test",
  "commons-io"           % "commons-io"               % "2.4"   % "test"
)
