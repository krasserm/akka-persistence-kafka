organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.2"

scalaVersion := "2.10.4"

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  //
  // Compile dependencies
  //
  "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.4",
  "org.apache.curator" % "curator-test" % "2.5.0",
  "org.apache.kafka"  %% "kafka" % "0.8.1.1",
  //
  // Test dependencies
  //
  "com.github.krasserm" %% "akka-persistence-testkit" % "0.3.4" % "test",
  "commons-io"           % "commons-io"               % "2.4"   % "test"
)