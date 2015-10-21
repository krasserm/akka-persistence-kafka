organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.5"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

scalacOptions += "-target:jvm-1.7"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"   %% "akka-persistence"              % "2.4.0",
  "com.typesafe.akka"   %% "akka-persistence-tck"          % "2.4.0" % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % "2.4.0" % Test,
  "commons-io"           % "commons-io"                    % "2.4"      % Test,
  "org.apache.kafka"    %% "kafka"                         % "0.8.2.1",
  "org.apache.curator"   % "curator-test"                  % "2.7.1"    % Test
)

