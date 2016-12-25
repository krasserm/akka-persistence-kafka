organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.5-cedb5ec150224ebe15c03470e98bb6578a4228f2"

scalaVersion := "2.12.1"

crossScalaVersions := Seq("2.10.4", "2.11.6", "2.12.1")

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"   %% "akka-persistence"              % "2.4.16",
  "com.typesafe.akka"   %% "akka-persistence-tck"          % "2.4.16" % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % "2.4.16" % Test,
  "commons-io"           % "commons-io"                    % "2.5"      % Test,
  "org.apache.kafka"    %% "kafka"                         % "0.10.1.1",
  "org.apache.curator"   % "curator-test"                  % "3.2.1"    % Test,
  "org.slf4j" 		 % "slf4j-log4j12" 		   % "1.7.22"	% Test,
  "com.typesafe.akka" 	%% "akka-slf4j" 		   % "2.4.16"	% Test
)

/*artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  val lastCommit = git.gitHeadCommit.value
  val suffix = {
    if (module.revision.trim.endsWith("-SNAPSHOT") && lastCommit.isDefined)
      s"${module.revision.split('-')(0)}-${lastCommit.get}"
    else module.revision
  }

  artifact.name + "_" + sv.binary + "-" + suffix + "." + artifact.extension
}*/

publishTo := Some("Bintray API Realm" at "https://api.bintray.com/maven/worldline-messaging-org/maven/akka-persistence-kafka")
//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
