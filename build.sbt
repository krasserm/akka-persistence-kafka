organization := "com.github.krasserm"

name := "akka-persistence-kafka"

version := "0.5-c47a57e6d30ac65f7e5883af98a0691d749b66ec"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

scalacOptions += "-target:jvm-1.7"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

parallelExecution in Test := false

publishArtifact in Test := true

libraryDependencies ++= Seq(
  "com.google.protobuf"  % "protobuf-java"                 % "2.5.0",
  "com.typesafe.akka"   %% "akka-persistence"              % "2.4.14",
  "com.typesafe.akka"   %% "akka-persistence-tck"          % "2.4.14" % Test,
  "com.typesafe.akka"   %% "akka-testkit"                  % "2.4.14" % Test,
  "commons-io"           % "commons-io"                    % "2.4"      % Test,
  "org.apache.kafka"    %% "kafka"                         % "0.10.0.1",
  "org.apache.curator"   % "curator-test"                  % "2.7.1"    % Test,
  "org.slf4j" 		 % "slf4j-log4j12" 		   % "1.7.21"	% Test,
  "com.typesafe.akka" 	%% "akka-slf4j" 		   % "2.4.14"	% Test
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
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
