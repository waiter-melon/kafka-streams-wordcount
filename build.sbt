name := "akka-streams-word-count"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.11.0.0"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.25"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25" % "test"
libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.11.0.0"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.3"

// leverage java 8
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
scalacOptions := Seq("-target:jvm-1.8")
initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}