// Defaults match GCP Serverless runtime 2.2 (Scala 2.13 / Spark 3.5).
// Override with -Dsourcework.scalaVersion=... -Dsourcework.sparkVersion=...
ThisBuild / scalaVersion := sys.props.getOrElse("sourcework.scalaVersion", "2.13.12")
ThisBuild / version := "0.1.0"
name := "source-onboarding"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sys.props.getOrElse("sourcework.sparkVersion", "3.5.3") % "provided"
Compile / unmanagedSourceDirectories += baseDirectory.value / ".." / "examples" / "default"
