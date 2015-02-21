name := "couchdoop-spark-demo"

version := "1.0.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0-cdh5.3.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.5.0-cdh5.3.0" % "provided",
//  "com.avira.couchdoop" % "couchdoop" % "1.7.3"
  "com.avira.couchdoop" % "couchdoop" % "1.8.0-SNAPSHOT"
)

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
