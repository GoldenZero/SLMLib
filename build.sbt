name := "SLMLib"

version := "1.0.3"

scalaVersion := "2.10.4"



libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.2"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.1.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.3.1" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))

libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "4.9.0"





