name := "sparkGrep"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.5.0","org.apache.spark" %% "spark-mllib" % "1.5.0")
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "1.2.3",
  "org.apache.hbase" % "hbase-client" % "1.2.3",
  "org.apache.hbase" % "hbase-common" % "1.2.3",
  "org.apache.hbase" % "hbase-server" % "1.2.3"
)
