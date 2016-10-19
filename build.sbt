name := "sparkGrep"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.5.0","org.apache.spark" %% "spark-mllib" % "1.5.0")
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.4.1" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp"))
