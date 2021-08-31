name := "analysislog"

version := "0.1"

scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

idePackagePrefix := Some("dev.vergil")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql"  % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
// https://mvnrepository.com/artifact/log4j/log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"

mainClass := Some("dev.vergil.main.FilterRawLog")

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}