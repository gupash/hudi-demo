name := "hudi-demo"

version := "0.1"

scalaVersion := "2.12.15"

val hudiVersion = "0.9.0"
val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  ("org.apache.hudi" % "hudi-hive-sync" % hudiVersion).exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.11"),
  ("org.apache.hudi" %% "hudi-spark3" % hudiVersion).exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.11"),
  ("org.apache.hudi" %% "hudi-spark" % hudiVersion).exclude("com.fasterxml.jackson.module", "jackson-module-scala_2.11")
)