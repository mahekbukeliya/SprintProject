import sbtassembly.MergeStrategy

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

lazy val root = (project in file("."))
  .settings(
    name := "SparkProject"
  )

ThisBuild / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case  x => MergeStrategy.first
}

val sparkVersion = "3.4.3"
val hadoopVersion = "3.2.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hadoop" % "hadoop-hdfs-native-client" % "3.2.4",
  "io.delta" %% "delta-core" % "2.4.0",
  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.postgresql" % "postgresql" % "42.7.3",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test


  // "org.apache.hive" % "hive-metastore" % "3.1.3",
  // "org.apache.spark" %% "spark-hive" % sparkVersion,
  // "org.apache.hive" % "hive-exec" % "3.1.3"
)

resolvers ++= Seq("Delta Lake Repository" at "https://packages.delta.io/maven",
  "Confluent" at "https://packages.confluent.io/maven/")
