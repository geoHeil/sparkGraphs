name := "sparkMiniSample"
organization := "myOrg"

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Xlint:missing-interpolator",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code",
  "-Ywarn-unused"
)

//The default SBT testing java options are too small to support running many of the tests
// due to the need to launch Spark in local mode.
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

lazy val spark = "2.2.0"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark % "provided",
  "org.apache.spark" %% "spark-sql" % spark % "provided",
  "com.typesafe" % "config" % "1.3.1",
  "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11",
  //  "org.apache.spark" %% "spark-hive" % spark % "provided",
  "org.apache.spark" %% "spark-graphx" % spark % "provided",
  //  "org.apache.spark" %% "spark-mllib" % spark % "provided",
  //  "org.apache.spark" %% "spark-streaming" % spark % "provided",
  "com.holdenkarau" %% "spark-testing-base" % s"${spark}_0.8.0" % "test"
)

fork := true
fullClasspath in reStart := (fullClasspath in Compile).value
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass.in(Compile, run), runner.in(Compile, run)).evaluated

// outlining some dummy assembly configuration
assemblyMergeStrategy in assembly := {
  //  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.deduplicate
}

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("com.google.**" -> "shadedguava.@1").inAll
//)

//test in assembly := {}

initialCommands in console :=
  """
    |
    |import org.apache.spark.SparkConf
    |import org.apache.spark.sql.{Column, DataFrame}
    |import org.apache.spark.sql.functions._
    |import org.apache.spark.sql.{SparkSession}
    |import org.graphframes.lib.AggregateMessages
    |
    |val conf: SparkConf = new SparkConf()
    |    .setAppName("exampleSQL")
    |    .setMaster("local[*]")
    |
    |val spark: SparkSession = SparkSession
    |    .builder()
    |    .config(conf)
    |    .getOrCreate()
    |
    |import spark.implicits._
    |val spark = createSparkSession(this.getClass.getName)
    |import org.graphframes._
  """.stripMargin

mainClass := Some("myOrg.SparkJob")