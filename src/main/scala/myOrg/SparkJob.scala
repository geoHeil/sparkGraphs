package myOrg

import myOrg.utils.SparkBaseRunner

object SparkJob extends SparkBaseRunner {
  val spark = createSparkSession(this.getClass.getName)

  import spark.implicits._
  import org.graphframes._

  val v = Seq(
    ("a", "Alice", 1),
    ("b", "Bob", 0),
    ("c", "Charlie", 0),
    ("d", "David", 0),
    ("e", "Esther", 0),
    ("f", "Fanny", 0),
    ("g", "Gabby", 0),
    ("h", "Fraudster", 1)).toDF("id", "name", "fraud")
  val e = Seq(
    ("a", "b", "call"),
    ("b", "c", "sms"),
    ("c", "b", "sms"),
    ("f", "c", "sms"),
    ("e", "f", "sms"),
    ("e", "d", "call"),
    ("d", "a", "call"),
    ("d", "e", "sms"),
    ("a", "e", "call"),
    ("a", "h", "call"),
    ("f", "h", "call")).toDF("src", "dst", "relationship")
  val g = GraphFrame(v, e)

  // Display the vertex and edge DataFrames
  g.vertices.show()
  g.edges.show()

  // Query: Get in-degree of each vertex.
  g.inDegrees.show()

  // find most fraudulent user
  g.vertices.groupBy().max("fraud").show()

  // Query: Count the number of "follow" connections in the graph.
  g.edges.filter("relationship = 'follow'").count()

  // Run PageRank algorithm, and show results.
  val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
  results.vertices.select("id", "pagerank").show()
}
