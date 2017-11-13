package myOrg

import myOrg.utils.SparkBaseRunner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.graphframes._
import org.graphframes.lib.AggregateMessages

object SparkJob extends SparkBaseRunner {
  val spark = createSparkSession(this.getClass.getName)

  import spark.implicits._

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
  g.vertices.show
  g.edges.show

  // Query: Get in-degree of each vertex.
  g.inDegrees.show
  g.outDegrees.show

  // Query: Count the number of "follow" connections in the graph.
  g.edges.filter("relationship = 'call'").count

  val friends: DataFrame = g.find("(a)-[e]->(b)")
  friends.show
  val friendsOfFriends: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
  friendsOfFriends.show

  // Find chains of 4 vertices.
  val chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
  //  (b) Use sequence operation to apply method to sequence of elements in motif.
  //      In this case, the elements are the 3 edges.
  val condition = Seq("ab", "bc", "cd").
    foldLeft(lit(0))((cnt, e) => sumSms(cnt, col(e)("relationship")))
  //  (c) Apply filter to DataFrame.
  val chainWith2Friends2 = chain4.where(condition >= 2)


  // We will use AggregateMessages utilities later, so name it "AM" for short.
  val AM = AggregateMessages
  // For each user, sum the ages of the adjacent users.
  val msgToSrc = AM.dst("fraud")
  chainWith2Friends2.show()
  val msgToDst = AM.src("fraud")
  val agg = g.aggregateMessages
    .sendToSrc(msgToSrc) // send destination user's age to source
    .sendToDst(msgToDst) // send source user's age to destination
    .agg(mean(AM.msg).as("fraudScore")) // fraud score, stored in AM.msg column

  // Query on sequence, with state (cnt)
  //  (a) Define method for updating state given the next element of the motif.
  def sumSms(cnt: Column, relationship: Column): Column = {
    when(relationship === "sms", cnt + 1).otherwise(cnt)
  }

  agg.show
  g.vertices.join(agg).show

  // Below can filter out only for specific interaction type - but that would require multiple passes over the data
  //  val msgForSrc: Column = when(AM.edge("relationship") === "sms", AM.dst("fraud"))
  //  val msgForDst: Column = when(AM.edge("relationship") === "sms", AM.src("fraud"))

  // also it would be interesting to see how to add weighting
  //  val msgForSrc: Column = when(AM.src("color") === color, AM.edge("b") * AM.dst("belief"))


}
