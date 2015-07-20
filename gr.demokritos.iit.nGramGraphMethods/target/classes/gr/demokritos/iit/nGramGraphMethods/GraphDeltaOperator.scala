package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class GraphDeltaOperator extends BinaryGraphOperator {
  //not actual implementation
  //just basic in order not to have compile errors
  val conf = new SparkConf().setAppName("Graph Methods").setMaster("local")
  val sc = new SparkContext(conf)
  def getResult(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Graph[(String, Int), Int] = {
    val vertexArray = Array(
      (1L, ("a", 28)),
      (2L, ("b", 27)),
      (3L, ("c", 65))
    )
    val edgeArray = Array(
      Edge(1L, 2L, 1),
      Edge(2L, 3L, 8)
    )
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    graph
}
  }