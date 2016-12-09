package graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
case class DistributedCachedNGramGraph(val edges: RDD[((String,String),Double)]) {

  // store edges to memory
  edges.persist
  // get the number of edges
  val numEdges = edges.count

  /**
    * Clear this graph from memory
    */
  def clearFromMemory: Unit = {
    edges.unpersist()
  }

  /**
    * Converts Distributed Cached Graph to Spark Graph
    * @return Spark Graph
    */
  def toSparkGraph: Graph[String,Double] = {
    val sparkEdges = edges.map(e => Edge(e._1._1.hashCode.toLong,e._1._2.hashCode.toLong,e._2))
    Graph.fromEdges(sparkEdges,"CachedGraph")
  }

}
