package graph

import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
case class DistributedCachedNGramGraph(val edges: RDD[((Long,Long),Double)]) {

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

}
