package graph.operators

import graph.NGramGraph
import org.apache.spark.rdd.RDD
import traits.DocumentGraph

/**
  * @author Kontopoulos Ioannis
  */
class MultipleGraphIntersectOperator extends Serializable {

  /**
    * Intersects all small graphs into one
    * with default learning rate = 0.5
    * @param graphs
    * @return document graph
    */
  def getGraph(graphs: RDD[DocumentGraph]): DocumentGraph = {
    val edges = graphs.flatMap(_.edges)
      // group by hash
      .groupBy(_._1)
      // take the edges with more than one weights
      .filter(_._2.size > 1)
      // take only the weights
      .mapValues(_.map(_._2))
      // graph is now very small, so collect to driver
      .collect.toMap
      // calculate the averaged weights
      .mapValues{
        weights =>
          val numWeights = weights.size
          weights.sum / numWeights
      }
    // create a dummy graph
    val g = new NGramGraph(0,0)
    // create graph from edges
    g.fromEdges(edges)
    g
  }

}
