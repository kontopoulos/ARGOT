package graph.operators

import graph.{DistributedCachedNGramGraph, NGramGraph}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class NGGMergeOperator(numPartitions: Int) extends Serializable {

  /**
    * Merges all small graphs from files
    * into a large distributed one with
    * default learning rate = 0.5
    * @param graphs
    * @return distributed graph
    */
  def getGraph(graphs: RDD[NGramGraph]): DistributedCachedNGramGraph = {
    val mergedEdges = graphs.flatMap(_.edges).mapValues((_,1))
      // partition the edges by hash
      .partitionBy(new HashPartitioner(numPartitions))
      // merge the edges
      .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
      // calculate the new edge weights
      .mapValues { case (sum, count) => sum / count }
    DistributedCachedNGramGraph(mergedEdges)
  }

}
