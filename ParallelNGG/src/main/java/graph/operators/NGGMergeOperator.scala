import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}
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
  def getGraph(graphs: RDD[NGramGraph]): Graph[String, Double] = {
    val mergedEdges = graphs.flatMap(_.edges).mapValues((_,1))
      // hash partition the edges
      .partitionBy(new HashPartitioner(numPartitions))
      // merge the edges
      .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2) }
      // calculate the new edge weights
      .mapValues { case (sum, count) => sum / count }
      // map to GraphX edges
      .map(e => Edge(e._1._1, e._1._2, e._2))
    // return distributed graph
    Graph.fromEdges(mergedEdges, "merged")
  }

}
