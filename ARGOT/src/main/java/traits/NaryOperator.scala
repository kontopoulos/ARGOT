package traits

import org.apache.spark.graphx.Graph

/**
  * @author Kontopoulos Ioannis
  */
trait NaryOperator {

  //@return graph after specific operation
  def getResult(graphs: Seq[Graph[String, Double]]): Graph[String, Double]

}
