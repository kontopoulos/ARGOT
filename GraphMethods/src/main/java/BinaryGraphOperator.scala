import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait BinaryGraphOperator {

  //@return graph after specific operation
  def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double]

}