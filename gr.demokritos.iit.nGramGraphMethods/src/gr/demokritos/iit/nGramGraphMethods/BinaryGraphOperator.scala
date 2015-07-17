package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait BinaryGraphOperator {
  
  //@return graph after specific operation
  def getResult(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Graph[(String, Int), Int]
  
}