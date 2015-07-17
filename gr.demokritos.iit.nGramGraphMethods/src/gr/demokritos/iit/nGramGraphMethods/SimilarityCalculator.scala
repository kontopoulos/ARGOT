package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait SimilarityCalculator {
  
  //@return similarity between two graphs
  def getSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Similarity
  
}