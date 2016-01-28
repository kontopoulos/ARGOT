import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait SimilarityCalculator {

  //@return similarity between two graphs
  def getSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Similarity

}