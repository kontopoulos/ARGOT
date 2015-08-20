package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarity(private val sizeSimilarity: Double, private val valueSimilarity: Double, private val containmentSimilarity: Double, private val normalizedValueSimilarity: Double) extends Similarity {
  
  /**
   * Calculates overall similarity
   * @return overall similarity
   */
  override def getOverallSimilarity = sizeSimilarity * valueSimilarity * containmentSimilarity
  
  /**
   * @return map with similarity components
   */
  override def getSimilarityComponents: Map[String, Double] = {
    val components = Map(("size", sizeSimilarity), ("value", valueSimilarity), ("containment", containmentSimilarity), ("normalized", normalizedValueSimilarity))
    components
  }
  
}