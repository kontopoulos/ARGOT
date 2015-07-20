package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarity(var sizeSimilarity: Double) extends Similarity {
  
  
  //value of value similarity
  val valueSimilarity = 0.0
  //value of containment similarity
  val containmentSimilarity = 0.0
  //map containing similarity components
  var components = Map(("size", sizeSimilarity), ("value", valueSimilarity), ("containment", containmentSimilarity))
  
  /**
   * Calculates overall similarity
   * @return overall similarity
   */
  override def getOverallSimilarity = sizeSimilarity * valueSimilarity * containmentSimilarity
  
  /**
   * @return map with similarity components
   */
  override def getSimilarityComponents = components
  
}