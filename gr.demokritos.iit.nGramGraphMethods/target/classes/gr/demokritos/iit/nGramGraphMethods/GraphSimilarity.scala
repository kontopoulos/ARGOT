package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarity extends Similarity {
  
  //value of size similarity
  var sizeSimilarity = 0.0
  //value of value similarity
  var valueSimilarity = 0.0
  //value of containment similarity
  var containmentSimilarity = 0.0
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