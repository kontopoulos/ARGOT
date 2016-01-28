/**
 * @author Kontopoulos Ioannis
 */
trait Similarity {

  //@return overall similarity
  def getOverallSimilarity: Double
  //@return map with similarity components
  def getSimilarityComponents: Map[String, Double]

}