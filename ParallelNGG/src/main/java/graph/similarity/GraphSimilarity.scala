package graph.similarity

import traits.Similarity

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarity(private val sizeSimilarity: Double, private val valueSimilarity: Double, private val containmentSimilarity: Double) extends Similarity {

  /**
   * Calculates overall similarity
   * @return overall similarity
   */
  override def getOverallSimilarity = sizeSimilarity * valueSimilarity * containmentSimilarity

  /**
   * @return map with similarity components
   */
  override def getSimilarityComponents: Map[String, Double] = {
    Map(("size", sizeSimilarity), ("value", valueSimilarity), ("containment", containmentSimilarity), ("normalized", valueSimilarity/sizeSimilarity))
  }

  override def toString: String = {
    val sSimil = BigDecimal(sizeSimilarity*100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val vSimil = BigDecimal(valueSimilarity*100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val nSimil = BigDecimal(valueSimilarity/sizeSimilarity*100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val cSimil = BigDecimal(containmentSimilarity*100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    val aSimil = (sSimil + vSimil + cSimil)/3
    "Size Similarity: " + sSimil + "%\n" + "Value Similarity: " + vSimil + "%\n" + "Normalized Value Similarity: " + nSimil + "%\n" + "Containment Similarity: " + cSimil + "%\n" + "Averaged Similarity: " + aSimil + "%"
  }

}