package traits

/**
  * @author Kontopoulos Ioannis
  */
trait SimilarityComparator {

  //@return similarity between two small graphs
  def getSimilarity(g1: DocumentGraph, g2: DocumentGraph): Similarity

}
