package traits

import graph.NGramGraph

/**
  * @author Kontopoulos Ioannis
  */
trait SimilarityComparator {

  //@return similarity between two small graphs
  def getSimilarity(g1: NGramGraph, g2: NGramGraph): Similarity

}
