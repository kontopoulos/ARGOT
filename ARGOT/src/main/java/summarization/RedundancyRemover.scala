package summarization

import org.apache.spark.SparkContext
import traits.SentenceFilter

/**
  * @author Kontopoulos Ioannis
  */
class RedundancyRemover(sc: SparkContext) extends SentenceFilter {

  // TODO better algorithm for removing redundant sentences
  /**
    * Removes redundant sentences based on the
    * normalized value similarity between them
    * @param sentences sentences to trim
    * @return the trimmed array of sentences
    */
  override def getFilteredSentences(sentences: Array[String]): Array[String] = ???
  /*{
    val ngc = new NGramCachedGraphComparator

    //variable that holds the sentences that should not enter the trimmed array
    var badSentences = Array.empty[String]
    //variable that holds the trimmed sentences
    var trimmedSentences = Array.empty[String]
    val numSentences = sentences.length

    var next = 1
    sentences.foreach{s =>
      if (!badSentences.contains(s)) {
        trimmedSentences :+= s
        val curG = new DocumentNGramSymWinGraph(3,3,3)
        curG.setDataString(s)
        for (i <- next to numSentences-1) {
          val curS = sentences(i)
          val g = new DocumentNGramSymWinGraph(3,3,3)
          g.setDataString(curS)
          val gs = ngc.getSimilarityBetween(g,curG)
          val nvs = gs.ValueSimilarity/gs.SizeSimilarity
          //if similarity over threshold, it means the sentence contains repeated information
          if (nvs > 0.2) {
            badSentences :+= curS
          }
        }
      }
      next += 1
    }
    //return trimmed sentences
    trimmedSentences
  }*/

}
