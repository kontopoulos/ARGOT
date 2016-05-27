import org.apache.spark.SparkContext

/**
  * @author Kontopoulos Ioannis
  */
class RedundancyRemover(sc: SparkContext) extends SentenceFilter {

  /**
    * Removes redundant sentences based on the
    * normalized value similarity between them
    * @param sentences sentences to trim
    * @return the trimmed array of sentences
    */
  override def getFilteredSentences(sentences: Array[String]): Array[String] = {
    val nggc = new NGramGraphCreator(3,3)
    val gsc = new GraphSimilarityCalculator

    //variable that holds the sentences that should not enter the trimmed array
    var badSentences = Array.empty[String]
    //variable that holds the trimmed sentences
    var trimmedSentences = Array.empty[String]
    val numSentences = sentences.length

    var next = 1
    sentences.foreach{s =>
      if (!badSentences.contains(s)) {
        trimmedSentences :+= s
        val curE = new StringEntity
        curE.fromString(sc,s,1)
        val curG = nggc.getGraph(curE)
        curG.cache
        for (i <- next to numSentences-1) {
          val curS = sentences(i)
          val e = new StringEntity
          e.fromString(sc,curS,1)
          val g = nggc.getGraph(e)
          val nvs = gsc.getSimilarity(g,curG).getSimilarityComponents("normalized")
          //if similarity over threshold, it means the sentence contains repeated information
          if (nvs > 0.2) {
            badSentences :+= curS
          }
        }
        curG.unpersist()
      }
      next += 1
    }
    //return trimmed sentences
    trimmedSentences
  }

}
