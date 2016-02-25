import java.io.FileInputStream

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class OpenNLPSentenceSplitter(val modelFile: String) extends SentenceSplitter with Serializable {

  /**
    * Given an entity return an RDD containing its sentences
    * @param e entity to segment into sentences
    * @return rdd of sentences
    */
  override def getSentences(e: Entity): RDD[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    //split string of each partition to sentences
    val badSentences = en.dataStringRDD.glom.map(_.mkString(" ").replaceAll(" +", " "))
      .flatMap(text => getDetectedSentences(text))
    //the text is originally split into lines and these lines spread to the partitions (that`s how spark reads a file)
    //sentences of the text might consist of more than one lines
    //because a sentence might be split to more than one partitions
    //we take the sentences that were not split to partitions
    val goodSplits = badSentences
        .filter(s => (s.endsWith(".") || s.endsWith("?") || s.endsWith("!") || s.endsWith(":") || s.endsWith(";")) && !Character.isLowerCase(s.codePointAt(0)))
    //we take sentences that were split to partitions
    val badSplits = badSentences
      .filter(s => (!s.endsWith(".") && !s.endsWith("?") && !s.endsWith("!") && !s.endsWith(":") && !s.endsWith(";")) || !Character.isUpperCase(s.codePointAt(0)))
      //we move them to one partition, create a single string and we split to sentences
      //even in a huge cluster the sentences that were split are not many
      //so moving them to one partition does not have a huge impact on the memory of that partition
      .coalesce(1).glom.map(_.mkString(" "))
      .flatMap(text => getDetectedSentences(text))
    //we union the good sentences with the partitioned ones and take the distinct
    goodSplits.union(badSplits).distinct.map(s => new StringAtom(s.hashCode, s))
  }

  /**
    * Segment input text to sentences using OpenNLP
    * @param text to segment into sentences
    * @return array of sentences
    */
  private def getDetectedSentences(text: String): Array[String] = {
    try {
      //if a trained model is found, use it to split sentences
      val model = new FileInputStream(modelFile)
      val sentenceModel = new SentenceModel(model)
      model.close
      val sentenceDetector = new SentenceDetectorME(sentenceModel)
      sentenceDetector.sentDetect(text)
    }
    catch {
      //else use simple string splitting (very inaccurate)
      case e: Exception => text.split("[.!?;:\\\"']")
    }
  }

}
