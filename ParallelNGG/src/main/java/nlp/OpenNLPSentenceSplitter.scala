import java.io.FileInputStream

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class OpenNLPSentenceSplitter(val modelFile: String) extends SentenceSplitter {

  /**
    * Given an entity return an RDD containing its sentences
    * @param e entity to segment into sentences
    * @return rdd of sentences
    */
  override def getSentences(e: Entity): RDD[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    en.dataStringRDD.glom.map(_.mkString(" "))
      .flatMap(text => getDetectedSentences(text))
      .map(sentence => new StringAtom(sentence.hashCode, sentence))
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
