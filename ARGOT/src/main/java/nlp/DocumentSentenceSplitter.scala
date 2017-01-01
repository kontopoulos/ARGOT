package nlp

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}
import org.apache.spark.rdd.RDD
import structs.DocumentAtom
import traits.Atom

/**
  * @author Kontopoulos Ioannis
  */
class DocumentSentenceSplitter(val modelFile: String) extends Serializable {

  /**
    * Given an RDD of documents return an RDD containing their sentences
    * @param documents documents to segment into sentences
    * @return rdd of sentences
    */
  def getSentences(documents: RDD[String]): RDD[Atom] = {
    documents.flatMap(getDetectedSentences(_))
      .map(sentence => DocumentAtom(sentence.hashCode.toLong,sentence))
  }

  /**
    * Segment input text to sentences using OpenNLP
    * @param text to segment into sentences
    * @return array of sentences
    */
  private def getDetectedSentences(text: String): Array[String] = {
    try {
      //if a trained model is found, use it to split sentences
      val model = new java.io.FileInputStream(modelFile)
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
