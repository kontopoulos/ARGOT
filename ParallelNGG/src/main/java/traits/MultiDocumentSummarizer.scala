/**
  * @author Kontopoulos Ioannis
  */
trait MultiDocumentSummarizer {

  def getSummary(directory: String): Map[Int,Array[String]]

}
