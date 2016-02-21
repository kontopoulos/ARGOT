import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
trait SentenceSplitter {

  def getSentences(e: Entity): RDD[Atom]

}
