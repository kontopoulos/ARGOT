package traits

import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
trait EntityTokenizer {

  def getTokens(partitionedDocument: RDD[String]): RDD[Atom]

}
