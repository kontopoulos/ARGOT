import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {

  //RDD containing the lines of a text file
  private var dataStringRDD: RDD[String] = null

  def getPayload = dataStringRDD

  /**
    * Reads dataString from a file, distributed version
    * @param sc SparkContext
    * @param file file to read
    * @param numPartitions number of partitions
    * @return lines of file
    */
  def readFile(sc: SparkContext, file: String, numPartitions: Int) = {
    dataStringRDD = sc.textFile(file, numPartitions)
  }

}
