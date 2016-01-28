import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {

  //single string of the entity
  private var singleString = ""
  //RDD containing the lines of a text file
  var dataStringRDD: RDD[String] = null


  /**
   * Returns the actual data of the entity
    *
    * @return dataString
   */
  override def getPayload: String = {
    this.singleString
  }

  /**
    * Sets the value of dataString
    * @param value of dataString
    */
  def setString(value: String) = {
    this.singleString = value
  }

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
