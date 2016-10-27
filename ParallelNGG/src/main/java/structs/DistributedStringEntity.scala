import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class DistributedStringEntity extends Entity {

  // RDD containing the lines of a text file
  private var dataStringRDD: RDD[String] = null

  override def getPayload: RDD[String] = dataStringRDD

  /**
    * Reads dataString from a file
    * @param sc SparkContext
    * @param file file to read
    * @param numPartitions number of partitions
    */
  def fromFile(sc: SparkContext, file: String, numPartitions: Int) = {
    dataStringRDD = sc.textFile(file, numPartitions)
  }

}
