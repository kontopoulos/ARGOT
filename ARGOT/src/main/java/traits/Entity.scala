package traits

import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
trait Entity {

  def getAtoms(numPartitions: Int): RDD[Atom]

}