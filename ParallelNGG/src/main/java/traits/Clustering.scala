package traits

/**
  * @author Kontopoulos Ioannis
  */
trait Clustering {

  def getClusters(documents: Array[String]): Map[Int, Array[String]]

}
