/**
  * @author Kontopoulos Ioannis
  */
trait Clustering {

  def getClusters(path: String): Map[Int, Array[String]]

}
