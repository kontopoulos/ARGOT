package traits

/**
  * @author Kontopoulos Ioannis
  */
trait DocumentGraph {

  var edges: Map[(String,String),Double]
  var numEdges: Int

  def fromString(dataString: String): Unit

}
