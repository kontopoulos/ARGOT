import org.apache.spark.graphx.{Edge, Graph}

/**
  * @author Kontopoulos Ioannis
  */
class NGramGraph(ngram: Int, dwin: Int) {

  // variable that holds the edges of the graph
  private var graphEdges: Map[(Long,Long),Double] = Map()

  /**
    * Gets the number of graph edges
    * @return number of edges
    */
  def numEdges: Long = graphEdges.size

  /**
    * Gets the edges of the graph
    * @return array of edges
    */
  def edges: Map[(Long,Long),Double] = graphEdges

  /**
    * Creates a graph from given string
    * @param dataString string text
    */
  def fromString(dataString: String): Unit = {
    val vertices = dataString
      .sliding(ngram)
      .map(atom => (("_" + atom).hashCode.toLong, "_" + atom))
      .toArray

    val edges = (vertices ++ Array.fill(dwin)((-1L, null))) //add dummy vertices at the end
      .sliding(dwin + 1) //slide over dwin + 1 vertices at the time
      .flatMap(arr => {
      val (srcId, _) = arr.head //take first
      // generate 2n edges
      arr.tail.flatMap{case (dstId, _) =>
        Array(Edge(srcId, dstId, 1.0), Edge(dstId, srcId, 1.0))
      }}.filter(e => e.srcId != -1L & e.dstId != -1L)) //drop dummies
      .toArray
      .map(e => ((e.srcId,e.dstId),e.attr))
      .groupBy(e => e._1)
      .mapValues(e => e.length.toDouble).toArray

    graphEdges = edges.toMap
  }

  /**
    * Creates a graph from specified file
    * @param document file
    */
  def fromFile(document: String): Unit = {
    fromString(scala.io.Source.fromFile(document).mkString)
  }

  /**
    * Creates a serial graph from a distributed one
    * @param graph distributed graph
    */
  def fromDistributedGraph(graph: Graph[String,Double]): Unit = {
    graphEdges = graph.edges.map{
      e =>
        ((e.srcId,e.dstId),e.attr)
    }.collect.toMap
  }

}
