package graph

import traits.DocumentGraph

/**
  * @author Kontopoulos Ioannis
  */
class WordNGramGraph(ngram: Int, dwin: Int) extends DocumentGraph {

  // variable that holds the edges of the graph
  override var edges: Map[(String,String),Double] = Map()
  override var numEdges = 0

  /**
    * Creates a graph from given string
    * @param dataString string text
    */
  override def fromString(dataString: String): Unit = {
    val vertices = dataString.split("\\s+")
      .sliding(ngram)
      .map(_.mkString(" "))
      .map(atom => (("_" + atom), atom)).toArray

    edges = (vertices ++ Array.fill(dwin)(("", null))) //add dummy vertices at the end
      .sliding(dwin + 1) //slide over dwin + 1 vertices at the time
      .flatMap(arr => {
      val (srcId, _) = arr.head //take first
      // generate 2n edges
      arr.tail.flatMap{case (dstId, _) =>
        Array(((srcId, dstId), 1.0), ((dstId, srcId), 1.0))
      }}.filter(e => e._1._1 != "" & e._1._2 != "")) //drop dummies
      .toArray
      .groupBy(e => e._1)
      .mapValues(e => e.length.toDouble)

    numEdges = edges.size
  }

}
