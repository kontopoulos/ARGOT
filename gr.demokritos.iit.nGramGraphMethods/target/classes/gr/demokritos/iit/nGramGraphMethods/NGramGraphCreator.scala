package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 * @param ngram size of ngrams
 * @param dwin size of adjacency window
 * @param sc SparkContext
 */
class NGramGraphCreator(val ngram:Int, val dwin:Int, val sc:SparkContext) extends GraphCreator {

  /**
   * Creates a graph based on ngram, dwin and entity
   * @param e entity from which a graph will be created
   * @return graph from entity
   */
  override def getGraph(e: Entity): Graph[String, Double] = {
    val en = e.asInstanceOf[StringEntity]
    //segment the entity
    val seg = new StringFixedNGramSegmentor(ngram)
    //get the list of entity atoms
    val atoms = seg.getComponents(e)
    //set the list of atoms of the entity
    en.setEntityComponents(atoms.map{ case i:StringAtom => i })
    //array that holds the vertices
    var vertices = Array.empty[Tuple2[Long, String]]
    //create vertices
    en.getEntityComponents.foreach{
      i =>
        //if a vertex exists do not add to array, vertices are unique
        if(!(vertices contains (i.label.toLong, i.dataStream))) {
          vertices = vertices ++ Array((i.label.toLong, i.dataStream))
        }
    }
    //array that holds the edges
    var edges = Array.empty[Edge[Double]]
    //create edges
    for(j <- 0 to en.getEntityComponents.size - 1) {
      for(i <- 1 to dwin) {
        if((j+i) < en.getEntityComponents.size) {
          //add edge
          edges = edges ++ Array(Edge(en.getEntityComponents(j).label.toLong, en.getEntityComponents(j+i).label.toLong, 1.0))
          //add inverse edge
          edges = edges ++ Array(Edge(en.getEntityComponents(j+i).label.toLong, en.getEntityComponents(j).label.toLong, 1.0))
        }
      }
    }
    //assign weights where occurrence is the number of times a given pair of n-grams
    //happen to be neighbors in a string within some distance dwin of each other
    var groupedEdges = Array.empty[Edge[Double]]
    for(i <- 0 to edges.length - 1) {
      var occurrence = 1.0
      //we increase by 2 because two consecutive edges are the inverted between them
      //we loop until dwin*2-1 for the same reason
      for(j <- 1 to dwin*2-1 by 2) {
        //the +1 occurs because the next edge from current is the inverse edge and we want the next one
        if((j+i+1) < edges.length) {
          if(edges(j+i+1).srcId == edges(i).srcId && edges(j+i+1).dstId == edges(i).dstId) {
            occurrence += 1.0
          }
        }
      }
      //add edge to array
      groupedEdges = groupedEdges ++ Array(Edge(edges(i).srcId, edges(i).dstId, occurrence))
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from groupedEdges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(groupedEdges)
    //create graph
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    //return graph
    graph
  }

}
