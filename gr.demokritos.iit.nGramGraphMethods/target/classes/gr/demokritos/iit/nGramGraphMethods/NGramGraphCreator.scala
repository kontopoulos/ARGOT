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
    //map that holds the srcId and dstId of edges
    var m:Map[String, Tuple2[Long, Long]] = Map()
    //create edges
    for(j <- 0 to e.getEntityComponents.size - 1) {
      for(i <- 1 to dwin) {
        if((j+i) < e.getEntityComponents.size) {
          //because a common edge is the one which connects same vertices
          //if an inverse srcId and dstId is found, inverse it so we can consider it a common edge
          if(!m.contains(en.getEntityComponents(j+i).label + "," + en.getEntityComponents(j).label)) {
            edges = edges ++ Array(Edge(en.getEntityComponents(j).label.toLong, en.getEntityComponents(j+i).label.toLong, 1.0))
            m += (en.getEntityComponents(j).label + "," + en.getEntityComponents(j+i).label -> new Tuple2(0L, 0L))
          }
          else {
            edges = edges ++ Array(Edge(en.getEntityComponents(j+i).label.toLong, en.getEntityComponents(j).label.toLong, 1.0))
          }
        }
      }
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges)
    //create graph
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    //group duplicate edges
    val finalGraph = graph.groupEdges((a, b) => a+b)
    //return graph
    finalGraph
  }

}
