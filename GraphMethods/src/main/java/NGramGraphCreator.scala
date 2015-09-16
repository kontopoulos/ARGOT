import org.apache.spark.SparkContext
import org.apache.spark.graphx.{PartitionStrategy, Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class NGramGraphCreator(val sc: SparkContext, val ngram: Int, val dwin: Int) extends GraphCreator {

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
    var vertices = Array.empty[(Long, String)]
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
    //create edges based on symmetric distribution
    for(j <- 0 to e.getEntityComponents.size - 1) {
      for(i <- 1 to dwin) {
        if((j+i) < e.getEntityComponents.size) {
          //add edge
          edges = edges ++ Array(Edge(en.getEntityComponents(j).label.toLong, en.getEntityComponents(j+i).label.toLong, 1.0))
          //add inverse edge
          edges = edges ++ Array(Edge(en.getEntityComponents(j+i).label.toLong, en.getEntityComponents(j).label.toLong, 1.0))
        }
      }
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges)
    //create graph from vertices and edges arrays, erase duplicate edges and increase occurence
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges( (a, b) => a + b )
    //return graph
    graph
  }

}
