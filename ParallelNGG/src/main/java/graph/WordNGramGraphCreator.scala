import org.apache.spark.SparkContext
import org.apache.spark.graphx.{PartitionStrategy, Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class WordNGramGraphCreator(val sc: SparkContext, val numPartitions: Int, val ngram: Int, val dwin: Int) extends GraphCreator {

  /**
    * Creates a graph based on dwin and entity
    * @param e entity from which a graph will be created
    * @return word n-gram graph from entity
    */
  override def getGraph(e: Entity): Graph[String, Double] = {
    val tokenizer = new StringEntityTokenizer
    //create vertices based on ngram size
    val atoms = tokenizer.getCapWordNGrams(e, ngram)
      .map(a => (a.dataStream.toLowerCase.hashCode.toLong, a.dataStream))
    val vertices = atoms.collect
    //create edges from vertices
    val edges = (vertices ++ Array.fill(dwin)((-1L, null))) //add dummy vertices at the end
      .sliding(dwin + 1) //slide over dwin + 1 vertices at the time
      .flatMap(arr => {
      val (srcId, _) = arr.head //take first
      //generate 2n edges
      arr.tail.flatMap{case (dstId, _) =>
        Array(Edge(srcId, dstId, 1.0), Edge(dstId, srcId, 1.0))
      }}.filter(e => e.srcId != -1L & e.dstId != -1L)) //drop dummies
      .toArray
    //create vertex RDD from vertices array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges, numPartitions)
    //create graph from vertices and edges arrays, erase duplicate edges and increase occurrence
    val graph: Graph[String, Double] = Graph(atoms.distinct, edgeRDD).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges( (a, b) => a + b )
    //return graph
    graph
  }

}
