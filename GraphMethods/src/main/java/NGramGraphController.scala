import java.io.{FileWriter, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class NGramGraphController(val sc: SparkContext) extends GraphController {

  /**
   * Creates a graph based on ngram, dwin and entity
   * @param e entity from which a graph will be created
   * @param ngram size of ngrams
   * @param dwin size of adjacency window
   * @return graph from entity
   */
  override def getGraph(e: Entity, ngram: Int, dwin: Int): Graph[String, Double] = {
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
    var m:Map[String, Edge[Double]] = Map()
    //erase duplicates and increase occurrence
    edges.foreach{
      e =>
        if(!m.contains(e.srcId + "," + e.dstId)) {
          //if not found just add it
          m += (e.srcId + "," + e.dstId -> Edge(e.srcId, e.dstId, e.attr))
        }
        else {
          //if it is found increase the occurrence by 1
          m += (e.srcId + "," + e.dstId -> Edge(e.srcId, e.dstId, e.attr*1.0 + 1.0))
        }
    }
    var buffer = Array.empty[Edge[Double]]
    //convert the map to array
    m.foreach{
      e =>
        buffer = buffer ++ Array(Edge(e._2.srcId, e._2.dstId, e._2.attr))
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(buffer)
    //create graph
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    //return graph
    graph
  }

  /**
   * Save graph to dot format file
   * NOTE: use this method only for small graphs and testing purposes,
   * because it fetches all the data to the driver program,
   * leading to memory overflow if the graph is too large
   * @param g graph to save
   */
  override def saveGraphToDotFormat(g: Graph[String, Double]) = {
    var str = "digraph nGramGraph {\n"
    //map that holds the vertices
    var vertices: Map[Int, String] = Map()
    //collect vertices from graph and replace punctuations
    g.vertices.collect
      .foreach{ v => vertices += ( v._1.toInt -> v._2.replaceAll("[`~!@#$%^&*()+-=,.<>/?;:' ]", "_")) }
    //construct the string
    g.edges.distinct.collect
      .foreach{ e => str += "\t" + vertices(e.srcId.toInt) + " -> " + vertices(e.dstId.toInt) + " [label=\"" + e.srcId + "" + e.dstId + "\" weight=" + e.attr + "];\n" }
    str += "}"
    //write string to file
    Some(new PrintWriter("nGramGraph.dot")).foreach{p => p.write(str); p.close}
  }

  /**
   * Save vertices ans edges of graph to files
   * NOTE: if graph with certain label has already been saved the files will be appended
   * therefore the saved edges and vertices will not correspond to the original graph
   * @param g graph to save
   */
  def saveGraphToTextFiles(g: Graph[String, Double], label: String) = {
    //collect edges per partition, so there is no memory overflow
    val ew = new FileWriter(label + "Edges.txt", true)
    val edgeParts = g.edges.distinct.partitions
    for (p <- edgeParts) {
      val idx = p.index
      //The second argument is true to avoid rdd reshuffling
      val partRdd = g.edges.distinct
        .mapPartitionsWithIndex((index: Int, it: Iterator[Edge[Double]]) => if(index == idx) it else Iterator(), true )
      //partRdd contains all values from a single partition
      partRdd.collect.foreach{ e =>
        ew.write(e.srcId + "<>" + e.dstId + "<>" + e.attr + "\n")
      }
    }
    //close file
    ew.close
    //collect vertices per partition, so there is no memory overflow
    val vw = new FileWriter(label + "Vertices.txt", true)
    val vertexParts = g.vertices.distinct.partitions
    for (p <- vertexParts) {
      val idx = p.index
      //The second argument is true to avoid rdd reshuffling
      val partRdd = g.vertices.distinct
        .mapPartitionsWithIndex((index: Int, it: Iterator[(Long, String)]) => if(index == idx) it else Iterator(), true )
      //partRdd contains all values from a single partition
      partRdd.collect.foreach{ v =>
        vw.write(v._1 + "<>" + v._2.replaceAll("\n", " ") + "\n")
      }
    }
    //close file
    vw.close
    }

  /**
   * Load graph from edges file and vertices file
   * @return graph
   */
  def loadGraphFromTextFiles(label: String): Graph[String, Double] = {
    //path for vertices file
    val vertexFile = label + "Vertices.txt"
    //path for edges file
    val edgeFile = label + "Edges.txt"
    //create EdgeRDD from file rows
    val edges: RDD[Edge[Double]] = sc.textFile(edgeFile).map{ line =>
      val row = line.split("<>")
      Edge(row(0).toLong, row(1).toLong, row(2).toDouble)
    }
    //create VertexRDD from file rows
    val vertices: RDD[(Long, String)] = sc.textFile(vertexFile).map{ line =>
      val row = line.split("<>")
      (row(0).toLong, row(1))
    }
    //create graph
    val graph: Graph[String, Double] = Graph(vertices, edges)
    graph
  }

}
