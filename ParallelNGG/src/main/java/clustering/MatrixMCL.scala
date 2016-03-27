import java.io.FileWriter

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  * special thanks to https://github.com/joandre for markov clustering implementation
  */
class MatrixMCL(val maxIterations: Int, val expansionRate: Int, val inflationRate: Double, val epsilon: Double) extends Serializable {

  /**
    * Markov Clustering on the matrix
    * The RDD returned contains (matrixId,clusterId)
    * @param matrix a matrix
    * @return clusters
    */
  def getMarkovClusters(matrix: IndexedRowMatrix): RDD[(Long,Int)] = {
    // Number of current iterations
    var iter = 0
    // Convergence indicator
    var change = 1.0

    var M1:IndexedRowMatrix = normalization(matrix)
    //loop until a steady state is reached (convergence)
    while (iter < maxIterations && change > 0) {
      val M2: IndexedRowMatrix = removeWeakConnections(normalization(inflation(expansion(M1))))
      change = difference(M1, M2)
      iter += 1
      M1 = M2
    }

    val clusters = M1.rows.flatMap( r => {
        val sv = r.vector.toSparse
        sv.indices.map(i => (r.index, (i, sv.apply(i))))
      }
    ).groupByKey.map(node => (node._1, node._2.maxBy(_._2)._1))
    clusters
  }

  /**
    * Save clusters to csv file format
    * @param clusters to save
    */
  def saveClustersToCsv(clusters: RDD[(Long,Int)]) = {
    val w = new FileWriter("clusters.csv")
    try {
      clusters.foreach(x => w.write(x._1 + "," + x._2 + "\n"))
    }
    catch {
      case ex: Exception => println("Could not write to file. Reason: " + ex.getMessage)
    }
    finally w.close
  }

  /**
    * Load clusters from csv file
    * @param sc SparkContext
    * @param file file to load
    * @return clusters
    */
  def loadClustersFromCsv(sc: SparkContext, file: String): RDD[(Long,Int)] = {
    sc.textFile(file).map{x =>
      val parts = x.split(",")
      (parts.head.toLong, parts.last.toInt)
    }
  }

  /**
    * Normalizes the matrix
    * @param mat a matrix
    * @return normalized matrix
    */
  private def normalization(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(
      mat.rows.map{row =>
        val svec = row.vector.toSparse
        IndexedRow(row.index, new SparseVector(svec.size, svec.indices, svec.values.map(v => v/svec.values.sum)))
      }
    )
  }

  /**
    * Expands the matrix
    * @param mat a matrix
    * @return expanded matrix
    */
  private def expansion(mat: IndexedRowMatrix): BlockMatrix = {
    val bmat = mat.toBlockMatrix()
    var resmat = bmat
    for(i <- 1 until expansionRate){
      resmat = resmat.multiply(bmat)
    }
    resmat
  }

  /**
    * Inflates the matrix
    * @param mat a matrix
    * @return inflated matrix
    */
  private def inflation(mat: BlockMatrix): IndexedRowMatrix = {
    new IndexedRowMatrix(
      mat.toIndexedRowMatrix().rows
        .map{row =>
          val svec = row.vector.toSparse
          IndexedRow(row.index, new SparseVector(svec.size, svec.indices, svec.values.map(v => Math.exp(inflationRate*Math.log(v)))))
        }
    )
  }

  /**
    * Calculates the distance between two matrices
    * @param m1 a matrix at step n
    * @param m2 same matrix at step n+1
    * @return a normalized distance between m1 and m2
    */
  private def difference(m1: IndexedRowMatrix, m2: IndexedRowMatrix): Double = {
    val m1RDD:RDD[((Long,Int),Double)] = m1.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })

    val m2RDD:RDD[((Long,Int),Double)] = m2.rows.flatMap(r => {
      val sv = r.vector.toSparse
      sv.indices.map(i => ((r.index,i), sv.apply(i)))
    })

    val diffRDD = m1RDD.fullOuterJoin(m2RDD).map(diff => Math.pow(diff._2._1.getOrElse(0.0) - diff._2._2.getOrElse(0.0), 2))
    diffRDD.sum()
  }

  /**
    * Removes weakest connections
    * Connections weight in matrix which is inferior to a very small value is set to 0
    * @param mat matrix
    * @return sparsed matrix
    */
  private def removeWeakConnections(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(
      mat.rows.map{row =>
        val svec = row.vector.toSparse
        IndexedRow(row.index,
          new SparseVector(svec.size, svec.indices,
            svec.values.map(v => {
              if(v < epsilon) 0.0
              else v
            })
          ))
      }
    )
  }


}
