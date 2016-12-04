package clustering

import java.io.FileWriter

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

// TODO keep checking original implementation for mcl improvements
/**
  * thanks to https://github.com/joandre for markov clustering implementation
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

    var M1: IndexedRowMatrix = normalization(matrix)
    //loop until a steady state is reached (convergence)
    while (iter < maxIterations && change > 0) {
      val M2: IndexedRowMatrix = inflation(expansion(M1))
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

  /** Normalize matrix
    * @param mat an unnormalized matrix
    * @return normalized matrix
    */
  def normalization(mat: IndexedRowMatrix): IndexedRowMatrix ={
    new IndexedRowMatrix(
      mat.rows
        .map{row =>
          val svec = row.vector.toSparse
          IndexedRow(row.index,
            new SparseVector(svec.size, svec.indices, svec.values.map(v => v/svec.values.sum)))
        })
  }

  /** Normalize row
    * @param row an unnormalized row of the  matrix
    * @return normalized row
    */
  def normalization(row: SparseVector): SparseVector ={
    new SparseVector(row.size, row.indices, row.values.map(v => v/row.values.sum))
  }

  /** Expand matrix
    * @param mat an matrix
    * @return expanded matrix
    */
  def expansion(mat: IndexedRowMatrix): BlockMatrix = {
    val bmat = mat.toBlockMatrix()
    var resmat = bmat
    for(i <- 1 until expansionRate){
      resmat = resmat.multiply(bmat)
    }
    resmat
  }

  /** Inflate matrix
    * Prune and normalization are applied locally (on each row). So we avoid two more complete scanning of matrix.
    * As explained in issue #8, pruning is applied on expanded matrix, so we take advantage of natural normalized expansion state.
    * @param mat an matrix
    * @return inflated matrix
    */
  def inflation(mat: BlockMatrix): IndexedRowMatrix = {
    new IndexedRowMatrix(
      mat.toIndexedRowMatrix.rows
        .map{row =>
          val svec = removeWeakConnections(row.vector.toSparse) // Pruning elements locally, instead of scanning all matrix again
          IndexedRow(row.index,
            // Normalizing rows locally, instead of scanning all matrix again
            normalization(
              new SparseVector(svec.size, svec.indices, svec.values.map(v => Math.exp(inflationRate*Math.log(v))))
            )
          )
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

  /** Remove weakest connections from a row
    * Connections weight in matrix which is inferior to a very small value is set to 0
    * @param row a row of the matrix
    * @return sparsed row
    * @todo Add more complex pruning strategies.
    * @see http://micans.org/mcl/index.html
    */
  def removeWeakConnections(row: SparseVector): SparseVector ={
    new SparseVector(
      row.size,
      row.indices,
      row.values.map(v => {
        if(v < epsilon) 0.0
        else v
      })
    )
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

}
