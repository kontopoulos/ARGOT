package experiments.optimized

import java.io.FileWriter

import graph.NGramGraph
import graph.operators.NGGMergeOperator
import org.apache.spark.SparkContext

/**
  * @author Kontopoulos Ioannis
  */
class BinaryNFoldCrossValidation(sc: SparkContext, numPartitions: Int, numFold: Int) {

  def run(classifier: String) = {
    println("Classifier Selected: " + classifier)
    //if (classifier == "SVM") runSVM
    if (classifier == "NaiveBayes") runNaiveBayes
    //else if (classifier == "simple") runSimple
    else println("Wrong Option!")
  }

  def runNaiveBayes: Unit = {
    println("Reading files...")
    val files1 = new java.io.File("C01/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    val files2 = new java.io.File("C02/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    println("Reading complete.")
    var fs = Array.empty[Double]
    var f = 0.0
    //numFold - 1
    for (j <- 0 to 0) {
      val metrics = naiveBayesFoldValidation(j, files1, files2)
      f += metrics
      fs :+= metrics
    }
    //calculate averaged metrics
    f = f/numFold
    //calculate standard deviation
    var sum = 0.0
    fs.foreach{ p =>
      sum += Math.pow((p-f), 2)
    }
    sum = sum/numFold
    val stdev = Math.sqrt(sum)
    //calculate standard error
    val sterr = stdev/(Math.sqrt(numFold))
    println("===================================")
    println("F-Measure = " + f)
    println("Standard Deviation = " + stdev)
    println("Standard Error = " + sterr)
    println("===================================")
  }

  private def naiveBayesFoldValidation(currentFold: Int, files1: Array[String], files2: Array[String]): Double = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = files1.slice(currentFold, currentFold+files1.length/numFold)
    val training1 = files1.slice(0, currentFold) ++ files1.slice(currentFold+files1.length/numFold, files1.length)
    //get training and testing datasets from second category
    val testing2 = files2.slice(currentFold, currentFold+files2.length/numFold)
    val training2 = files2.slice(0, currentFold) ++ files2.slice(currentFold+files2.size/numFold, files2.length)
    println("Separation complete.")
    println("Getting files to merge...")
    val trainingGraphs1 = training1.slice(0,(training1.length*0.9).toInt)
    val trainingGraphs2 = training2.slice(0,(training2.length*0.9).toInt)
    println("Merging graphs...")
    val mergeGraphs1 = sc.parallelize(trainingGraphs1,numPartitions).map{
      document =>
        val g = new NGramGraph(3,3)
        g.fromFile(document)
        g
    }
    val mo = new NGGMergeOperator(numPartitions)
    val m1start = System.currentTimeMillis
    val classGraph1 = mo.getGraph(mergeGraphs1)
    val m1end = System.currentTimeMillis

    val mergeGraphs2 = sc.parallelize(trainingGraphs2,numPartitions).map{
      document =>
        val g = new NGramGraph(3,3)
        g.fromFile(document)
        g
    }
    val m2start = System.currentTimeMillis
    val classGraph2 = mo.getGraph(mergeGraphs2)
    val m2end = System.currentTimeMillis
    println("Merging complete.")

    val m1time = m1end - m1start
    val m2time = m2end - m2start

    val mTimes = new FileWriter("experiment.log",true)
    mTimes.write("Threads " + numPartitions + " | class1 " + m1time + "\n")
    mTimes.write("Threads " + numPartitions + " | class2 " + m2time + "\n")
    mTimes.close

    val start = System.currentTimeMillis
    val firstGraphSet = sc.parallelize(training1).map{
      document =>
        val g = new NGramGraph(3,3)
        g.fromFile(document)
        g
    }.map((0.0,_))
    val secondGraphSet = sc.parallelize(training2).map{
      document =>
        val g = new NGramGraph(3,3)
        g.fromFile(document)
        g
    }.map((1.0,_))
    val graphsToTrain = firstGraphSet.union(secondGraphSet).collect
    val end = System.currentTimeMillis
    val time = end - start
    val timer = new FileWriter("experiment.log",true)
    timer.write("Threads " + numPartitions + " | Train graphs " + time + "\n")
    timer.close

    //start training
    println("Creating feature vectors...")
    val cls = new BinaryFeatureExtractor(sc, numPartitions)
    val model = cls.train(Array(classGraph1, classGraph2), graphsToTrain)

    val s = System.currentTimeMillis
    val firstTestSet = sc.parallelize(testing1).map{
      document =>
        val g = new NGramGraph(3,3)
        g.fromFile(document)
        g
    }.map((0.0,_))
    val secondTestSet = sc.parallelize(testing2).map{
      document =>
        val g = new NGramGraph(3,3)
        g.fromFile(document)
        g
    }.map((1.0,_))
    val graphsToTest = firstTestSet.union(secondTestSet).collect
    val e = System.currentTimeMillis
    val es = e - s
    val eee = new FileWriter("experiment.log",true)
    eee.write("Threads " + numPartitions + " | Test graphs " + es + "\n")
    eee.close

    // =========

    //start testing
    println("Testing...")
    val testStart = System.currentTimeMillis
    val metrics = cls.test(model, Array(classGraph1, classGraph2), graphsToTest)
    val testEnd = System.currentTimeMillis
    val testTime = testEnd - testStart
    val tes = new FileWriter("experiment.log",true)
    tes.write("Threads " + numPartitions + " | Test time " + testTime + "\n")
    tes.close

    //free memory of stored edges
    classGraph1.clearFromMemory
    classGraph2.clearFromMemory
    println("Testing complete")
    println("===========================")
    println("Fold Completed = " + (currentFold+1))
    println("===========================")

    val acc = new FileWriter("experiment.log",true)
    acc.write("F-measure " + metrics + "\n")
    acc.close

    metrics
  }


}
