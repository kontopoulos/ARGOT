package analytics

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
object Analyze {

  private var startTime = 0L
  private var endTime = 0L
  private var expStartTime = 0L
  private var expEndTime = 0L
  var executionTime = 0L
  var totalExecutionTime = 0L

  /**
    * Updates the start time
    */
  def start: Unit = {
    startTime = System.currentTimeMillis
  }

  /**
    * Updates the end time and execution time
    */
  def end: Unit = {
    endTime = System.currentTimeMillis
    executionTime = endTime - startTime
  }

  /**
    * Updates the experiment start time
    */
  def initiateExecution: Unit = {
    expStartTime = System.currentTimeMillis
  }

  /**
    * Updates the experiment end time
    */
  def terminateExecution: Unit = {
    expEndTime = System.currentTimeMillis
    totalExecutionTime = expEndTime - expStartTime
  }

  /**
    * Write logs of application
    * @param msg message
    */
  def writeLog(msg: String): Unit = {
    val w = new java.io.FileWriter("argot.log",true)
    w.write(s"${java.util.Calendar.getInstance.getTime} Message: $msg\n")
    w.close
  }

  /**
    * Writes execution time to file
    * @param label label of the execution time
    * @param fileName file to write
    */
  def writeTime(label: String, fileName: String): Unit = {
    val w = new java.io.FileWriter(fileName,true)
    w.write(s"$label|$executionTime\n")
    w.close
  }

  /**
    * Calculate several analytics and write to file
    * @param accuracy of experiment
    * @param train instances
    * @param test instances
    * @param fileName file to write
    */
  def writeAnalytics(accuracy: Double, train: Array[Array[String]], test: Array[Array[String]], fileName: String): Unit = {
    // calculate number of classes
    val numClasses = train.length
    // calculate number of training instances per class
    val numTrainInstances = train.map(_.length).zipWithIndex
      .map{case (num,index) => (num,index.toDouble)}
      .map(_.swap).map(i => s"Number of training instances in class ${i._1}: ${i._2}")
      .mkString("\n")
    // calculate number of testing instances per class
    val numTestInstances = test.map(_.length).zipWithIndex
      .map{case (num,index) => (num,index.toDouble)}
      .map(_.swap).map(i => s"Number of testing instances in class ${i._1}: ${i._2}")
      .mkString("\n")
    // calculate total number of instances
    val totalNumInstances = train.map(_.length).sum + test.map(_.length).sum
    // calculate average size of the instances in kilobytes
    val avgInstanceSize = (train ++ test).flatten.map(new java.io.File(_).length).sum.toDouble / (train ++ test).flatten.length / 1024
    // write analytics to file
    val w = new java.io.FileWriter(fileName,true)
    w.write(s"Accuracy: ${accuracy*100}%\n")
    w.write(s"$numTrainInstances\n")
    w.write(s"$numTestInstances\n")
    w.write(s"Total number of instances: $totalNumInstances\n")
    w.write(s"Total number of graph comparisons: ${numClasses*totalNumInstances}\n")
    w.write(s"Average size of each instance: $avgInstanceSize KB\n")
    w.close
  }

  /**
    * Writes total time to file
    * @param label label of the time
    * @param fileName file to write
    */
  def writeTotalTime(label: String, fileName: String): Unit = {
    val w = new java.io.FileWriter(fileName,true)
    w.write(s"$label|$totalExecutionTime\n")
    w.close
  }

  /**
    * Write all extracted features to file
    * @param label partial name of file
    * @param extractedFeatures
    */
  def writeFeaturesToFile(label: String, extractedFeatures: RDD[LabeledPoint]): Unit = {
    val features = extractedFeatures.collect
      .map(p => s"${p.label} ${p.features.toArray.mkString(" ")}")
      .mkString("\n")
    val w = new java.io.FileWriter(s"features$label.csv")
    w.write(features)
    w.close
  }

}
