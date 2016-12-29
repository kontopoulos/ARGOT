package experiments

import analytics.Analyze
import graph.NGramGraph
import graph.operators.MultipleGraphMergeOperator
import ml.classification._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import traits.DocumentGraph

/**
  * @author Kontopoulos Ioannis
  */
class CrossValidation(sc: SparkContext, selectedClassifier: String, directory: String, numOfFolds: Int) {

  // read documents per class based on directory given
  val documentsPerClass = getListOfSubDirectories(directory)
    .map(dir => (dir,new java.io.File(s"$directory/$dir").listFiles.map( f => f.getAbsolutePath)))

  /**
    * Run n-fold cross validation
    * @param numPartitions
    */
  def run(numPartitions: Int): Unit = {
    for (fold <- 0 until numOfFolds) {
      classifyOnFold(numPartitions,fold)
      println(s"${fold+1} fold(s) completed.")
    }
  }

  /**
    * Run the classification experiment on one fold
    * @param currentFold
    * @param numPartitions
    */
  private def classifyOnFold(currentFold: Int, numPartitions: Int): Unit = {
    // file to export statistics and results
    val resultsFileName = s"experiment${numPartitions}_$currentFold.txt"

    println("Separating training and testing instances...")
    Analyze.writeLog("Separating training and testing instances...")
    val trainInstances = documentsPerClass.map{
      case (className,documents) =>
        documents.slice(0, currentFold) ++ documents.slice(currentFold+documents.length/numOfFolds, documents.length)
    }
    val testInstances = documentsPerClass.map{
      case (className,documents) =>
        documents.slice(currentFold, currentFold+documents.length/numOfFolds)
    }

    println("Getting training instances that need to be merged...")
    Analyze.writeLog("Getting training instances that need to be merged...")
    // randomly select 90% of the training instances to merge
    val mergeDocuments = trainInstances.map(docs => docs.slice(0,(docs.length*0.9).toInt))
      // index instances for statistics
      .zipWithIndex
      .map(_.swap)
      .map{case (index,documents) => (index.toDouble,documents)}

    Analyze.initiateExecution
    println("Merging graphs...")
    Analyze.writeLog("Merging graphs...")
    val mo = new MultipleGraphMergeOperator(numPartitions)
    // create a class graph per category/class
    val classGraphs = mergeDocuments.map{
      case (index,documents) =>
        val graphs = sc.parallelize(documents,numPartitions).map{
          doc =>
            val g = new NGramGraph(3,3)
            g.fromFile(doc)
            g
        }
        Analyze.start
        val classGraph = mo.getGraph(graphs.asInstanceOf[RDD[DocumentGraph]])
        Analyze.end
        Analyze.writeTime(s"class$index",resultsFileName)
        classGraph
    }

    println("Indexing training and testing instances...")
    Analyze.writeLog("Indexing training and testing instances...")
    // index train instances
    val docsToTrain = trainInstances
      .zipWithIndex
      .map(_.swap)
      .flatMap{
        case (index,documents) =>
          documents.map((index.toDouble,_))
      }
    // index test instances
    val docsToTest = testInstances
      .zipWithIndex
      .map(_.swap)
      .flatMap{
        case (index,documents) =>
          documents.map((index.toDouble,_))
      }

    println("Extracting features from training instances...")
    Analyze.writeLog("Extracting features from training instances...")
    val extractor = new NGramGraphFeatureExtractor(sc,numPartitions)
    Analyze.start
    // extract features from train instances
    val trainingFeatures = extractor.extract(classGraphs,docsToTrain)
    Analyze.end
    Analyze.writeTime("Training",resultsFileName)

    println("Extracting features from testing instances...")
    Analyze.writeLog("Extracting features from testing instances...")
    Analyze.start
    // extract features from test instances
    val testingFeatures = extractor.extract(classGraphs,docsToTest)
    Analyze.end
    Analyze.writeTime("Testing",resultsFileName)

    Analyze.start
    val accuracy = runClassificationAlgorithm(trainingFeatures,testingFeatures)
    Analyze.end
    Analyze.writeTime("ML",resultsFileName)
    Analyze.terminateExecution
    Analyze.writeTotalTime("Total",resultsFileName)

    // free memory from distributed graphs
    classGraphs.foreach(_.clearFromMemory)

    println(s"Accuracy: ${accuracy*100}%")
    println("Writing statistics of experiment...")
    Analyze.writeLog("Writing statistics of experiment...")

    // write analytics of experiment to file
    Analyze.writeAnalytics(accuracy,trainInstances,testInstances,resultsFileName)
    // write all extracted features to file
    Analyze.writeFeaturesToFile(currentFold.toString, trainingFeatures.union(testingFeatures))
    println("Experiment completed.")
    Analyze.writeLog("Experiment completed.")
  }

  /**
    * Classifies on one random fold only
    * @param numPartitions
    */
  def classify(numPartitions: Int): Unit = {
    val r = scala.util.Random
    val fold = r.nextInt(numOfFolds)
    classifyOnFold(fold,numPartitions)
  }

  /**
    * Runs selected classification algorithm
    * @param train
    * @param test
    * @return accuracy
    */
  private def runClassificationAlgorithm(train: RDD[LabeledPoint], test: RDD[LabeledPoint]): Double = {
    println(s"Running classification algorithm [$selectedClassifier]...")
    Analyze.writeLog(s"Running classification algorithm [$selectedClassifier]...")
    selectedClassifier match {
      case "Naive Bayes" => {
        val classifier = new NaiveBayesClassifier
        val model = classifier.train(train)
        return classifier.test(model,test)
      }
      case "SVMbinary" => {
        val classifier = new SVMBinaryClassifier
        val model = classifier.train(train)
        return classifier.test(model,test)
      }
      case "SVMMulticlass" => {
        val classifier = new SVMMulticlassClassifier
        val model = classifier.train(train)
        return classifier.test(model,test)
      }
      case "Random Forest" => {
        val classifier = new RandomForestClassifier
        val model = classifier.train(train)
        return classifier.test(model,test)
      }
    }
  }

  /**
    * Gets the list of subdirectories of given directory
    * @param directory
    * @return array of subdirectories
    */
  def getListOfSubDirectories(directory: String): Array[String] = {
    (new java.io.File(directory)).listFiles.filter(_.isDirectory).map(_.getName)
  }

}
