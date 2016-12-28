package experiments

import analytics.Analyze
import graph.NGramGraph
import graph.operators.MultipleGraphMergeOperator
import ml.classification.{NGramGraphFeatureExtractor, NaiveBayesClassifier}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import traits.DocumentGraph

/**
  * @author Kontopoulos Ioannis
  */
class CrossValidation(sc: SparkContext, directory: String, numOfFolds: Int) {

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
  def classifyOnFold(currentFold: Int, numPartitions: Int): Unit = {
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

    Analyze.initiate
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

    val classifier = new NaiveBayesClassifier
    Analyze.start
    // train Naive Bayes model
    println("Running classification algorithm...")
    Analyze.writeLog("Running classification algorithm...")
    val model = classifier.train(trainingFeatures)
    // test the model and get the accuracy
    val accuracy = classifier.test(model,testingFeatures)
    Analyze.end
    Analyze.terminate
    Analyze.writeTime("ML",resultsFileName)
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
    * Gets the list of subdirectories of given directory
    * @param directory
    * @return array of subdirectories
    */
  def getListOfSubDirectories(directory: String): Array[String] = {
    (new java.io.File(directory)).listFiles.filter(_.isDirectory).map(_.getName)
  }

}
