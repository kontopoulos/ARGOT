# ARGOT
ARGOT is an Apache spaRk based text mininG tOolkiT and library that supports and demonstrates the use of n-gram graphs within Natural Language Processing applications.

## Specifications
* [Apache Spark](http://spark.apache.org/) 2.0.2 ([GraphX](http://spark.apache.org/graphx/), [MLlib](http://spark.apache.org/mllib/))
* [Scala](https://www.scala-lang.org/) 2.11.7
* Maven Project

## Main concepts that were distributed with Apache Spark

## Code Snippets
* Create an n-gram graph from a string:

```scala

import graph.NGramGraph

...

// Creates a dummy graph object with ngram and dwin size equal to 3 
val g = new NGramGraph(3,3)

// Create the graph
g.fromString(Hello World!);

```

* Create an n-gram graph from a file 

```scala

...

import graph.NGramGraph

...
        
	val g = new NGramGraph(3,3) 
	// Load the data string from the file
	g.fromFile("file.txt")

```

* Merge multiple small graphs to a huge distributed one 

```scala

import graph.operators.MultipleGraphMergeOperator
import traits.DocumentGraph
import org.apache.spark.rdd.RDD


...

	// RDD of document graphs
	// A document graph may be an n-gram graph, a word n-gram graph
	// or any other graph that extends the DocumentGraph trait
	val manyGraphs // RDD[DocumentGraph]
	// create an instance of the operator with 8 partitions
	val merger = new MultipleGraphMergeOperator(8)
	// create the distributed graph
	val dGraph = merger.getGraph(manyGraphs)

```

* Compare a small graph with a distributed one, extracting their similarity 

```scala

...

import graph.similarity.DiffSizeGSCalculator
import org.apache.spark.SparkContext
import graph.DistributedCachedNGramGraph
import traits.DocumentGraph

...

    val gsc = new DiffSizeGSCalculator(sc) // pass the spark context as parameter
    // extract their similarity
    val gs = gsc.getSimilarity(smallGraph,dGraph) 
    // print containment similarity
    println(gs.getSimilarityComponents("containment"))
    // println value similarity
    println(gs.getSimilarityComponents("value"))
    // print normalized value similarity
    println(gs.getSimilarityComponents("normalized"))
    // print size similarity
    println(gs.getSimilarityComponents("size"))

```

* Run n-fold cross validation classification experiment 

```scala

import experiments.CrossValidation
import org.apache.spark.{SparkConf, SparkContext}

...

	// create an instance of the experiment class
	// directory contains subdirectories with classes, each containing their corresponding texts
	val exp = new CrossValidation(sc,"docs",10) // spark context, directory to classify, number of folds
	// run the experiment with 8 partitions
	exp.run(8)

```



## Details
Current version/branch of ARGOT contains the following:
- The n-gram graphs (NGG) representations. See [thesis, Chapter 3](http://www.iit.demokritos.gr/~ggianna/thesis.pdf) of George Giannakopoulos for more info.
- The NGG operators update/merge, intersect, allNotIn, etc. See [thesis, Chapter 4](http://www.iit.demokritos.gr/~ggianna/thesis.pdf) of George Giannakopoulos for more info.
- A text tokenizer (extraction of n-grams, words, sentences from a text etc.).
- Feature extraction algorithm for document classification.
- Classifiers (Naive Bayes, SVM (current algorithm supports binary classification only)).
- Markov clustering algorithm for similarity matrices. Many thanks to user [joandre](https://github.com/joandre/).
- A simple clustering algorithm for documents based on graph similarities (under construction).
- A multiple document summarizer (under construction).

Above operations can be executed in a cluster and make use of every available processor.