#Application of N-gram Graph Methods on Distributed Platforms

- Maven Project  
- IntelliJ Project  

#Specifications
- Apache Spark 1.6.0 with Graphx  
- scala 2.10.4   

#Examples of Basic Use
//ideally number of partitions should be equal to the number of physical cores of your cluster  
val numPartitions = 4
- Create n-gram graph from string  
val e = new StringEntity  
e.dataString = "Hello World!"  
val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)  
val ngg = nggc.getGraph(e)  
- Create n-gram graph from file  
val e = new StringEntity  
e.readDataStringFromFile("file.txt")  
val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)  
val ngg = nggc.getGraph(e)  
- Merge two graphs (ngg1, ngg2)  
val mo = new MergeOperator(numPartitions, 0.5)  
val resultGraph = mo.getResult(ngg1, ngg2)  
- Intersect two graphs  
val io = new IntersectOperator(numPartitions, 0.5)  
val resultGraph = io.getResult(ngg1, ngg2)  
- Inverse Intersect two graphs  
val ii = new InverseIntersectOperator(numPartitions)  
val resultGraph = ii.getResult(ngg1, ngg2)  
- Delta Operator between two graphs  
val do = new DeltaOperator(numPartitions)  
val resultGraph = do.getResult(ngg1, ngg2)  
- Extract Similarities between two graphs  
val gsc = new GraphSimilarityCalculator  
val gs = gsc.getSimilarity(ngg1, ngg2)  
gs.getOverallSimilarity  
gs.getSimilarityComponents("size")  
gs.getSimilarityComponents("value")  
gs.getSimilarityComponents("containment")  
gs.getSimilarityComponents("normalized")  

For large datasets it is recommended to increase the driver`s memory from spark-env.sh file
