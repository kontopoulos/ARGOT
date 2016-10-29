#Application of N-gram Graph Methods on Distributed Platforms

- Maven Project  
- IntelliJ Project  

#Specifications
- Apache Spark 2.0.1 with Graphx and MLlib  
- scala 2.11.7   
- Import JInsect library ("https://sourceforge.net/projects/jinsect/")   

#Examples of Basic Use
//ideally number of partitions should be equal to the number of physical cores of your cluster  
val numPartitions = 4 
- Create n-gram graph from file  
val e = new StringEntity  
e.fromFile("file.txt")  
val nggc = new NGramGraphCreator(sc, 3, 3)  
val ngg = nggc.getGraph(e)  
- Merge two graphs (ngg1, ngg2)  
val mo = new MergeOperator(0.5)  
val resultGraph = mo.getResult(ngg1, ngg2)  
- Intersect two graphs  
val io = new IntersectOperator(0.5)  
val resultGraph = io.getResult(ngg1, ngg2)  
- Inverse Intersect two graphs  
val ii = new InverseIntersectOperator 
val resultGraph = ii.getResult(ngg1, ngg2)  
- Delta Operator between two graphs  
val do = new DeltaOperator 
val resultGraph = do.getResult(ngg1, ngg2)  
- Extract Similarities between two graphs  
val gsc = new GraphSimilarityCalculator  
val gs = gsc.getSimilarity(ngg1, ngg2)  
gs.getOverallSimilarity  
gs.getSimilarityComponents("size")  
gs.getSimilarityComponents("value")  
gs.getSimilarityComponents("containment")  
gs.getSimilarityComponents("normalized")  
gs.toString  

#Summarization
val sum = new NGGSummarizer(sc,numPartitions)  
//give a folder which contains the documents you need the summary from  
val sums = sum.getSummary("folder")  
//save summaries to files  
sum.saveSummaries(sums)  

For large datasets it is recommended to increase the driver`s memory from spark-env.sh file
