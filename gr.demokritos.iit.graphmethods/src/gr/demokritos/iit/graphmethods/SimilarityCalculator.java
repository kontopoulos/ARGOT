package gr.demokritos.iit.graphmethods;

import org.apache.spark.graphx.Graph;

/**
 *
 * @author JKLS
 */
public interface SimilarityCalculator {
    
    /**
     * 
     * @param g1 graph1
     * @param g2 graph2
     * @return similarity between two graphs
     */
    public Similarity getSimilarity(Graph g1, Graph g2);
    
}
