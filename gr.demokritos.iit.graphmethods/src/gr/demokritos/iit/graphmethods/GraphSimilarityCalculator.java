package gr.demokritos.iit.graphmethods;

import org.apache.spark.graphx.Graph;

/**
 *
 * @author JKLS
 */
public class GraphSimilarityCalculator implements SimilarityCalculator {

    /**
     * Retrieves the similarity between two graphs
     * @param g1 graph1
     * @param g2 graph2
     * @return similarity between two graphs
     */
    @Override
    public Similarity getSimilarity(Graph g1, Graph g2) {
        GraphSimilarity gs = new GraphSimilarity();
        gs.setSizeSimilarity(calculateSizeSimilarity(g1, g2));
        gs.setValueSimilarity(calculateValueSimilarity(g1, g2));
        gs.setContainmentSimilarity(calculateContainmentSimilarity(g1, g2));
        return gs;
    }
    
    /**
     * Calculates size similarity of two graphs
     * @param g1 graph1
     * @param g2 graph2
     * @return value of size similarity
     */
    private double calculateSizeSimilarity(Graph g1, Graph g2) {
        double sSimil;
        //number of edges of graph1
        long eNum1 = g1.edges().distinct().count();
        //number of edges of graph2
        long eNum2 = g2.edges().distinct().count();
        //calculate size similarity
        if (eNum1 > eNum2) sSimil = (double) eNum2/eNum1;
        else sSimil = (double) eNum1/eNum2;
        return sSimil;
    }
    
    /**
     * Calculates value similarity of two graphs
     * @param g1 graph1
     * @param g2 graph2
     * @return value of value similarity
     */
    private double calculateValueSimilarity(Graph g1, Graph g2) {
        double vSimil = 0.0;
        //TODO code here
        return vSimil;
    }
    
    /**
     * Calculates containment similarity of two graphs
     * @param g1 graph g1
     * @param g2 graph g2
     * @return value of containment similarity
     */
    private double calculateContainmentSimilarity(Graph g1, Graph g2) {
        double cSimil = 0.0;
        //TODO code here
        return cSimil;
    }
    
}
