package gr.demokritos.iit.graphmethods;

import org.apache.spark.graphx.Graph;

/**
 *
 * @author JKLS
 */
public class GraphSimilarityCalculator implements SimilarityCalculator {

    /**
     * 
     * @param g1 graph1
     * @param g2 graph2
     * @return similarity between two graphs
     */
    @Override
    public Similarity getSimilarity(Graph g1, Graph g2) {
        //TODO code here, delete next line
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        long eNum1 = g1.edges().count();
        //number of edges of graph2
        long eNum2 = g2.edges().count();
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
