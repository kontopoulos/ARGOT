package gr.demokritos.iit.graphmethods;

import org.apache.spark.graphx.Graph;

/**
 *
 * @author JKLS
 */
public interface BinaryGraphOperator {
    
    /**
     * Creates a new graph based on an operation
     * @param g1 graph1
     * @param g2 graph2
     * @return graph after operation upon it
     */
    public Graph getResult(Graph g1, Graph g2);
    
}
