package gr.demokritos.iit.graphmethods;

import java.util.Map;

/**
 *
 * @author JKLS
 */
public interface Similarity {
    
    /**
     * 
     * @return overall similarity
     */
    public double getOverallSimilarity();
    
    /**
     * 
     * @return map of similarity components
     */
    public Map getSimilarityComponents();
    
}
