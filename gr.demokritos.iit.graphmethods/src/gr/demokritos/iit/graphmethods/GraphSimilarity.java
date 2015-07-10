package gr.demokritos.iit.graphmethods;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author JKLS
 */
public class GraphSimilarity implements Similarity {

    //value of size similarity
    private double sizeSimilarity;
    //value of value similarity
    private double valueSimilarity;
    //value of contaiment similarity
    private double containmentSimilarity;
    //value of overall similarity
    private double overallSimilarity;
    private final Map components;

    /**
     * Constructor
     */
    public GraphSimilarity() {
        components = new HashMap();
    }
    
    /**
     * 
     * @return overall similarity
     */
    @Override
    public double getOverallSimilarity() {
        return overallSimilarity;
    }

    /**
     * 
     * @return map of graph similarity components
     */
    @Override
    public Map getSimilarityComponents() {
        components.put("size", sizeSimilarity);
        components.put("value", valueSimilarity);
        components.put("containment", containmentSimilarity);
        return components;
    }

    /**
     * Setter for size similarity
     * @param sizeSimilarity 
     */
    public void setSizeSimilarity(double sizeSimilarity) {
        this.sizeSimilarity = sizeSimilarity;
    }

    /**
     * Setter for value similarity
     * @param valueSimilarity 
     */
    public void setValueSimilarity(double valueSimilarity) {
        this.valueSimilarity = valueSimilarity;
    }

    /**
     * Setter for containment similarity
     * @param containmentSimilarity 
     */
    public void setContainmentSimilarity(double containmentSimilarity) {
        this.containmentSimilarity = containmentSimilarity;
    }  
    
}
